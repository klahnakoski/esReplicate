# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from datetime import timedelta, datetime

from pyLibrary.collections import MAX
from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import wrap, unwraplist, literal_field
from pyLibrary.env import elasticsearch, http
from pyLibrary.env.files import File
from pyLibrary.maths import Math
from pyLibrary.queries import qb
from pyLibrary.thread.threads import Queue, Thread, Signal
from pyLibrary.times.dates import Date
from pyLibrary.times.timer import Timer

from mohg.hg_mozilla_org import HgMozillaOrg



# REPLICATION
#
# Replication has a few benefits:
# 1) The slave can have scripting enabled, allowing more powerful set of queries
# 2) Physical proximity increases latency
# 3) The slave can be configured with better hardware
# 4) The slave's exclusivity increases availability (Mozilla's public cluster may have time of high load)

far_back = datetime.utcnow() - timedelta(weeks=52)
BATCH_SIZE = 1000
http.ZIP_REQUEST = False
hg = None


def get_last_updated(es, primary_field):
    try:
        results = es.search({
            "query": {"match_all": {}},
            "from": 0,
            "size": 0,
            "facets": {"last_time": {"statistical": {"field": primary_field}}}
        })

        if results.facets.last_time.count == 0:
            return Date.MIN
        return results.facets.last_time.max
    except Exception, e:
        Log.warning("Can not get_last_updated from {{host}}/{{index}}", {
            "host": es.settings.host,
            "index": es.settings.index
        }, e)
        return None


def get_pending(es, since, pending_bugs, primary_key, please_stop):
    while not please_stop:
        if since == None:
            Log.note("Get all records")
            result = es.search({
                # "query": {"match_all": {}},
                "query": {
                    "filtered": {
                        "filter": {"exists": {"field": primary_key}},
                        "query": {"match_all": {}}
                    }},
                "fields": ["_id", primary_key],
                "from": 0,
                "size": BATCH_SIZE,
                "sort": [primary_key]
            })
        else:
            Log.note("Get records with {{primary_key}} >= {{max_time|datetime}}", primary_key=primary_key, max_time=since)
            result = es.search({
                "query": {"filtered": {
                    "query": {"match_all": {}},
                    "filter": {"range": {primary_key: {"gte": since}}},
                }},
                "fields": ["_id", primary_key],
                "from": 0,
                "size": BATCH_SIZE,
                "sort": [primary_key]
            })

        new_max_value = MAX([unwraplist(h.fields[literal_field(primary_key)]) for h in result.hits.hits])

        if since == new_max_value:
            # GET ALL WITH THIS TIMESTAMP
            result = es.search({
                "query": {"filtered": {
                    "query": {"match_all": {}},
                    "filter": {"term": {primary_key: since}},
                }},
                "fields": ["_id", primary_key],
                "from": 0,
                "size": 100000
            })
            since = new_max_value + 0.5
        else:
            since = new_max_value

        ids = result.hits.hits._id
        Log.note("Adding {{num}} to pending queue", num=len(ids))
        pending_bugs.extend(ids)

        if len(result.hits.hits) < BATCH_SIZE:
            pending_bugs.add(Thread.STOP)
            break

    Log.note("No more ids")


def replicate(source, destination, pending, fixes, please_stop):
    """
    COPY source RECORDS TO destination
    """

    def fixer(_source):
        for k, v in _source.items():
            locals()[k] = v

        for k, f in fixes.items():
            try:
                _source[k] = eval(f)
            except Exception, e:
                Log.alert("not evaluated {{expression}}", expression=f, cause=e)

        return _source


    for g, docs in qb.groupby(pending, max_size=BATCH_SIZE):
        if please_stop:
            break

        with Timer("Replicate {{num_docs}} bug versions", {"num_docs": len(docs)}):
            data = source.search({
                "query": {"filtered": {
                    "query": {"match_all": {}},
                    "filter": {"terms": {"_id": set(docs)}}
                }},
                "from": 0,
                "size": 200000,
                "sort": []
            })

            destination.extend([{"id": h._id, "value": fixer(h._source)} for h in data.hits.hits])

    Log.note("Done replication")


def main(settings):
    global BATCH_SIZE

    current_time = Date.now()
    time_file = File(settings.last_replication_time)

    # SYNCH WITH source ES INDEX
    source = elasticsearch.Index(settings.source)
    destination = elasticsearch.Cluster(settings.destination).get_or_create_index(settings.destination)

    # GET LAST UPDATED
    if settings.since != None:
        last_updated = Date(settings.since).unix
    else:
        last_updated = get_last_updated(destination, settings.primary_field)

    if settings.batch_size:
        BATCH_SIZE = settings.batch_size

    Log.note("updating records with {{primary_field}}>={{last_updated}}", last_updated=last_updated, primary_field=settings.primary_field)

    please_stop = Signal()
    done = Signal()

    def worker(please_stop):
        pending = Queue("pending ids")

        pending_thread = Thread.run(
            "get pending",
            get_pending,
            es=source,
            since=last_updated,
            pending_bugs=pending,
            primary_key=settings.primary_field,
            please_stop=please_stop
        )
        replication_thread = Thread.run(
            "replication",
            replicate,
            source,
            destination,
            pending,
            settings.fix,
            please_stop=please_stop
        )
        pending_thread.join()
        replication_thread.join()
        done.go()
        please_stop.go()

    Thread.run("wait for replication to finish", worker, please_stop=please_stop)
    Thread.wait_for_shutdown_signal(please_stop=please_stop)

    if done:
        Log.note("done all")
        # RECORD LAST UPDATED< IF WE DID NOT CANCEL OUT
        time_file.write(unicode(current_time.milli))


def start():
    global hg
    _ = wrap

    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)
        if settings.hg:
            hg = HgMozillaOrg(settings.hg)
        main(settings)
    except Exception, e:
        Log.error("Problems exist", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    start()
