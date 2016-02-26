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
# 1) The replicate can have scripting enabled, allowing more powerful set of queries
# 2) Physical proximity increases latency
# 3) The replicate can be configured with better hardware
# 4) The replicate's exclusivity increases availability (Mozilla's public cluster may have time of high load)

far_back = datetime.utcnow() - timedelta(weeks=52)
BATCH_SIZE = 1000
http.ZIP_REQUEST = False
hg = None
config = None

def get_last_updated(es):
    try:
        results = es.search({
            "query": {"match_all": {}},
            "from": 0,
            "size": 0,
            "facets": {"last_time": {"statistical": {"field": config.primary_field}}}
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


def get_pending(source, since, pending_bugs, please_stop):
    while not please_stop:
        if since == None:
            Log.note("Get all records")
            result = source.search({
                # "query": {"match_all": {}},
                "query": {
                    "filtered": {
                        "filter": {"exists": {"field": config.primary_field}},
                        "query": {"match_all": {}}
                    }},
                "fields": ["_id", config.primary_field],
                "from": 0,
                "size": BATCH_SIZE,
                "sort": [config.primary_field]
            })
        else:
            Log.note("Get records with {{primary_field}} >= {{max_time|datetime}}", primary_field=config.primary_field, max_time=since)
            result = source.search({
                "query": {"filtered": {
                    "query": {"match_all": {}},
                    "filter": {"range": {config.primary_field: {"gte": since}}},
                }},
                "fields": ["_id", config.primary_field],
                "from": 0,
                "size": BATCH_SIZE,
                "sort": [config.primary_field]
            })

        new_max_value = MAX([unwraplist(h.fields[literal_field(config.primary_field)]) for h in result.hits.hits])

        if since == new_max_value:
            # GET ALL WITH THIS TIMESTAMP
            result = source.search({
                "query": {"filtered": {
                    "query": {"match_all": {}},
                    "filter": {"term": {config.primary_field: since}},
                }},
                "fields": ["_id", config.primary_field],
                "from": 0,
                "size": 100000
            })
            if Math.is_integer(new_max_value):
                since = int(new_max_value)+1
            elif Math.is_number(new_max_value):
                since = float(new_max_value)+0.5
            else:
                since = unicode(new_max_value) + "a"
        else:
            since = new_max_value

        ids = result.hits.hits._id
        Log.note("Adding {{num}} to pending queue", num=len(ids))
        pending_bugs.extend(ids)

        if len(result.hits.hits) < BATCH_SIZE:
            pending_bugs.add(Thread.STOP)
            break

    Log.note("No more ids")


def diff(source, destination, pending, please_stop):
    """
    SEARCH FOR HOLES IN DESTINATION
    :param source:
    :param destination:
    :param pending:  QUEUE TO FILL WITH MISSING RECORDS
    :return:
    """

    if not config.primary_interval:
        Log.warning("Require a `primary_interval` to hunt down possible holes in the destination.")
        return

    # FIND source MIN/MAX
    results = source.search({
        "query": {"match_all": {}},
        "from": 0,
        "size": 0,
        "facets": {"last_time": {"statistical": {"field": config.primary_field}}}
    })

    if results.facets.last_time.count == 0:
        return

    for min_, max_ in qb.intervals(results.facets.last_time.min, results.facets.last_time.max, config.primary_interval):
        try:
            if please_stop:
                Log.note("Scanning was aborted")
                return

            Log.note("Scan from {{min}} to {{max}}", min=min_, max=max_)
            source_result = source.search({
                "query": {"filtered": {
                    "query": {"match_all": {}},
                    "filter": {"range": {config.primary_field: {"gte": min_, "lt": max_}}}
                }},
                "fields": ["_id"],
                "from": 0,
                "size": 200000
            })
            source_ids = set(source_result.hits.hits._id)

            destination_result = destination.search({
                "query": {"filtered": {
                    "query": {"match_all": {}},
                    "filter": {"range": {config.primary_field: {"gte": min_, "lt": max_}}}
                }},
                "fields": ["_id"],
                "from": 0,
                "size": 200000
            })
            destination_ids = set(destination_result.hits.hits._id)

            missing = source_ids - destination_ids

            if missing:
                pending.extend(missing)
        except Exception, e:
            Log.warning("Scanning had a problem", cause=e)

    Log.note("Done scanning for holes")


def replicate(source, destination, pending_ids, fixes, please_stop):
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


    for g, docs in qb.groupby(pending_ids, max_size=BATCH_SIZE):
        with Timer("Replicate {{num_docs}} documents", {"num_docs": len(docs)}):
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

        if please_stop:
            break


    Log.note("Done replication")


def main():
    global BATCH_SIZE

    current_time = Date.now()
    time_file = File(config.last_replication_time)

    # SYNCH WITH source ES INDEX
    source = elasticsearch.Index(config.source)
    destination = elasticsearch.Cluster(config.destination).get_or_create_index(config.destination)

    # GET LAST UPDATED
    if config.since != None:
        last_updated = Date(config.since).unix
    else:
        last_updated = get_last_updated(destination)

    if config.batch_size:
        BATCH_SIZE = config.batch_size

    Log.note("updating records with {{primary_field}}>={{last_updated}}", last_updated=last_updated, primary_field=config.primary_field)

    please_stop = Signal()
    done = Signal()

    def worker(please_stop):
        pending = Queue("pending ids")

        pending_thread = Thread.run(
            "get pending",
            get_pending,
            source=source,
            since=last_updated,
            pending_bugs=pending,
            please_stop=please_stop
        )
        diff_thread = Thread.run(
            "diff",
            diff,
            source,
            destination,
            pending,
            please_stop=please_stop
        )
        replication_thread = Thread.run(
            "replication",
            replicate,
            source,
            destination,
            pending,
            config.fix,
            please_stop=please_stop
        )
        pending_thread.join()
        diff_thread.join()
        pending.add(Thread.STOP)
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
    global config
    _ = wrap

    try:
        config = startup.read_settings()
        constants.set(config.constants)
        Log.start(config.debug)
        if config.hg:
            hg = HgMozillaOrg(config.hg)
        main()
    except Exception, e:
        Log.error("Problems exist", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    start()
