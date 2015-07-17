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

from pyLibrary import convert
from pyLibrary.collections import MAX
from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.logs import Log
from pyLibrary.env import elasticsearch
from pyLibrary.env.files import File
from pyLibrary.queries import qb
from pyLibrary.thread.threads import Queue, Thread




# REPLICATION
#
# Replication has a few benefits:
# 1) The slave can have scripting enabled, allowing more powerful set of queries
# 2) Physical proximity increases the probability of reduced latency
# 3) The slave can be configured with better hardware
# 4) The slave's exclusivity increases availability (Mozilla's public cluster may have time of high load)
from pyLibrary.times.dates import Date
from pyLibrary.times.timer import Timer

far_back = datetime.utcnow() - timedelta(weeks=52)
BATCH_SIZE = 1000


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
        Log.error("Can not get_last_updated from {{host}}/{{index}}", {
            "host": es.settings.host,
            "index": es.settings.index
        }, e)


def get_pending(es, since, primary_key):
    pending_bugs = Queue("pending ids")

    def filler(please_stop, max_value):
        while not please_stop:
            Log.note("Get records with {{primary_key}} >= {{max_time|datetime}}", primary_key=primary_key, max_time=max_value)
            result = es.search({
                "query": {"filtered": {
                    "query": {"match_all": {}},
                    "filter": {"range": {primary_key: {"gte": unicode(max_value)}}},
                }},
                "fields": ["_id", primary_key],
                "from": 0,
                "size": BATCH_SIZE,
                "sort": [primary_key]
            })

            # try:
            #     max_time = MAX([float(h.fields[primary_key]) for h in result.hits.hits])
            # except Exception:
            max_value = MAX([h.fields[primary_key] for h in result.hits.hits])

            ids = result.hits.hits._id
            Log.note("Adding {{num}} to pending queue", num=len(ids))
            pending_bugs.extend(ids)

            if len(result.hits.hits) < BATCH_SIZE:
                pending_bugs.add(Thread.STOP)
                break

        Log.note("Source has {{num}} bug versions for updating", num=len(pending_bugs))

    Thread.run("get pending", target=filler, max_value=since)

    return pending_bugs


def replicate(source, destination, pending, fixes):
    """
    COPY source RECORDS TO destination
    """

    def fixer(_source):
        for k, v in _source.items():
            locals()[k] = v

        for k, f in fixes.items():
            _source[k] = eval(f)

        return _source


    for g, docs in qb.groupby(pending, max_size=BATCH_SIZE):
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


def main(settings):
    current_time = Date.now()
    time_file = File(settings.last_replication_time)

    # SYNCH WITH source ES INDEX
    source = elasticsearch.Index(settings.source)
    destination = elasticsearch.Cluster(settings.destination).get_or_create_index(settings.destination)

    # GET LAST UPDATED
    last_updated = get_last_updated(destination, settings.primary_field)
    Log.note("updating records with {{primary_field}}>={{last_updated}}", last_updated=last_updated, primary_field=settings.primary_field)

    pending = get_pending(source, last_updated, settings.primary_field)
    replicate(source, destination, pending, settings.fix)

    # RECORD LAST UPDATED
    time_file.write(unicode(current_time.milli))


def start():
    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)
        main(settings)
    except Exception, e:
        Log.error("Problems exist", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    start()
