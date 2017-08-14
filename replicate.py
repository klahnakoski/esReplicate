# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals


from datetime import timedelta, datetime

from mo_dots import wrap, unwraplist, literal_field
from mo_files import File
from mo_logs import startup, constants, Log
from mo_math import Math, MAX
from mo_threads import Queue, Thread, Signal, THREAD_STOP
from mo_times import Date
from mo_times.timer import Timer

from mo_hg.hg_mozilla_org import HgMozillaOrg
from pyLibrary.env import elasticsearch, http
from pyLibrary.queries import jx

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
        results_max = es.search({
            "query": {"match_all": {}},
            "from": 0,
            "size": 1,
            "sort": {config.primary_field: "desc"}
        })

        max_ = results_max.hits.hits[0]._source[config.primary_field]
        if isinstance(max_, unicode):
            pass
        elif Math.is_integer(max_):
            max_ = int(max_)
        return max_
    except Exception, e:
        Log.warning("Can not get_last_updated from {{host}}/{{index}}", {
            "host": es.settings.host,
            "index": es.settings.index
        }, e)
        return None


def get_pending(source, since, pending_bugs, please_stop):
    try:
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
                Log.note(
                    "Get records with {{primary_field}} >= {{max_time|datetime}}",
                    primary_field=config.primary_field,
                    max_time=since
                )
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
                    since = int(new_max_value) + 1
                elif Math.is_number(new_max_value):
                    since = float(new_max_value) + 0.5
                else:
                    since = unicode(new_max_value) + "a"
            else:
                since = new_max_value

            ids = result.hits.hits._id
            Log.note("Adding {{num}} to pending queue", num=len(ids))
            pending_bugs.extend(ids)

            if len(result.hits.hits) < BATCH_SIZE:
                break

        Log.note("No more ids")
    except Exception, e:
        please_stop.go()
        Log.error("Problem while copying records", cause=e)


def diff(source, destination, pending, please_stop):
    """
    SEARCH FOR HOLES IN DESTINATION
    :param source:
    :param destination:
    :param pending:  QUEUE TO FILL WITH MISSING RECORDS
    :return:
    """

    if config.diff == False:
        return

    # FIND source MIN/MAX
    results_max = source.search({
        "query": {"match_all": {}},
        "from": 0,
        "size": 1,
        "sort": {config.primary_field: "desc"}
    })

    results_min = source.search({
        "query": {"match_all": {}},
        "from": 0,
        "size": 1,
        "sort": {config.primary_field: "asc"}
    })

    if results_max.hits.total == 0:
        return

    _min = results_min.hits.hits[0]._source[config.primary_field]
    _max = results_max.hits.hits[0]._source[config.primary_field]

    def _copy(min_, max_):
        try:
            if please_stop:
                Log.note("Scanning was aborted")
                return

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
            Log.note(
                "Scan from {{min}} to {{max}}:  source={{source}}, dest={{dest}}, diff={{diff}}",
                min=min_,
                max=max_,
                source=len(source_ids),
                dest=len(destination_ids),
                diff=len(missing)
            )

            if missing:
                pending.extend(missing)
        except Exception, e:
            if min_ + 1 == max_:
                Log.warning("Scanning had a with field {{value||quote}} problem", value=min_, cause=e)
            else:
                mid_ = Math.round((min_+max_)/2, decimal=0)
                _copy(min_, mid_)
                _copy(mid_, max_)

    num_mismatches = [0]  # TRACK NUMBER OF MISMATCHES DURING REPLICATION

    def _partition(min_, max_):
        try:
            source_count = source.search({
                "query": {"filtered": {
                    "query": {"match_all": {}},
                    "filter": {"range": {config.primary_field: {"gte": min_, "lt": max_}}}
                }},
                "size": 0
            })

            if num_mismatches[0] < 10:
                # SOMETIMES THE TWO ARE TOO DIFFERENT TO BE OPTIMISTIC
                dest_count = destination.search({
                    "query": {"filtered": {
                        "query": {"match_all": {}},
                        "filter": {"range": {config.primary_field: {"gte": min_, "lt": max_}}}
                    }},
                    "size": 0
                })

                if source_count.hits.total == dest_count.hits.total:
                    return
                elif source_count.hits.total < 200000:
                    num_mismatches[0] += 1

            if source_count.hits.total < 200000:
                _copy(min_, max_)
            elif Math.is_number(min_) and Math.is_number(max_):
                mid_ = int(round((float(min_) + float(max_)) / 2, 0))
                # WORK BACKWARDS
                _partition(mid_, max_)
                _partition(min_, mid_)
            else:
                Log.error("can not split alphabetical in half")
        except Exception, e:
            Log.error("Scanning had a problem", cause=e)

    try:
        _partition(_min, _max)
    finally:
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
                if "Problem pulling pushlog" in e:
                    pass
                elif "can not find branch" in e:
                    pass
                else:
                    Log.warning("not evaluated {{expression}}", expression=f, cause=e)

        return _source

    for g, docs in jx.groupby(pending_ids, max_size=BATCH_SIZE):
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

    Log.note("updating records with {{primary_field}}>={{last_updated}}", last_updated=last_updated,
             primary_field=config.primary_field)

    please_stop = Signal()
    done = Signal()

    pending = Queue("pending ids", max=BATCH_SIZE*3, silent=False)

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
    pending.add(THREAD_STOP)
    try:
        replication_thread.join()
    except Exception, e:
        Log.warning("Replication thread failed", cause=e)
    done.go()
    please_stop.go()

    Log.note("done all")
    # RECORD LAST UPDATED, IF WE DID NOT CANCEL OUT
    time_file.write(unicode(current_time.milli))


def start():
    global hg
    global config
    _ = wrap

    try:
        config = startup.read_settings()
        with startup.SingleInstance(config.args.filename):
            constants.set(config.constants)
            Log.start(config.debug)
            if config.hg:
                hg = HgMozillaOrg(config.hg)
            main()
    except Exception, e:
        Log.warning("Problems exist", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    start()
