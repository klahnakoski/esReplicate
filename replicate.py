# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from datetime import timedelta, datetime

import jx_elasticsearch
from jx_python import jx
from mo_dots import wrap
from mo_files import File
from mo_future import text_type
from mo_logs import startup, constants, Log
from mo_math import Math, MAX
from mo_threads import Queue, Thread, Signal, THREAD_STOP
from mo_times import Date, Timer
from pyLibrary.env import elasticsearch, http

# REPLICATION
#
# Replication has a few benefits:
# 2) Physical proximity decreases latency
# 3) The replicate can be configured with better hardware
# 4) The replicate's exclusivity increases availability (Mozilla's public cluster may have time of high load)

far_back = datetime.utcnow() - timedelta(weeks=52)
INSERT_BATCH_SIZE = 1
SCAN_BATCH_SIZE = 50000
http.ZIP_REQUEST = False
config = None


def get_last_updated(es):
    try:
        results_max = es.query({
            "select": [config.primary_field],
            "from": "repo",
            "sort": {config.primary_field: "desc"},
            "limit": 1,
            "format": "list"
        })

        max_ = results_max.data[0][config.primary_field]
        if isinstance(max_, unicode):
            pass
        elif Math.is_integer(max_):
            max_ = int(max_)
        return max_
    except Exception as e:
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
                result = source.query({
                    "select": ["_id", config.primary_field],
                    "from": "repo",
                    "where": {"exists": config.primary_field},
                    "sort": [config.primary_field],
                    "limit": INSERT_BATCH_SIZE,
                    "format": "list"
                })
            else:
                Log.note(
                    "Get records with {{primary_field}} >= {{max_time|datetime}}",
                    primary_field=config.primary_field,
                    max_time=since
                )
                result = source.query({
                    "select": ["_id", config.primary_field],
                    "from": "repo",
                    "where": {"gte": {config.primary_field: since}},
                    "sort": [config.primary_field],
                    "limit": INSERT_BATCH_SIZE,
                    "format": "list"
                })

            new_max_value = MAX(result.data[config.primary_field])

            if since == new_max_value:
                # GET ALL WITH THIS TIMESTAMP
                result = source.query({
                    "select": ["_id", config.primary_field],
                    "from": "repo",
                    "where": {"eq": {config.primary_field: since}},
                    "sort": [config.primary_field],
                    "limit": 100000,
                    "format": "list"
                })
                if Math.is_integer(new_max_value):
                    since = int(new_max_value) + 1
                elif Math.is_number(new_max_value):
                    since = float(new_max_value) + 0.5
                else:
                    since = text_type(new_max_value) + "a"
            else:
                since = new_max_value

            ids = result.data._id
            Log.note("Adding {{num}} to pending queue", num=len(ids))
            pending_bugs.extend(ids)

            if len(result.data) < INSERT_BATCH_SIZE:
                break

        Log.note("No more ids")
    except Exception as e:
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
    results = source.search({
        "select": [
            {"name": "max", "value": config.primary_field, "aggregate": "max"},
            {"name": "min", "value": config.primary_field, "aggregate": "min"}
        ],
        "from": "repo",
        "format": "list"
    }).data

    if len(results) == 0:
        return

    _max, _min = results[0].max, results[0].min

    def _copy(min_, max_):
        try:
            if please_stop:
                Log.note("Scanning was aborted")
                return

            source_result = source.query({
                "select": "_id",
                "from": "repo",
                "where": {"and": [
                    {"gte": {config.primary_field: min_}},
                    {"lt": {config.primary_field: max_}},
                ]},
                "limit": SCAN_BATCH_SIZE,
                "format": "list"
            })
            source_ids = set(source_result.data)

            destination_result = destination.query({
                "select": "_id",
                "from": "repo",
                "where": {"and": [
                    {"gte": {config.primary_field: min_}},
                    {"lt": {config.primary_field: max_}},
                ]},
                "limit": SCAN_BATCH_SIZE,
                "format": "list"
            })
            destination_ids = set(destination_result.data)

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
        except Exception as e:
            if min_ + 1 == max_:
                Log.warning("Scanning had a with field {{value||quote}} problem", value=min_, cause=e)
            else:
                mid_ = Math.round((min_ + max_) / 2, decimal=0)
                _copy(min_, mid_)
                _copy(mid_, max_)

    num_mismatches = [0]  # TRACK NUMBER OF MISMATCHES DURING REPLICATION

    def _partition(min_, max_):
        try:
            source_count = source.search({
                "select": {"aggregate": "count"},
                "from": "repo",
                "where": {"and": [
                    {"gte": {config.primary_field: min_}},
                    {"lt": {config.primary_field: max_}},
                ]},
                "format": "list"
            }).data

            if num_mismatches[0] < 10:
                # SOMETIMES THE TWO ARE TOO DIFFERENT TO BE OPTIMISTIC
                dest_count = destination.search({
                    "select": {"aggregate": "count"},
                    "from": "repo",
                    "where": {"and": [
                        {"gte": {config.primary_field: min_}},
                        {"lt": {config.primary_field: max_}},
                    ]},
                    "format": "list"
                }).data

                if source_count == dest_count:
                    return
                elif source_count < SCAN_BATCH_SIZE:
                    num_mismatches[0] += 1

            if source_count < SCAN_BATCH_SIZE:
                _copy(min_, max_)
            elif Math.is_number(min_) and Math.is_number(max_):
                mid_ = int(round((float(min_) + float(max_)) / 2, 0))
                # WORK BACKWARDS
                _partition(mid_, max_)
                _partition(min_, mid_)
            else:
                Log.error("can not split alphabetical in half")
        except Exception as e:
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
            except Exception as e:
                if "Problem pulling pushlog" in e:
                    pass
                elif "can not find branch" in e:
                    pass
                else:
                    Log.warning("not evaluated {{expression}}", expression=f, cause=e)

        return _source

    for g, docs in jx.groupby(pending_ids, max_size=INSERT_BATCH_SIZE):
        try:
            with Timer("Replicate {{num_docs}} documents", {"num_docs": len(docs)}):
                data = source.search({
                    "query": {"filtered": {
                        "query": {"match_all": {}},
                        "filter": {"terms": {"_id": set(docs)}}
                    }},
                    "from": 0,
                    "size": SCAN_BATCH_SIZE,
                    "sort": []
                })

                destination.extend([{"id": h._id, "value": fixer(h._source)} for h in data.hits.hits])
        except Exception as e:
            Log.warning("could not replicate batch", cause=e)

        if please_stop:
            break

    Log.note("Done replication")


def main():
    global INSERT_BATCH_SIZE

    current_time = Date.now()
    time_file = File(config.last_replication_time)
    # ENSURE THE DESTINATION EXISTS
    elasticsearch.Cluster(config.destination).get_or_create_index(config.destination)

    # SYNCH WITH source ES INDEX
    source = jx_elasticsearch.new_instance(config.source)
    destination = jx_elasticsearch.new_instance(config.destination)

    # GET LAST UPDATED
    if config.since != None:
        last_updated = Date(config.since).unix
    else:
        last_updated = get_last_updated(destination)

    if config.batch_size:
        INSERT_BATCH_SIZE = config.batch_size

    Log.note("updating records with {{primary_field}}>={{last_updated}}", last_updated=last_updated,
             primary_field=config.primary_field)

    please_stop = Signal()
    done = Signal()

    pending = Queue("pending ids", max=INSERT_BATCH_SIZE * 3, silent=False)

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
    except Exception as e:
        Log.warning("Replication thread failed", cause=e)
    done.go()
    please_stop.go()

    Log.note("done all")
    # RECORD LAST UPDATED, IF WE DID NOT CANCEL OUT
    time_file.write(text_type(current_time.milli))


def start():
    global config
    _ = wrap

    try:
        config = startup.read_settings()
        with startup.SingleInstance(config.args.filename):
            constants.set(config.constants)
            Log.start(config.debug)
            main()
    except Exception as e:
        Log.warning("Problems exist", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    start()
