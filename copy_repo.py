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

from mo_hg.hg_mozilla_org import DEFAULT_LOCALE

import jx_elasticsearch
from mo_dots import wrap, coalesce
from mo_files import File
from mo_future import text_type
from mo_logs import startup, constants, Log
from mo_threads import Queue, Thread, Signal, THREAD_STOP
from mo_times import Date, DAY
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






def main():
    global INSERT_BATCH_SIZE

    # ENSURE THE DESTINATION EXISTS
    destination = elasticsearch.Cluster(config.destination).get_or_create_index(config.destination)

    # SYNCH WITH source ES INDEX
    source = jx_elasticsearch.new_instance(config.source)


    for i in range(30):
        date = Date.today()-(i*DAY)
        result = source.query({
            "from": "repo",
            "where": {"and": [
                {"gte": {"push.date": date}},
                {"lt": {"push.date": date + DAY}}
            ]},
            "format": "list",
            "limit": 100000
        })

        if len(result.data) % 10000 ==0:
            Log.warning("too many records")

        bulk = [
            {
                "_id": coalesce(rev.changeset.id12, "") + "-" + rev.branch.name + "-" + coalesce(rev.branch.locale, DEFAULT_LOCALE),
                "value": rev
            }
            for rev in result.data

        ]

        Log.note("Add repo records for {{date|datetime}}", date=date)
        destination.extend(bulk)


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
