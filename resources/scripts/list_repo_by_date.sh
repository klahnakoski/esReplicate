#!/usr/bin/env bash


curl -XPOST http://54.149.21.8:9200/repo/_search -d '{"from": 0, "query": {"bool": {"filter": [{"exists": {"field": "changeset.date"}}]}},"size": 1}'  -H "Content-Type: application/json"
