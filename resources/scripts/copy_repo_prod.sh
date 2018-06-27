#!/usr/bin/env bash


export PYTHONPATH=.:vendor
python copy_repo.py --settings=./resources/settings/copy_repo_prod.json
