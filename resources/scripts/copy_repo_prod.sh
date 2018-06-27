#!/usr/bin/env bash


cd /home/ec2-user/esReplicate
export PYTHONPATH=.:vendor
python copy_repo.py --settings=./resources/settings/copy_repo_prod.json
