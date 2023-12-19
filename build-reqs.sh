#!/usr/bin/env bash

#mkdir -p /opt/data
#wget http://files.grouplens.org/datasets/movielens/ml-1m.zip -O /tmp/ml-1m.zip
#unzip /tmp/ml-1m.zip -d /data

pex --python=python3 --inherit-path=prefer "$(pipenv requirements)" -o ./dist/jobs.pex -D .