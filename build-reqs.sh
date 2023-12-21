#!/usr/bin/env bash
pex --python=python3 --inherit-path=prefer "$(pipenv requirements)" -o ./dist/jobs.pex -D .