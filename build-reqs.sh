#!/usr/bin/env -S bash -x

pex --python=python3 --inherit-path=prefer "$(pipenv requirements)" -o ./dist/jobs.pex -D .