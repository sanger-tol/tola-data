#!/bin/bash

set -euo pipefail

if [ $# -eq 1 ]
then
    msg=$1
else
    echo "Usage: alembic_autogenerate.sh <MESSAGE>"
    exit 2
fi

export DB_URI="postgresql://sts-dev:build-that-sts@127.0.0.1:5435/tolqc"
cd ~/git/tolqc/tolqc-api/migrations
conf_file="alembic.ini"
perl -i.bak -pe 's{^(script_location\s*=\s*)/migrations/alembic}{${1}alembic}' "$conf_file"
alembic --config alembic.ini revision --autogenerate -m "$msg"
mv "$conf_file.bak" "$conf_file"
