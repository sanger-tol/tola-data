#!/bin/bash

set -euo pipefail

if [ $# -eq 1 ]
then
    msg=$1
else
    echo "Usage: alembic_autogenerate.sh <MESSAGE>"
    exit 2
fi

export DB_URI="postgresql://tolqc-dev@127.0.0.1:5435/tolqc"

# We assume that the `tolqc` git repository is checked out in
# the same directory as `tola-data`
migrations_folder="$(dirname "$0")/../../tolqc/tolqc-api/migrations"
cd "$migrations_folder"

conf_file="alembic.ini"
conf_file_bak="$conf_file.bak"
if [ ! -e "$conf_file_bak" ]
then
    perl -i.bak -pe 's{^(script_location\s*=\s*)/migrations/alembic}{${1}alembic}' "$conf_file"
fi
alembic --config alembic.ini revision --autogenerate -m "$msg"
mv "$conf_file_bak" "$conf_file"
