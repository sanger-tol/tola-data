#!/bin/bash

set -euxo pipefail

# Load a python and create virtualenv
module load "ISG/python"
cd -- "$( dirname -- "${BASH_SOURCE[0]}" )"
VERSION=$(basename -- "$(pwd)")
VENV_DIR="venv"
if [ ! -d "$VENV_DIR" ]
then
	python3 -m venv "$VENV_DIR" --prompt "tola-data $VERSION"
fi
# shellcheck source=/dev/null
source "./$VENV_DIR/bin/activate"
pip install --upgrade pip

# Install ToL Python software
GITLAB="gitlab.internal.sanger.ac.uk"
GIT_CLONE="git -c advice.detachedHead=false clone --depth 1"

$GIT_CLONE "git@$GITLAB:tol/platforms/tolqc.git" --branch "tola-1.0.7"
pip install --requirement ./tolqc/tolqc-api/app/requirements.txt
pip install ./tolqc/tolqc-api/app
rm -rf tolqc
$GIT_CLONE "git@$GITLAB:jgrg/tola-data.git" --branch "$VERSION"
pip install ./tola-data
rm -rf tola-data

# Copy the venv wrapper scripts for our commands to bin/
mkdir "bin"
commands="diff-mlwh fetch-mlwh-seq-data status-duckdb tqc"
for file in $commands
do
    cp "$VENV_DIR/bin/$file" "bin/$file"
done

# Download duckdb command line binary and move to bin/
duckdb_url="https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-amd64.zip"
duckdb_zip="duckdb_cli.zip"
curl -L "$duckdb_url" > "$duckdb_zip"
unzip "$duckdb_zip"
mv "duckdb" "bin/duckdb"
rm "$duckdb_zip"
