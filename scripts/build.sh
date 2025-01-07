#!/bin/bash

set -euxo pipefail

# Load a python and create virtualenv
module unload tola-data
module load "ISG/python/3.12.3"
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

local_pypi="https://gitlab.internal.sanger.ac.uk/api/v4/projects/3429/packages/pypi/simple"
pip install --extra-index-url="$local_pypi" "tol-sdk[api_base2,postgresql] == 1.2.4"
$GIT_CLONE "git@$GITLAB:jgrg/tola-data.git" --branch "$VERSION"
pip install ./tola-data
rm -rf tola-data

# Copy the venv wrapper scripts for our commands to bin/
mkdir "bin"
commands="data-processed diff-mlwh fetch-mlwh-seq-data goat-client status-duckdb tqc"
for file in $commands
do
    cp "$VENV_DIR/bin/$file" "bin/$file"
done

# Download patched version of plot-bamstats until it is available in a samtools release
plot_bmsts="bin/plot-bamstats"
curl -s "https://raw.githubusercontent.com/jgrg/samtools/develop/misc/plot-bamstats" > "$plot_bmsts"
chmod +x "$plot_bmsts"

