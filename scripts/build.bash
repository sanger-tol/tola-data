#!/bin/bash

set -euxo pipefail

export MODULEPATH="/software/team311/jgrg/modules:$MODULEPATH"
module unload tola-data
module load uv

# Load a python and create virtualenv
cd -- "$( dirname -- "${BASH_SOURCE[0]}" )"
VERSION=$(basename -- "$(pwd)")
VENV_DIR="venv"
uv venv --prompt "tola-data $VERSION" "$VENV_DIR"
# shellcheck source=/dev/null
source "./$VENV_DIR/bin/activate"

uv pip install "git+https://github.com/sanger-tol/tola-data@v$VERSION"

# Copy the venv wrapper scripts for our commands to bin/
mkdir "bin"
commands="data-processed diff-mlwh fetch-mlwh-seq-data goat-client jv status-duckdb tol tqc"
for file in $commands
do
    cp "$VENV_DIR/bin/$file" "bin/$file"
done

# Download patched version of plot-bamstats until it is available in a samtools release
plot_bmsts="bin/plot-bamstats"
curl -s "https://raw.githubusercontent.com/jgrg/samtools/develop/misc/plot-bamstats" > "$plot_bmsts"
chmod +x "$plot_bmsts"

