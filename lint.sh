#!/usr/bin/env bash
set -euo pipefail

# Simple helper to run pylint using the local .venv.
# Usage:
#   ./lint.sh [pylint-args]
# Examples:
#   ./lint.sh
#   ./lint.sh --disable=C0114

if [ ! -d ".venv" ]; then
  echo "Creating local virtual environment at .venv"
  python3 -m venv .venv
fi

if [ ! -x ".venv/bin/pylint" ]; then
  echo "Installing dependencies and pylint into .venv"
  .venv/bin/python -m pip install --upgrade pip
  .venv/bin/pip install -r requirements.txt pylint
fi

FILES=$(git ls-files '*.py')
if [ -z "$FILES" ]; then
  echo "No Python files found to lint."
  exit 0
fi

exec .venv/bin/pylint "$@" $FILES
