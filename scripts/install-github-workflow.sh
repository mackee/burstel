#!/usr/bin/env bash
#
# install-github-workflow.sh - install the CI workflow into .github/workflows/.
#
# The managed push pipeline's GitHub App lacks the `workflows` permission, so
# files under .github/workflows/ must be installed by an account/app that has
# it (or through the GitHub UI). This script only copies the checked-in source;
# run it from a checkout where you can push workflow files.
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="$REPO_ROOT/ci/github-actions/container-image.yml"
DEST="$REPO_ROOT/.github/workflows/container-image.yml"
mkdir -p "$(dirname "$DEST")"
cp "$SRC" "$DEST"
echo "installed $DEST" >&2
echo "next: git add .github/workflows/container-image.yml && git commit && git push" >&2
