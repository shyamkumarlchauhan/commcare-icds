#!/usr/bin/env bash
set -e

echo "CommCare Setup"
hq_branch=$(git ls-remote --heads https://github.com/dimagi/commcare-hq.git mk/icds-extract-aadhaar-number-2)
if [ -z "$hq_branch" ]; then
    echo "Using 'master' branch for commcare-hq clone."
else
    echo "Using 'mk/icds-extract-aadhaar-number-2' for commcare-hq clone."
    GIT_BRANCH_ARGS="--branch mk/icds-extract-aadhaar-number-2 --single-branch"
fi

CLONE_COMMAND="git clone https://github.com/dimagi/commcare-hq.git $GIT_BRANCH_ARGS $HOME/commcare --depth=1"
eval $CLONE_COMMAND

cd $HOME/commcare && git submodule update --init --recursive

mkdir -p $HOME/commcare/extensions/icds && mv $TRAVIS_BUILD_DIR/* $HOME/commcare/extensions/icds
mkdir -p $TRAVIS_BUILD_DIR/docker-volumes
