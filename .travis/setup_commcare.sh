#!/usr/bin/env bash
set -e

echo "CommCare Setup"
hq_branch=$(git ls-remote --heads https://github.com/dimagi/commcare-hq.git $TRAVIS_PULL_REQUEST_BRANCH)
if [ -z "$hq_branch" ]; then
    echo "Using 'master' branch for commcare-hq clone."
else
    echo "Using '$TRAVIS_PULL_REQUEST_BRANCH' for commcare-hq clone."
    GIT_BRANCH_ARGS="--branch $TRAVIS_PULL_REQUEST_BRANCH --single-branch"
fi

CLONE_COMMAND="git clone https://github.com/dimagi/commcare-hq.git $GIT_BRANCH_ARGS $HOME/commcare --depth=1"
eval $CLONE_COMMAND

cd $HOME/commcare && git submodule update --init --recursive

mkdir -p $HOME/commcare/extensions/icds && mv $TRAVIS_BUILD_DIR/* $HOME/commcare/extensions/icds
mkdir -p $TRAVIS_BUILD_DIR/docker-volumes
