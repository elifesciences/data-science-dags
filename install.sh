#!/bin/bash

set -e
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

pip install --user -r $DIR/requirements.pipelines.txt
pip install --user -r $DIR/requirements.prophet.txt
pip install -e $DIR/ --user
cp $DIR/notebooks $2/ -r
