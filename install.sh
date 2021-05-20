#!/bin/bash

set -e
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

pip install --user -r $DIR/requirements.dag.txt
pip install --user -r $DIR/requirements.fbprophet.txt
pip install --user -r $DIR/requirements.notebook.txt
pip install -e $DIR/ --user
cp $DIR/dags $1 -r
cp $DIR/notebooks $2/ -r
