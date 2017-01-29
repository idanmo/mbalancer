#!/bin/bash

rm -rf experiment-1.0*

cp build/distributions/*.zip .
unzip -q -o *.zip

./experiment-1.0/bin/experiment "$@"


