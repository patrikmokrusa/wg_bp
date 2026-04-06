#!/bin/bash

cd src
python3 -u main.py create --run --ip 10.0.0.1 --sync $1 < ../test/create-scenario.txt