#!/bin/bash

cd src
sleep 8
python3 -u main.py broadcast --run --ip 10.0.0.3 < ../test/join-exit_scenario.txt