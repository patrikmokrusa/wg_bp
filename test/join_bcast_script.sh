#!/bin/bash

cd src
sleep 20
python3 -u main.py broadcast --ip 10.0.0.3 < ../test/join-exit_scenario.txt