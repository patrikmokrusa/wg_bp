#!/bin/bash

cd src
sleep 5
python3 -u main.py join create_peer --run --ip 10.0.0.2 < ../test/join-exit_scenario.txt