#!/bin/bash

cd src
python3 -u main.py dnssd --run --ip 10.0.0.4 < ../test/join-dnssd_scenario.txt