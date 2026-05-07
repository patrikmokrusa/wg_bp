#!/bin/bash

cd src
sleep 30
python3 -u main.py dnssd --ip 10.0.0.4 < ../test/join-dnssd_scenario.txt