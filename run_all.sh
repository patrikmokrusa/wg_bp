#!/bin/bash

BUILD_STUN="docker build -f test/stun/Dockerfile -t wg-stun ."
BUILD_WG="docker build -f test/Dockerfile -t wg-bp ."
CREATE_NET="docker network create mynet"
REMOVE_NET="docker network rm mynet"

DELETE_CONTAINERS="docker rm -f create_peer join_peer bcast_peer dnssd_peer stun"

RUN_STUN="docker run -d --privileged --network host --name stun wg-stun"
RUN_CREATE_PEER="docker run -d --entrypoint /bin/bash --privileged --network=mynet \
    --name create_peer wg-bp \
    /app/test/create_script.sh $2"
RUN_JOIN_PEER="docker run -d --entrypoint /bin/bash --privileged --network=mynet \
    --name join_peer wg-bp \
    /app/test/join_direct_script.sh"
RUN_BCAST_PEER="docker run -d --entrypoint /bin/bash --privileged --network=mynet \
    --name bcast_peer wg-bp \
    /app/test/join_bcast_script.sh"
RUN_DNSSD_PEER="docker run -d --entrypoint /bin/bash --privileged --network=mynet \
    --name dnssd_peer wg-bp \
    /app/test/join_dnssd_script.sh"

if [ "$#" == 0 ]; then
    echo "Usage: $0 [prep|delete|all|bcast|dnssd|join|create]"
    exit 1
fi

if [ "$1" == "prep-wg" ]; then
    echo "$BUILD_WG"
    $BUILD_WG
fi

if [ "$1" == "prep" ]; then
    echo "$BUILD_STUN"
    $BUILD_STUN
    echo "$BUILD_WG"
    $BUILD_WG
fi

if [ "$1" == "delete" ]; then
    echo "$DELETE_CONTAINERS"
    $DELETE_CONTAINERS
    echo "$REMOVE_NET"
    $REMOVE_NET
fi

if [ "$1" == "all" ]; then

    if [ "$#" -ne 2 ]; then
        echo "Usage: $0 all [DHT|Gossip|ALL|MQ]"
        exit 1
    fi
    echo "$CREATE_NET"
    $CREATE_NET

    echo "$RUN_STUN"
    $RUN_STUN 
    echo "$RUN_CREATE_PEER"
    $RUN_CREATE_PEER
    echo "$RUN_JOIN_PEER"
    $RUN_JOIN_PEER
    echo "$RUN_BCAST_PEER"
    $RUN_BCAST_PEER

    echo "Need some time to start dnssd discovery, waiting 15 seconds..."
    sleep 15
    
    echo "$RUN_DNSSD_PEER"
    $RUN_DNSSD_PEER
fi

if [ "$1" == "bcast" ]; then
    echo "$RUN_BCAST_PEER"
    $RUN_BCAST_PEER
fi

if [ "$1" == "dnssd" ]; then
    echo "$RUN_DNSSD_PEER"
    $RUN_DNSSD_PEER
fi

if [ "$1" == "join" ]; then
    echo "$RUN_JOIN_PEER"
    $RUN_JOIN_PEER
fi

if [ "$1" == "create" ]; then

    if [ "$#" -ne 2 ]; then
        echo "Usage: $0 create [DHT|Gossip|ALL|MQ]"
        exit 1
    fi

    echo "$RUN_CREATE_PEER"
    $RUN_CREATE_PEER
fi