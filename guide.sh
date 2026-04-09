#!/bin/bash
# Get the container id for each container (will be needed later)
create_id=$(docker ps --format '{{.ID}}' --filter name=create-peer)
stun_id=$(docker ps --format '{{.ID}}' --filter name=stun-server)
router_a_id=$(docker ps --format '{{.ID}}' --filter name=router-a)
client_a_id=$(docker ps --format '{{.ID}}' --filter name=client-a)
router_b_id=$(docker ps --format '{{.ID}}' --filter name=router-b)
client_b_id=$(docker ps --format '{{.ID}}' --filter name=client-b)


# Get the containers pids which will be used to find their network namespace
create_pid=$(docker inspect -f '{{.State.Pid}}' ${create_id})
stun_pid=$(docker inspect -f '{{.State.Pid}}' ${stun_id})
router_a_pid=$(docker inspect -f '{{.State.Pid}}' ${router_a_id})
client_a_pid=$(docker inspect -f '{{.State.Pid}}' ${client_a_id})
router_b_pid=$(docker inspect -f '{{.State.Pid}}' ${router_b_id})
client_b_pid=$(docker inspect -f '{{.State.Pid}}' ${client_b_id})

# create the /var/run/netns/ path if it doesn't already exist
mkdir -p /var/run/netns/


# Create a soft link to the containers network namespace to /var/run/netns/
ln -sfT /proc/$create_pid/ns/net /var/run/netns/$create_id
ln -sfT /proc/$stun_pid/ns/net /var/run/netns/$stun_id
ln -sfT /proc/$router_a_pid/ns/net /var/run/netns/$router_a_id
ln -sfT /proc/$client_a_pid/ns/net /var/run/netns/$client_a_id
ln -sfT /proc/$router_b_pid/ns/net /var/run/netns/$router_b_id
ln -sfT /proc/$client_b_pid/ns/net /var/run/netns/$client_b_id


# Now lets show the ip addresses in each contaier namespace

ip netns exec $create_id ip a
ip netns exec $stun_id ip a
ip netns exec $router_a_id ip a
ip netns exec $client_a_id ip a
ip netns exec $router_b_id ip a
ip netns exec $client_b_id ip a
