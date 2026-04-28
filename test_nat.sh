#!/bin/bash
# created using method from this article: https://pancho.dev/posts/linux-router-with-containers/

if [ "$1" == "clean" ]; then
    echo "Cleaning up..."
    docker-compose down
    rm -rf /var/run/netns/*
    exit 0
fi

echo "This script only works on linux"

docker-compose up -d

echo "Setting up network for containers..."
# Get the container id for each container (will be needed later)
echo "Getting container IDs..."
create_id=$(docker ps --format '{{.ID}}' --filter name=create-peer)
stun_id=$(docker ps --format '{{.ID}}' --filter name=stun-server)
router_a_id=$(docker ps --format '{{.ID}}' --filter name=router-a)
client_a_id=$(docker ps --format '{{.ID}}' --filter name=client-a)
router_b_id=$(docker ps --format '{{.ID}}' --filter name=router-b)
client_b_id=$(docker ps --format '{{.ID}}' --filter name=client-b)
router_net_id=$(docker ps --format '{{.ID}}' --filter name=router-net)


# Get the containers pids which will be used to find their network namespace
echo "Getting container PIDs..."
create_pid=$(docker inspect -f '{{.State.Pid}}' ${create_id})
stun_pid=$(docker inspect -f '{{.State.Pid}}' ${stun_id})
router_a_pid=$(docker inspect -f '{{.State.Pid}}' ${router_a_id})
client_a_pid=$(docker inspect -f '{{.State.Pid}}' ${client_a_id})
router_b_pid=$(docker inspect -f '{{.State.Pid}}' ${router_b_id})
client_b_pid=$(docker inspect -f '{{.State.Pid}}' ${client_b_id})
router_net_pid=$(docker inspect -f '{{.State.Pid}}' ${router_net_id})
# create the /var/run/netns/ path if it doesn't already exist
mkdir -p /var/run/netns/


# Create a soft link to the containers network namespace to /var/run/netns/
echo "Creating soft links to the containers network namespaces..."
ln -sfT /proc/$create_pid/ns/net /var/run/netns/$create_id
ln -sfT /proc/$stun_pid/ns/net /var/run/netns/$stun_id
ln -sfT /proc/$router_a_pid/ns/net /var/run/netns/$router_a_id
ln -sfT /proc/$client_a_pid/ns/net /var/run/netns/$client_a_id
ln -sfT /proc/$router_b_pid/ns/net /var/run/netns/$router_b_id
ln -sfT /proc/$client_b_pid/ns/net /var/run/netns/$client_b_id
ln -sfT /proc/$router_net_pid/ns/net /var/run/netns/$router_net_id



# create interfaces
echo "Creating veth pairs..."
ip link add 'create-eth0' type veth peer name 'router-net-eth0'
ip link add 'stun-eth0' type veth peer name 'router-net-eth1'

ip link add 'router-a-eth1' type veth peer name 'router-net-eth2'
ip link add 'client-a-eth0' type veth peer name 'router-a-eth0'

ip link add 'router-net-eth3' type veth peer name 'router-b-eth1'
ip link add 'router-b-eth0' type veth peer name 'client-b-eth0'

# pass to the containers network namespaces
echo "Moving interfaces to the containers network namespaces..."
ip link set 'create-eth0' netns $create_id
ip link set 'router-net-eth0' netns $router_net_id
ip link set 'stun-eth0' netns $stun_id
ip link set 'router-net-eth1' netns $router_net_id

ip link set 'client-a-eth0' netns $client_a_id
ip link set 'router-a-eth0' netns $router_a_id
ip link set 'router-a-eth1' netns $router_a_id
ip link set 'router-net-eth2' netns $router_net_id

ip link set 'router-net-eth3' netns $router_net_id
ip link set 'router-b-eth1' netns $router_b_id
ip link set 'router-b-eth0' netns $router_b_id
ip link set 'client-b-eth0' netns $client_b_id

# rename the interfaces in the containers
echo "Renaming interfaces in the containers..."
ip netns exec $create_id ip link set 'create-eth0' name 'eth0'
ip netns exec $router_net_id ip link set 'router-net-eth0' name 'eth0'
ip netns exec $stun_id ip link set 'stun-eth0' name 'eth0'
ip netns exec $router_net_id ip link set 'router-net-eth1' name 'eth1'

ip netns exec $client_a_id ip link set 'client-a-eth0' name 'eth0'
ip netns exec $router_a_id ip link set 'router-a-eth0' name 'eth0'
ip netns exec $router_net_id ip link set 'router-net-eth2' name 'eth2'
ip netns exec $router_a_id ip link set 'router-a-eth1' name 'eth1'

ip netns exec $router_net_id ip link set 'router-net-eth3' name 'eth3'
ip netns exec $router_b_id ip link set 'router-b-eth1' name 'eth1'
ip netns exec $router_b_id ip link set 'router-b-eth0' name 'eth0'
ip netns exec $client_b_id ip link set 'client-b-eth0' name 'eth0'

# bring up the interfaces
echo "Bringing up interfaces..."
ip netns exec $create_id ip link set 'eth0' up
ip netns exec $router_net_id ip link set 'eth0' up
ip netns exec $stun_id ip link set 'eth0' up
ip netns exec $router_net_id ip link set 'eth1' up

ip netns exec $client_a_id ip link set 'eth0' up
ip netns exec $router_a_id ip link set 'eth0' up
ip netns exec $router_a_id ip link set 'eth1' up
ip netns exec $router_net_id ip link set 'eth2' up

ip netns exec $router_net_id ip link set 'eth3' up
ip netns exec $router_b_id ip link set 'eth1' up
ip netns exec $router_b_id ip link set 'eth0' up
ip netns exec $client_b_id ip link set 'eth0' up


# set the ip addresses
echo "Setting up IP addresses..."
ip netns exec $create_id ip addr add 172.20.3.2/24 dev eth0
ip netns exec $router_net_id ip addr add 172.20.3.1/24 dev eth0
ip netns exec $stun_id ip addr add 172.20.2.2/24 dev eth0
ip netns exec $router_net_id ip addr add 172.20.2.1/24 dev eth1

ip netns exec $client_a_id ip addr add 192.168.1.2/24 dev eth0
ip netns exec $router_a_id ip addr add 192.168.1.1/24 dev eth0
ip netns exec $router_a_id ip addr add 172.20.4.2/24 dev eth1
ip netns exec $router_net_id ip addr add 172.20.4.1/24 dev eth2

ip netns exec $router_net_id ip addr add 172.20.1.1/24 dev eth3
ip netns exec $router_b_id ip addr add 172.20.1.2/24 dev eth1
ip netns exec $router_b_id ip addr add 192.168.2.1/24 dev eth0
ip netns exec $client_b_id ip addr add 192.168.2.2/24 dev eth0


# # Now lets show the ip addresses in each contaier namespace
# ip netns exec $create_id ip a
# ip netns exec $stun_id ip a
# ip netns exec $router_a_id ip a
# ip netns exec $client_a_id ip a
# ip netns exec $router_b_id ip a
# ip netns exec $client_b_id ip a


# add default routes
echo "Adding default routes..."
ip netns exec $create_id ip route add default via 172.20.3.1 dev eth0
ip netns exec $stun_id ip route add default via 172.20.2.1 dev eth0
ip netns exec $router_b_id ip route add default via 172.20.1.1 dev eth1
ip netns exec $router_a_id ip route add default via 172.20.4.1 dev eth1

ip netns exec $client_a_id ip route add default via 192.168.1.1 dev eth0
ip netns exec $client_b_id ip route add default via 192.168.2.1 dev eth0


docker compose logs -f