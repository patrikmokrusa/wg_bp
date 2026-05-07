# wg_bp

This script is a tool to create mesh WireGuard networks, configure and synchronize state across peers.

You can eighter create or join a network.
To begin you start the program with initial arguments, and after that you can interact with it through TTY inputs.

## Requirements

Install [WireGuard](https://www.wireguard.com/install/) and install required libraries.

```bash
sudo apt install wireguard
pip install -r requirements.txt
```

Running the program requires root priviliges, because it interacts with network configuration.

## Creating a network

```bash
python3 main.py create --ip 10.0.0.1 --sync ALL
```

- create - subprogram
- --ip - virtual ip
- --sync - synchronization module selection (DHT|MQ|Gossip|ALL)

After you can enable discovery modules by:
```
discover-join
```

or

```
discover-broadcast
```

and start advertising them with:
```
advertise
```

## Joining existing network

You can join an existing network 3 different ways.
Each join method can be enabled by creating node as stated above.

You can start discovery modules just like the create peer after you join the network.

### Direct join

```bash
python3 main.py join <peer_ip> --ip 10.0.0.2
```

- join - direct join subprogram
- --ip - selected virtual ip

### Broadcast

```bash
python3 main.py broadcast --ip 10.0.0.3
```

- broadcast - broadcast subprogram
- --ip - selected virtual ip

### DNSSD

```bash
python3 main.py dnssd --ip 10.0.0.4
```

- dnssd - dnssd subprogram
- --ip - selected virtual ip

After you can select discovery method you want to join through by its index.

## Allowed IPs

While in a network you can add allowed_ips, that are accessable through your node for other peers to route traficc through you.
You can do this by typing:

```
add-allowed-ips
```

And to remove them:

```
remove-allowed-ips
```

## Leaving network

You can leave network by using Ctrl+C or by typing:

```
exit
```

## Limitations

Script only works when not behind SYMETRIC-NAT or behind network components that act like one. Port scanning is not implemented it uses the public ip and port discovered from STUN.
In theese situations you need to manualy add mapping to the wireguard port on your router.

If 2 nodes are in the same private network and want to connect, your router needs to support NAT loopback or Hairpin NAT so theese nodes are accesible through their public address even within their private network. The router doesnt try to find peers private ips.

DHT module sometimes struggles when repeatadly joining and leaving a network. Its because it takes a while to clearout its routing table from disconected peers.

## Customization

* src/state - STUN_SERVERS - You can change the queried public stun servers.
* src/state - CUSTOM_STUN_SERVERS - You can change the queried custom test server (test/stun/stun.py) or leave blank if you dont want to contact them.
* src/sync/gossip - DEGREE - You can define the degree of gossip (number of peers contacted in each cycle).

## Testing

Running tests requires having Docker and docker-compose installed.

You can see the logs by using Docker desktop or inspecting the containers directly.

### Synchronization test

To run this test first build the containers:

```bash
./test_sync.sh prep
```

and run the test using your inputed synchronization module. (DHT, MQ, Gossip, ALL)

```bash
./test_sync.sh all <chosen module>
```

To clean run:

```bash
./test_sync.sh delete
```

### NATed test

Network topology creation in docker containers requires Linux. I ran this test in Linux VM ubuntu 20.04.

First build the containers

```bash
docker-compose build
```

and run the test.

```bash
./test_nat.sh
```

To cleanup run:

```bash
./test_nat.sh clean
```

### Running program on localhost

To test manualy on localhost you need to run stun.py script and when running the peers use more arguments to avoid port and wireguard interface conflicts.

Run stun.py:

```bash
python3 test/stun/stun.py
```

Localhost example:

Run the first peer in a terminal and choose sync.

```bash
# first peer creating network using the ALL synchronization.
python3 src/main.py create --ip 10.0.0.1 --sync ALL
```

To enable discovery see [creating network](#creating-a-network).

Run the second peer from a different terminal using one of following:

```bash
# joining network using direct join.
python3 src/main.py join 127.0.0.1 --bootstrap-port 17777 --ip 10.0.0.2 --port 51821 --interface wg1 --sync-port 6888

# joining using broadcast
python3 src/main.py broadcast --ip 10.0.0.2 --port 51821 --interface wg1 --sync-port 6888

# joinging using dnssd
python3 src/main.py dnssd --ip 10.0.0.2 --port 51821 --interface wg1 --sync-port 6888
```

* localhost can only be run with 2 peers, because WireGuard doesnt allow same peer on 2 different interfaces

### Running program in test NAT enviroment

If you want to try the program out i provided a Linux VM image.

Once you are in the VM build the containers using

```bash
docker-compose -f docker-compose-norun.yaml build
```

and run them

```bash
./test_nat.sh norun
```

It runs the testing STUN server and starts the containers without running the program.

To interact with the containers use `docker ps` to show the name of containers and connect to them using

```bash
# connecting to client b
docker exec -it sf_wg_bp_client-b_1 bash
```

Once you are connected you can start the script.

To cleanup use

```bash
./test_nat.sh clean norun
```