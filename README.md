# wg_bp

This script is a tool to create mesh WireGuard networks, configure them and synchronize state across peers.

You can eighter create or join a network.
To begin you start the program with initial arguments, and after that you can interact with it through TTY inputs.

- [wg\_bp](#wg_bp)
  - [Requirements](#requirements)
  - [Creating a network](#creating-a-network)
  - [Joining existing network](#joining-existing-network)
    - [Direct join](#direct-join)
    - [Broadcast](#broadcast)
    - [DNSSD](#dnssd)
  - [Allowed IPs](#allowed-ips)
  - [Leaving network](#leaving-network)
  - [Limitations](#limitations)
  - [Customization](#customization)
  - [Testing](#testing)
    - [Synchronization test](#synchronization-test)
    - [NATed test](#nated-test)
  - [Usage](#usage)
    - [Running program on localhost](#running-program-on-localhost)
    - [Running program in test NAT enviroment](#running-program-in-test-nat-enviroment)

## Requirements

Install [WireGuard](https://www.wireguard.com/install/) and install required libraries.

```bash
sudo apt install wireguard
pip install -r requirements.txt
```

Running the program requires root priviliges, because it interacts with network configuration.

## Creating a network

```bash
python3 main.py create --ip 10.0.0.1 --prefix 24 --port 51820 --interface wg-0 --sync-port 6881 --interval 1 --forwarded-port --endpoint 127.0.0.1:51820 --sync ALL
```

**bold** are mandatory

- **create** - subprogram
- **--ip** - virtual ip
- --prefix - default 24 - Wg interface mask, combines with ip 
- --port - default 51820 - WireGuard port
- --interface - default wg0 - WireGuard interface name
- --sync-port - default 6881 - synchronization port, used for synchronization modules communication
- --interval - default 1 - synchronization interval in seconds
- --forwarded-port - default False - if set, the program will override the port discovered from STUN
- --endpoint - default None - if set, the program will set the endpoint to this value instead of using STUN
- **--sync** - synchronization module selection (DHT|MQ|Gossip|ALL)

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
python3 main.py join <peer_ip> --bootstrap-port 17777 --ip 10.0.0.2 --prefix 24 --port 51821 --interface wg-0 --sync-port 6881 --interval 1 --forwarded-port --endpoint 127.0.0.1:51820
```

**bold** are mandatory

- **join** - direct join subprogram
- **<peer_ip>** - public IP of peer to join through
- --bootstrap-port - default 17777 - port of the peer to join through
- **--ip** - selected virtual ip
- --prefix - default 24 - Wg interface mask, combines with ip
- --port - default 51820 - WireGuard port
- --interface - default wg0 - WireGuard interface name
- --sync-port - default 6881 - synchronization port, used for synchronization modules communication
- --interval - default 1 - synchronization interval in seconds
- --forwarded-port - default False - if set, the program will override the port discovered from STUN
- --endpoint - default None - if set, the program will set the endpoint to this value instead of using STUN

### Broadcast

```bash
python3 main.py broadcast --ip 10.0.0.3 --bootstrap-port 18888 --ip 10.0.0.2 --prefix 24 --port 51821 --interface wg-0 --sync-port 6881 --interval 1 --forwarded-port --endpoint 127.0.0.1:51820
```

**bold** are mandatory

- **broadcast** - broadcast subprogram
- **--ip** - selected virtual ip
- --bootstrap-port - default 18888 - port to broadcast on
- **--ip** - selected virtual ip
- --prefix - default 24 - Wg interface mask, combines with ip
- --port - default 51820 - WireGuard port
- --interface - default wg0 - WireGuard interface name
- --sync-port - default 6881 - synchronization port, used for synchronization modules communication
- --interval - default 1 - synchronization interval in seconds
- --forwarded-port - default False - if set, the program will override the port discovered from STUN
- --endpoint - default None - if set, the program will set the endpoint to this value instead of using STUN

### DNSSD

```bash
python3 main.py dnssd --ip 10.0.0.4 --prefix 24 --port 51821 --interface wg-0 --sync-port 6881 --interval 1 --forwarded-port --endpoint 127.0.0.1:51820
```

**bold** are mandatory

- **dnssd** - dnssd subprogram
- **--ip** - selected virtual ip
- --prefix - default 24 - Wg interface mask, combines with ip
- --port - default 51820 - WireGuard port
- --interface - default wg0 - WireGuard interface name
- --sync-port - default 6881 - synchronization port, used for synchronization modules communication
- --interval - default 1 - synchronization interval in seconds
- --forwarded-port - default False - if set, the program will override the port discovered from STUN
- --endpoint - default None - if set, the program will set the endpoint to this value instead of using STUN

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

Script only works when not behind SYMETRIC-NAT (endpoint dependant NAT) or behind network components that act like one. Port scanning is not implemented it uses the public ip and port discovered from STUN.
In theese situations you need to manualy add mapping to the wireguard port on your router and use the `--forwarded-port` or `--endpoint` arguments.

If 2 nodes are in the same private network and want to connect, your router needs to support NAT loopback or Hairpin NAT so theese nodes are accesible through their public address even within their private network. The router doesnt try to find peers private ips.

DHT module sometimes struggles when repeatadly joining and leaving a network. Its because it takes a while to clear out its routing table from disconected peers.

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

## Usage

When you run the program you need to make sure you eighter have Endpoint independant NAT or you have to manually forward the wireguard port and use the:

* `--forwarded-port` argument to override the port discovered from STUN.

or

* `--endpoint` argument to set the forwarded endpoint manualy.


### Running program on localhost

To test manualy on localhost you need to run stun.py script and when running the peers use more arguments to avoid port and wireguard interface conflicts. You can run your stun or set the endpoints manualy using the `--endpoint` argument, but the stun server is provided.

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

If you want to try out the program in izolated envirometnment with simulated full cone NAT, you can use the provided docker-compose file.

```bash
docker-compose -f docker-compose-norun.yaml build
```

and run them (only works on Linux because of network configuration)

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
