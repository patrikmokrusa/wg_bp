# wg_bp

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
python3 main.py join create_peer --ip 10.0.0.2
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

## Leaving network

You can leave network by using Ctrl+C or by typing:

```
exit
```

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
./test_sync.sh all <your module>
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
