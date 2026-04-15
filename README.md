# wg_bp

You can eighter create or join a network.
To begin you start the program with initial arguments, and after that you can interact with it through TTY inputs.

## Creating a network

```bash
python3 main.py create --run --ip 10.0.0.1 --sync ALL
```

- create - subprogram
- --run - keep the program running
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
python3 main.py join create_peer --run --ip 10.0.0.2
```

- join - direct join subprogram
- --run - keep the program running
- --ip - selected virtual ip

### Broadcast

```bash
python3 -u main.py broadcast --run --ip 10.0.0.3
```

- broadcast - broadcast subprogram
- --run - keep the program running
- --ip - selected virtual ip

### DNSSD

```bash
python3 -u main.py dnssd --run --ip 10.0.0.4
```

- dnssd -  subprogram
- --run - keep the program running
- --ip - selected virtual ip

After you can select discovery method you want to join through by its index.

## Leaving network

You can leave network by typing:
```
exit
```

or if you want to keep the interface:
```
return
```
