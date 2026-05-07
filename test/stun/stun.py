# Autor: Patrik Mokruša (xmokrup00)
import json
import socket

print("Starting STUN server on port 9999...")

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
# sock.bind(("10.10.2.104", 9999))
sock.bind(("0.0.0.0", 9999))

while True:
    try:
        data, addr = sock.recvfrom(1024)
        print(f"Received message: {data.decode('utf-8')} from {addr[0]}:{addr[1]}")
        data = {
            "ip": addr[0],
            "port": addr[1]   
            }
        print(f"Sending response: {data} to {addr[0]}:{addr[1]}")
        send_data = json.dumps(data).encode('utf-8')
        sock.sendto(send_data, addr)
    except Exception as e:
        print(f"Error handling request: {e}")
        sock.close()
        exit(1)
