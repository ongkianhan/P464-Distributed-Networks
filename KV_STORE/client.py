import socket
import time

HEADER = 1000
PORT = 9889
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "***!DISCONNECT***"
SERVER = "10.20.72.4"
ADDR = (SERVER, PORT)

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(ADDR)
CONNECTED = True



def send(msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)
    print(client.recv(1000).decode(FORMAT))
    print("\n")
    if msg == 'quit':
        client.shutdown(socket.SHUT_RDWR)
        global CONNECTED
        CONNECTED = False


while CONNECTED:
    time.sleep(0.30)
    print("-------------------------------------------------\n")
    print('Please enter one of three commands available: \n')
    print('get <key> to get a key-value pair from the database if possible\n')
    print('set <key> <value> to set a key-value pair in the database if not existing\n')
    print('quit to quit the program\n')
    send(input("Enter here: "))
    #print("**************************************************\n")

