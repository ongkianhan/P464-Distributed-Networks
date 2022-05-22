import socket
import time

# for eventual_server 1
HEADER = 1000
PORT = 55000

FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "***!DISCONNECT***"
FORWARD_MESSAGE = "*** Forwarding message to Primary Server ***"
ACKNOWLEDGE_MESSAGE_UPDATE = "*** Acknowledgement for update... ***"
ACKNOWLEDGE_MESSAGE_WRITE_COMPLETED = "*** Acknowledgement for completed write update... ***"
BACKUP_UPDATE_MESSAGE = "*** Updating Backup... ***"
QUIT_MESSAGE = 'quit'
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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

