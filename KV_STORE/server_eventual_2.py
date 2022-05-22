import socket
import threading
import time
import socketserver
from _thread import *
import os.path
import sys
from threading import Timer

HEADER = 1000
PORT = 53000
# SERVER = "10.20.72.4" //personal server
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "***!DISCONNECT***"
FORWARD_MESSAGE = "*** Forwarding message to Primary Server ***"
ACKNOWLEDGE_MESSAGE_UPDATE = "*** Acknowledgement for update... ***"
ACKNOWLEDGE_MESSAGE_WRITE_COMPLETED = "*** Acknowledgement for completed write update... ***"
BACKUP_UPDATE_MESSAGE = "*** Updating Backup... ***"
QUIT_MESSAGE = 'quit'
# print(SERVER)
ThreadCount = 0
mode = "Eventual"
# server_eventual_primary address is (SERVER, 9889) in this instance
##PRIMARY = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
##PRIMARY.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

PRIMARY_ADDR = (SERVER, 52000)
##PRIMARY.bind(PRIMARY_ADDR)

counter = 0
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(ADDR)

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
client.connect(PRIMARY_ADDR)


def multi_threaded_client(connection):
    connection.send(str.encode('Server is working: '))
    while True:
        data = connection.recv(2048)
        response = 'Sever message: ' + data.decode(FORMAT)
        if not data:
            break
        connection.sendall(str.encode(response))
    connection.close()


def get_key(key):  # key is a string
    file1 = open("database_eventual_r2.txt", "r")
    flag = 0
    index = 0
    keyLength = len(key)
    # Loop through the database, line by line
    for line in file1:
        if key in line:
            check_key = line[0: keyLength]
            if check_key == key:
                flag = 1
                break
            else:
                pass
        index += 1

    if flag == 0:
        print('Key: ', key, ' not found in database.\n')
        string = "Key: (" + key + ") not found in database.\n"
        file1.close()
        return string

    else:
        readThis = open("database_eventual_r2.txt")
        content = readThis.readlines()
        thisLine = content[index]
        print(thisLine)
        value = thisLine[keyLength:]
        value = value.strip("\n")
        print('Key: ', key, ' found in database.\n')
        s1 = "Key: (" + key + ") found in database.\n"
        print('VALUE ', value, ' ', str(len(value)), '\r\n')
        s2 = "VALUE " + value + " " + str(len(value)) + "\n"
        print('Data block is at line number ', str(index + 1), ' in file. \r\n')
        s3 = "\nData block is at line number " + str(index + 1) + " in file.\r\n"
        string = "{}{}{}".format(s1, s2, s3)
        file1.close()
        return string


def set_key(key, value, sleep):  # key and value are strings
    file1 = open('database_eventual_r2.txt', 'a+')
    flag = 0
    index = 0
    keyLength = len(key)

    if len(value) > HEADER:
        return "The size of your value (nbytes) is too big. Please use a smaller value. \n"

    else:
        # Loop through the database, line by line
        for line in file1:
            index += 1
            if key in line:
                check_key = line[0: keyLength]
                if check_key == key:
                    flag = 1
                    break
                else:
                    pass
        if flag == 0:
            print('Key: ', key, ' not found in database.\n')
            newline = key + ' ' + value + "\n"
            file1.write(newline)
            print("set " + str(HEADER) + " \r\n" + value + " \r\n")
            print('STORED\r\n')
            file1.close()
            time.sleep(sleep)
            return 'STORED\r\n'

        else:
            print('Key: ', key, ' found in database.\n')
            print('NOT-STORED\r\n')
            file1.close()
            return 'NOT-STORED\r\n'


def increment_counter():
    global counter
    counter = counter + 1


def forward_request_to_primary(msg):
    new_msg = "forward " + msg
    message = new_msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)
    print(client.recv(1000).decode(FORMAT))
    print("\n")
    # client.sendto(new_msg.encode(FORMAT), PRIMARY_ADDR)
    # time.sleep(10)
    # client.sendall(msg.encode(FORMAT))


def timer():
    while True:
        time.sleep(180)
        update_database()


def handle_client(conn, addr):
    increment_counter()
    print(f"[NEW CONNECTION] Client ID: {counter} with address: {addr} connected.")
    connected = True
    t = threading.Thread(target=timer, args=())
    t.start()
    while connected:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)

            if 'get' in msg:
                string = get_key(msg[4:])
                output = string + "END\r\n"
                conn.sendall(output.encode(FORMAT))

            elif 'set' in msg:
                forward_request_to_primary(msg)
                index = 0
                white = 0
                keystart = 0
                keyend = 0
                valuestart = 0
                for i in msg:
                    if i == " " and white == 0:
                        keystart = index + 1
                        white += 1
                        pass
                    if i == " " and white == 1:
                        keyend = index
                        valuestart = index + 1
                    index += 1
                key_string = msg[keystart:keyend]
                value_string = msg[valuestart:]
                print(key_string + " **** " + value_string)

                string = set_key(key_string, value_string, 5)
                conn.sendall(string.encode(FORMAT))

            elif msg == DISCONNECT_MESSAGE or msg == QUIT_MESSAGE:
                connected = False
                conn.sendall('This client is exiting the server. \n'.encode(FORMAT))
                server.close()

            else:
                output = "\nYour input is wrong. Please enter an acceptable input.\n"
                conn.sendall(output.encode(FORMAT))

    print(f"[{addr}] {msg}")
    conn.close()


def update_database():
    file_exists_master = os.path.isfile("database_master.txt")
    file_exists_replica2 = os.path.isfile("database_eventual_r2.txt")
    if file_exists_replica2 and file_exists_master:
        file_r2 = open("database_eventual_r2.txt", "w")
        file_m = open("database_master.txt", "r")
        while file_m:
            line = file_m.readline()
            file_r2.write(line)
            if line == "":
                break
        file_m.close()
        file_r2.close()
    display_msg()


def display_msg():
    print("Eventual consistency regular update complete... Time interval: 180 seconds")


def start(port_number):
    global PORT
    PORT = port_number
    file_exists = os.path.isfile("database_eventual_r2.txt")

    print("Server_eventual REPLICA 2 has connected to Server_eventual PRIMARY...")
    if file_exists:
        pass
    else:
        file = open("database_eventual_r2.txt", "a")
        file.close()
    print(f"[LISTENING] Server(EVENTUAL, REPLICA 2) is listening on server: {SERVER}, port: {PORT}")
    server.listen()
    while True:
        conn, addr = server.accept()
        # thread = threading.Thread(target=handle_client, args=(conn, addr))
        # thread.start()
        start_new_thread(handle_client, (conn, addr))
        global ThreadCount
        ThreadCount += 1
        print(f"[ACTIVE CONNECTIONS] {str(ThreadCount)}")
        """
        for i in range(0, 10):
            time.sleep(180)
            update_database()
        """


# # def(update) -> this reads from the master database_eventual_primary.txt and copies everything. set sleep to be 30
# seconds or so...


print("[Starting] server is starting... ")
start(PORT)
