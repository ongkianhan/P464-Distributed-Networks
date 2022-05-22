import socket
import threading
import time
import socketserver
from _thread import *
import os.path
import sys

HEADER = 1000
PORT = 52000
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

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(ADDR)

counter = 0

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
    file1 = open("database_master.txt", "r")
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
        readThis = open("database_master.txt")
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


def set_key(key, value):  # key and value are strings
    file1 = open('database_master.txt', 'a+')
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
            return 'STORED\r\n'

        else:
            print('Key: ', key, ' found in database.\n')
            print('NOT-STORED\r\n')
            file1.close()
            return 'NOT-STORED\r\n'


def increment_counter():
    global counter
    counter = counter + 1

def handle_client(conn, addr):
    increment_counter()
    print(f"[NEW CONNECTION] Eventual_REPLICA_{counter} with information: {addr} connected.")
    connected = True
    while connected:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)

            if 'get' in msg:
                string = get_key(msg[4:])
                output = string + "END\r\n"
                conn.sendall(output.encode(FORMAT))

            if 'set' in msg:
                index = 0
                white = 0
                keystart = 0
                keyend = 0
                valuestart = 0

                for i in msg:
                    if i == " " and white != 2:
                        keystart = index + 1
                        white += 1
                        pass
                    if i == " " and white == 2:
                        keyend = index
                        valuestart = index + 1
                    index += 1
                key_string = msg[keystart:keyend]
                value_string = msg[valuestart:]

                string = set_key(key_string, value_string)
                conn.sendall(string.encode(FORMAT))

            if msg == DISCONNECT_MESSAGE or msg == QUIT_MESSAGE:
                connected = False
                conn.sendall('This client is exiting the server. \n'.encode(FORMAT))

    print(f"[{addr}] {msg}")
    conn.close()

def display_msg():
    print("Eventual consistency regular update complete... Time interval: 45 seconds")

def start(port_number):
    global PORT
    PORT = port_number

    file_exists = os.path.isfile("database_master.txt")
    if file_exists:
        pass
    else:
        file = open("database_master.txt", "w")
        file.close()
    print(f"[LISTENING] Server(EVENTUAL, PRIMARY) is listening on server: {SERVER}, port: {PORT}")
    server.listen()
    while True:
        conn, addr = server.accept()
        # thread = threading.Thread(target=handle_client, args=(conn, addr))
        # thread.start()
        start_new_thread(handle_client, (conn, addr))
        global ThreadCount
        ThreadCount += 1
        print(f"[ACTIVE CONNECTIONS] {str(ThreadCount)}")



print("[Starting] server is starting... ")
start(PORT)
