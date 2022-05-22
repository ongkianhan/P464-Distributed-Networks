#! /usr/bin/python3
import sys


x, y = input("Please write ./start_server.sh <listen-port> to start the server script.\n").split()

print("This is the value of x: ", x)
print("This is the value of y: ", y)

python3 server.py y

