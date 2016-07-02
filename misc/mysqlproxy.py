#!/usr/bin/env python3
##############################################################################
#The MIT License (MIT)
#
#Copyright (c) 2016 Hajime Nakagami
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in all
#copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#SOFTWARE.
##############################################################################
import sys
import time
import socket
import binascii
import select

def recv_mysql_packet(sock):
    head = sock.recv(4)
    if not head:
        return

    n = int.from_bytes(head[:3], byteorder='little')

    recieved = b''
    while n:
        bs = sock.recv(n)
        recieved += bs
        n -= len(bs)
    return head + recieved

def asc_dump(s):
    r = ''
    for c in s:
        r += chr(c) if (c >= 32 and c < 128) else '.'
    if r:
        print('   [' + r + ']')


def proxy_wire(server_name, server_port, listen_host, listen_port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((listen_host, listen_port))
    sock.listen(1)
    client_sock, addr = sock.accept()
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.connect((server_name, server_port))

    while True:
        server_data = recv_mysql_packet(server_sock)
        if server_data:
            client_sock.send(server_data)
            print('<<', binascii.b2a_hex(server_data).decode('ascii'))
            asc_dump(server_data)

        client_data = recv_mysql_packet(client_sock)
        if client_data:
            server_sock.send(client_data)
            print('>>', binascii.b2a_hex(client_data).decode('ascii'))
            asc_dump(client_data)
        time.sleep(1000)


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage : ' + sys.argv[0] + ' server[:port] [listen_host:]listen_port')
        sys.exit()

    server = sys.argv[1].split(':')
    server_name = server[0]
    if len(server) == 1:
        server_port = 3306
    else:
        server_port = int(server[1])

    listen = sys.argv[2].split(':')
    if len(listen) == 1:
        listen_host = 'localhost'
        listen_port = int(listen[0])
    else:
        listen_host = listen[0]
        listen_port = int(listen[1])

    proxy_wire(server_name, server_port, listen_host, listen_port)
