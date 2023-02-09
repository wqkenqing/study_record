#!/usr/bin/python
# -*- coding: UTF-8 -*-

import socket
import json
import random
import time
s = socket.socket()
s.bind(("localhost", 8883))
s.listen(5)
c, addr = s.accept()
print('连接地址：', addr)
carid="carNo-"
while True:
    # info=input("")
    carid = "carNo-"
    number = random.randint(0, 100)
    carInfo={}
    caridj = carid + str(number)
    carInfo.__setitem__(caridj,1)
    c.send(json.dumps(carInfo).encode())
    c.send("\n".encode())
    time.sleep(3)
c.close()

