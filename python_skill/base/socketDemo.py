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
carid = "carNo-"
while True:
    # info=input("")
    carid = "carNo-"
    number = random.randint(0, 100)
    carInfo = {}
    ## 车牌
    caridj = carid + str(number)
    ##事件时间
    millis = int(round(time.time() * 1000))
    ##封状CarInfo，并转成json字符串
    carInfo.__setitem__("carNumber", caridj)
    carInfo.__setitem__("carSpeed", random.randint(0, 100))
    carInfo.__setitem__("eventTime", millis)
    c.send(json.dumps(carInfo).encode())
    c.send("\n".encode())
    time.sleep(1)
c.close()
