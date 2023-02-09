import random
import json
print('I\'m ok')
print(r"I'm ok")
print(r'''hello,\n world''')
print('%2d-%03d' % (3, 111))
print('%.2f' % 3.1415926)


while True:
    # info=input("")
    carid = "carNo-"
    number = random.randint(0, 100)
    carInfo={}
    caridj = carid + str(number)
    carInfo.__setitem__(caridj,1)
    json.dumps(carInfo)


