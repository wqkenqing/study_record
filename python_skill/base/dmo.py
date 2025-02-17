from bs4 import BeautifulSoup
import json
import urllib.request


page = urllib.request.urlopen('http://39.105.221.200:4765/api/Shield/GetShield')
sheild_data = page.read().decode('utf8')
sheild= json.loads(sheild_data)
sdata=sheild["data"]
print(sdata)
tags={}
for data in sdata:
    if(tags.get(data['Tag'])==None):
        tags.__setitem__(data['Tag'],1)
    else:
        num=tags.__getitem__(data['Tag']);
        num+=1
        tags.__setitem__(data['Tag'],num)
print("key的长度为:")
print(len(tags.keys()))


















