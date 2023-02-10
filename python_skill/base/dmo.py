from bs4 import BeautifulSoup
import json
import urllib.request

# 打开美团重庆巫山的地址
page = urllib.request.urlopen('https://hotel.meituan.com/chongqing-wushan')
html_source = page.read().decode('utf8')

# 解析网页源码
soup = BeautifulSoup(html_source, "html.parser")

# 获取所有酒店信息
res_list = soup.find_all('div', class_="js-deal-tile")

# 保存信息
info_data = []

# 遍历表格
for res_temp in res_list:
    # 获取酒店名称
    name = res_temp.attrs["title"]
    # 获取酒店评分
    score = res_temp.find('p', class_="hotel-comment-score")
    score = score.attrs["data-score"]
    # 获取酒店地址
    address = res_temp.find('p', class_="hotel-position")
    address = address.attrs["title"]
    # 保存酒店信息
    data = {
        "name": name,
        "score": score,
        "address": address
    }
    info_data.append(data)

# 以json格式写入文件
with open('data.json', 'a+') as f:
    json.dump(info_data, f)
