import requests
import json

def read_data_from_file(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

def upload_to_elasticsearch(data, elasticsearch_url, index_name, batch_size):
    # 转换每个JSON对象并上传到Elasticsearch
    headers = {'Content-Type': 'application/json'}
    total_data = len(data)
    for i in range(0, total_data, batch_size):
        batch_data = data[i:i+batch_size]
        payload = ''
        for json_obj in batch_data:
            action = {
                "index": {
                    "_index": index_name,
                    "_type": "_doc"
                }
            }
            payload += '{}\n{}\n'.format(json.dumps(action), json.dumps(json_obj))

        # 使用Elasticsearch的_bulk API上传数据
        bulk_url = elasticsearch_url + '/_bulk'
        response = requests.post(bulk_url, data=payload, headers=headers)

        # 检查上传是否成功
        if response.status_code == 200:
            print('批次数据上传成功！')
        else:
            print('批次数据上传失败，错误信息：', response.text)

if __name__ == '__main__':
    # 输入数据文件路径
    file_path = '/Users/kuiqwang/work.txt'

    # 从文件中读取数据
    data = read_data_from_file(file_path)

    # 输入Elasticsearch URL和Index名称
    elasticsearch_url = 'http://192.168.3.9:9200'
    index_name = 'nas_files_new'  # 修改为您需要的Index名称

    # 指定每批上传的数据数量
    batch_size = 1000

    # 按批次上传数据
    upload_to_elasticsearch(data, elasticsearch_url, index_name, batch_size)
