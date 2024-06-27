import os
import csv

def split_filename(filename):
    parts = filename.split('_')  # 使用下划线进行切割
    if len(parts) >= 4:
        name = parts[-1]  # 获取最后一个部分作为姓名
        score = parts[0]  # 获取第一个部分作为分数
        return name, score
    else:
        return None, None

def calculate_total_score(scores):
    total = 0
    for score in scores:
        score=score.replace('分', '')
        if(len(score)>0):
            total += float(score)
    return total

def process_files(folder_paths, output_file):
    data = {}

    # 遍历文件夹中的文件，将分数存储到字典中
    for folder_path in folder_paths:
        for filename in os.listdir(folder_path):
                name, score = split_filename(filename)
                if name and score:
                    if name in data:
                        data[name].append(score)  # 姓名已存在，添加分数
                    else:
                        data[name] = [score]  # 姓名不存在，创建新的分数列表

    # 计算总分并将数据写入CSV文件
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['姓名', '科目一', '科目二', '科目三', '总分'])  # 写入CSV文件的表头
        for name, scores in data.items():
            # 如果分数列表不足三个，用空字符串补齐
            scores.extend([''] * (3 - len(scores)))
            total_score = calculate_total_score(scores)
            writer.writerow([name] + [score.replace('分', '') for score in scores] + [total_score])

# 指定文件夹路径
folder_paths = ['/Users/kuiqwang/Desktop/初一期中考试/春秋/302', '/Users/kuiqwang/Desktop/初一期中考试/春秋/302-4', '/Users/kuiqwang/Desktop/初一期中考试/春秋/302-8']
output_file = '/Users/kuiqwang/Desktop/初一期中考试/春秋/整理4.csv'
# 处理文件夹中的文件，并输出到CSV文件
process_files(folder_paths, output_file)
