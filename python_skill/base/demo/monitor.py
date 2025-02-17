# /bin/python3

import os
import subprocess
import sys

command_get_offset = "kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list kafka01:9092 --topic  "


## 读取配置，创建一个结果字典
def createResultDict(path):
    file = open(path)
    topicOldOffset = {}
    info = file.readline()
    while info:
        infos = info.replace("\n", "").split(":")
        if (len(infos) == 3):
            topicOldOffset.__setitem__(infos[0] + ":" + infos[1], infos[2])
        info = file.readline()
    file.close()
    return topicOldOffset


def readTopics(path):
    file = open(path)
    info = file.readline()
    topics = []
    while info:
        if (len(info) > 0):
            topics.append(info.replace("\n", ""))
        info = file.readline()
    return topics


## 执行命令
def runCommand(command):
    res = os.system(command)
    return res


def runCommand(command: list):
    result = subprocess.run(command, stdout=subprocess.PIPE).stdout
    return result


def createCommand(command: str, topics: list):
    commands = command.strip().split(" ")
    commandAll = []
    for topic in topics:
        commandn = commands.copy()
        commandn.append(topic)
        commandAll.append(commandn)
    return commandAll


def compareResult(resultDict: dict, result: str, statusDict: {}):
    res = result.split("\n")
    resKey = ""
    resVal = ""
    topic = ""
    for r in res:
        rr = r.split(":")
        if (len(rr) == 3):
            resKey = rr[0] + ":" + rr[1]
            resVal = rr[2]
            topic = rr[0]
            oldVal = resultDict.get(resKey)
            if (resVal != oldVal):
                resultDict.__setitem__(resKey, resVal)
                if (statusDict.get(topic) == None):
                    statusDict.__setitem__(topic, 1)


def writeResToFile(result: dict, path: str, tag: str):
    res = open(path, os.O_WRONLY)
    count = len(result.keys())
    c = 1;
    for r in result:
        c += 1
        res.write(r)
        if (tag == "status"):
            res.write(":")
        res.write(result.get(key=r))
        if (c != count):
            res.write("\n")


## TODO 1.创建结果字典 2. 创建待执行命令集 3. 执行命令，并获取执行结果 4. 进行结果比对，并生成状态字典 5 按比对结果执行任务 6. 持久化结果字典 7.持久化状态字典
if __name__ == '__main__':
    tpath = sys.argv[0]
    resultPath = sys.argv[1]
    statusPath = sys.argv[2]
    ## 1. 创建结果字典
result = createResultDict(resultPath)
## 2. 创建待执行命令集
topics = readTopics(tpath)
commands = createCommand(command_get_offset, topics)
statusDict = {}
## 执行命令
for command in commands:
    res = runCommand(command)
    ## 比对结果
    compareResult(resultDict=result, result=res, statusDict=statusDict)
writeResToFile(result=result, path=resultPath, tag="res")
writeResToFile(result=statusDict, path=statusPath, tag="status")
