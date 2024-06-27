# 本文件的主要作用是用来部署后监听、备份、自启等操作
import logging
import threading
from datetime import datetime, timedelta
import subprocess
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# TODO  1. archive.sh 2. zk_check.sh 3. monitor.sh
def file_backup(filename, source_path, dest_path, max_files=5):
    # 检查路径
    if not os.path.exists(source_path):
        logging.warning(f"Source path: {source_path} does not exist")
        return
    if not os.path.exists(dest_path):
        logging.warning(f"Destination path: {dest_path} does not exist")
        return

    # 生成文件名
    filename_normal = filename_generate(filename)
    dest_file = os.path.join(dest_path, filename_normal)

    # 检查文件是否存在
    if os.path.isfile(f"{dest_file}.tar.gz"):
        logging.warning(
            f"{dest_file}.tar.gz already exists. Would you like to continue? (y/n) (default y in 3 seconds)")
        try:
            user_input = get_user_input_with_timeout(3)
        except SystemError as e:
            logging.error("enter thread is not shutdown!")

        if user_input.lower() not in ['y', '']:
            logging.info("Operation cancelled by user.")
            return

    # 压缩文件
    logging.info("Starting tar process!")
    exec_command = f'tar -cvf {dest_file}.tar.gz {source_path}'
    command_exec_generate(exec_command)
    # 检查压缩文件是否成功创建
    if os.path.isfile(f"{dest_file}.tar.gz"):
        logging.info(f"{dest_file}.tar.gz is created, the tar job succeeded!")
    else:
        logging.error(f"Failed to create {dest_file}.tar.gz")

    logging.info("Tar process finished!")
    return ""


def clean_old_file(destpath, factor):
    file_list = os.listdir(destpath)

    if len(file_list) <= int(factor):
        pass
    file_list.sort(reverse=True)
    for index in range(7, len(file_list)):
        print(file_list[index])
        os.remove(os.path.join(dest_path, file_list[index]))


def file_create(destpath):
    file_base = "demo"
    for a in range(10):
        file_normal = f"{file_base}_{a}.txt"
        f = open(os.path.join(dest_path, file_normal), "w")
        f.close()


def get_user_input_with_timeout(timeout):
    def user_input_func():
        nonlocal user_input
        user_input = input()

    user_input = ''
    input_thread = threading.Thread(target=user_input_func)
    input_thread.daemon = True
    input_thread.start()
    input_thread.join(timeout)
    if input_thread.is_alive():
        logging.info("No user input detected, proceeding with default 'y'")
        user_input = 'y'
    return user_input


def command_exec_generate(command):
    try:
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"Command failed with error: {e}")


## TODO 生成文件名

def filename_generate(file):
    if len(file.strip()) == 0:
        logging.warning("file name is empty!")
    time_suffix = (datetime.now() + timedelta(days=-1)).strftime("%Y-%m-%d")
    file_name = f'{file}_{time_suffix}'
    return file_name


if __name__ == '__main__':
    # 参数有 component_name source_path dest_path factor
    ## 示例 python3 es /data/colony/es /data/backup/es/

    # source_path = "/Users/kuiqwang/Desktop/tmp/source"
    dest_path = "/Users/kuiqwang/Desktop/tmp/dest"
    # file_backup("demo", source_path, dest_path)
    # file_create(dest_path)
    clean_old_file(dest_path, 7)
