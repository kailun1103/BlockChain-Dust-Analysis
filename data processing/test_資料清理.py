import csv
import os
import shutil  # 导入shutil用于移动文件

csv_file_path = 'step4 資料清洗(invalid or amount為0)'  # 指定原始CSV文件的文件夹路径
temp_output_path = 'temp_cleaned'  # 临时存储清理后文件的目录

# 如果临时输出目录不存在，则创建该目录
if not os.path.exists(temp_output_path):
    os.makedirs(temp_output_path)

# 遍历指定目录及其子目录中的所有文件
for root, dirs, files in os.walk(csv_file_path):
    csv_files = [file for file in files if file.endswith('.csv')]  # 筛选出所有CSV文件
    for csv_file in csv_files:
        csv_path = os.path.join(root, csv_file)  # 原始CSV文件的完整路径
        temp_csv_path = os.path.join(temp_output_path, csv_file)  # 临时文件的完整路径
        print(csv_path)

        # 打开原始文件进行读取，同时打开临时文件准备写入
        with open(csv_path, mode='r', newline='', encoding='utf-8') as infile, \
             open(temp_csv_path, mode='w', newline='', encoding='utf-8') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            header = next(reader)  # 读取表头
            writer.writerow(header)  # 将表头写入新的临时文件

            # 遍历原始文件中的每一行
            for row in reader:
                # if row[9] != "Invalid":
                if row[3] != "0.0":
                # if row[3] != "0":
                    writer.writerow(row)

