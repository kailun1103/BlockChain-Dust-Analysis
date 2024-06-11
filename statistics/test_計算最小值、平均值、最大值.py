import csv
import os
from datetime import datetime
csv.field_size_limit(2147483647)

csv_file_path = 'step4 資料清洗(invalid or amount為0)'
# csv_file_path = 'step4 資料清洗(invalid or amount為0)'
total = 0
data = []
for root, dirs, files in os.walk(csv_file_path):
    csv_files = [file for file in files if file.endswith('.csv')]
    for csv_file in csv_files:
        # time.sleep(0.5)
        csv_path = os.path.join(root, csv_file)
        print(csv_path)

        with open(csv_path, mode='r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            header = next(reader)  # 讀取並丟棄標題行
            
            for row in reader:
                if row[6] == 'Confirmed':
                    # if float(row[3]) != 0:
                    data.append(int(row[5]))
            

print(len(data))
min_value = min(data)
max_value = max(data)
average_value = sum(data) / len(data)
print(f"最小值: {min_value}, 平均值: {average_value}, 最大值: {max_value}")