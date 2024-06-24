import csv
import os
import json
import time
from datetime import datetime
csv.field_size_limit(2147483647)

csv_file_path = '2024_01_18-2024_01_24 - 資料集'

data = []

total = 0
for root, dirs, files in os.walk(csv_file_path):
    csv_files = [file for file in files if file.endswith('.csv')]
    for csv_file in csv_files:
        # time.sleep(0.5)
        csv_path = os.path.join(root, csv_file)

        with open(csv_path, mode='r', newline='', encoding='utf-8') as infile:
            print(csv_file)
            reader = csv.reader(infile)
            header = next(reader)
            count = 0
            for row in reader:
                count += 1

 
            total += count


print(total)
