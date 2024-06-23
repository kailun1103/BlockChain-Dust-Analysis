import os
import csv
import json

unique_addresses = set()

for root, dirs, files in os.walk("2024_01_18-2024_01_24"):
    csv_files = [file for file in files if file.endswith('.csv')]
    for csv_file in csv_files:
        print(csv_file)
        csv_path = os.path.join(root, csv_file)
        with open(csv_path, mode='r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            header = next(reader)
            for row in reader:
                if int(row[4]) > 75 or int(row[5]) > 75:
                    continue
                else:
                    Txn_Input_Details = json.loads(row[12])
                    Txn_Output_Details = json.loads(row[13])

                    for input_detail in Txn_Input_Details:
                        unique_addresses.add(input_detail['inputHash'])

                    for output_detail in Txn_Output_Details:
                        unique_addresses.add(output_detail['outputHash'])

# 将unique_addresses集合转换为列表并保存为JSON文件
unique_addresses_list = list(unique_addresses)
with open('unique_addresses.json', 'w', encoding='utf-8') as jsonfile:
    json.dump(unique_addresses_list, jsonfile, ensure_ascii=False, indent=4)

print("Unique addresses have been saved to unique_addresses.json")
