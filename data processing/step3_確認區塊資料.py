import requests
import time
import os
import csv

# 初始化 headers 和基础 URL
headers = {'Ok-Access-Key': 'b5bae772-4c29-49bd-912b-7005e275837a'}
base_url = "https://www.oklink.com/api/v5/explorer/block/block-fills?chainShortName=btc&height="

# 用于存储区块数据的缓存字典
block_data_cache = {}

def get_block_data(height):
    if height not in block_data_cache:
        response = requests.get(f"{base_url}{height}", headers=headers)
        block_data = response.json()
        params = block_data["data"][0]
        param_names = ['chainFullName', 'chainShortName', 'hash', 'height', 'validator', 'blockTime', 'txnCount', 'amount', 'blockSize', 'mineReward', 'totalFee', 'feeSymbol', 'ommerBlock', 'merkleRootHash', 'gasUsed', 'gasLimit', 'gasAvgPrice', 'state', 'burnt', 'netWork', 'txnInternal', 'miner', 'difficuity', 'nonce', 'tips', 'confirm', 'baseFeePerGas']
        block_data_cache[height] = {name: (params[name] if params[name] != '' else "ull") for name in param_names}
    return block_data_cache[height]

csv_file_path = 'test2'

total_files_count = 0
current_file_number = 0

for root, dirs, files in os.walk(csv_file_path):
    csv_files = [file for file in files if file.endswith('.csv')]
    total_files_count = len(csv_files)
    for csv_file in csv_files:
        
        block_data_cache = {}

        current_file_number += 1
        time.sleep(0.5)
        csv_path = os.path.join(root, csv_file)
        temp_csv_path = os.path.join(root, 'temp_' + csv_file)

        with open(csv_path, mode='r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            header = next(reader)
            param_names = ['chainFullName', 'chainShortName', 'hash', 'height', 'validator', 'blockTime', 'txnCount', 'amount', 'blockSize', 'mineReward', 'totalFee', 'feeSymbol', 'ommerBlock', 'merkleRootHash', 'gasUsed', 'gasLimit', 'gasAvgPrice', 'state', 'burnt', 'netWork', 'txnInternal', 'miner', 'difficuity', 'nonce', 'tips', 'confirm', 'baseFeePerGas']
            header.extend(param_names)

            with open(temp_csv_path, mode='w', newline='', encoding='utf-8') as outfile:
                writer = csv.writer(outfile)
                writer.writerow(header)

                for row in reader:
                    print(f"Processing file {current_file_number} of {total_files_count}: {csv_file}")
                    if row[9] == "Confirmed":
                        block_height = int(row[11])
                        print(block_height)
                        block_data_dict = get_block_data(block_height)
                        row.extend(block_data_dict.values())
                        writer.writerow(row)
                    else:
                        print("Skipping")
                        row.extend(['null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null'])
                        writer.writerow(row)
                        continue

        os.replace(temp_csv_path, csv_path)
        print(f"Completed updating CSV file: {csv_file}")