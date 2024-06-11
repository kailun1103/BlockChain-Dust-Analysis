import csv
import requests
import json
import time
from datetime import datetime
import os
csv.field_size_limit(2147483647)

def timestamp_to_datetime(timestamp):
    timestamp /= 1000
    dt_object = datetime.fromtimestamp(timestamp)
    formatted_datetime = dt_object.strftime('%Y/%m/%d %I:%M:%S %p')
    return formatted_datetime

# 檔案資料夾位置
# folder_path = 'step1 確認已打包交易資料/2024_01_18'
folder_path = 'test'

# 得到資料夾內所有csv名稱，並儲存為列表
csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]

for csv_file in csv_files:
    csv_path = os.path.join(folder_path, csv_file)
    # 開啟 CSV 檔案並提取 Txn Hash
    txn_hashes = []
    with open(csv_path, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            txn_hashes.append(row['Txn Hash'])

    # 分批處理 Txn Hash，每次處理五個
    batch_size = 5
    for i in range(0, len(txn_hashes), batch_size):
        batch_txn_hashes = txn_hashes[i:i+batch_size]

        # 設定 API 呼叫的參數
        payload = {
        "chainShortName": "btc",
            "txid": ",".join(batch_txn_hashes)
        }
        headers = {
        'Ok-Access-Key': 'b5bae772-4c29-49bd-912b-7005e275837a'
        }

        response = requests.get("https://www.oklink.com/api/v5/explorer/transaction/transaction-fills", headers=headers, params=payload)
        print(csv_file)
        print(response.status_code)
        print(time.ctime(time.time()))
        print('----------------------------')
        while response.status_code != 200:
            time.sleep(5)
            response = requests.get("https://www.oklink.com/api/v5/explorer/transaction/transaction-fills", headers=headers, params=payload)
            if response.status_code == 200:
                break
    
        data = response.json()

        # 將 JSON 資料轉換為字典格式
        transactions = []
        for transaction_data in data["data"]:
            print(transaction_data)
            print('-----------------')
            if transaction_data["state"] == 'success':
                transaction_data["state"] = 'Confirmed'
            transaction_data["transactionTime"] = timestamp_to_datetime(int(transaction_data["transactionTime"]))
            
            transaction_dict = {
                'hash': transaction_data['txid'],
                "state": transaction_data["state"],
                "transactionTime": transaction_data["transactionTime"],
                "height": transaction_data["height"],
                "inputDetails": transaction_data["inputDetails"],
                "outputDetails": transaction_data["outputDetails"]
            }
            transactions.append(transaction_dict)

        # 開啟 CSV 檔案，將交易資料寫入
        with open(csv_path, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)

            # 根據 Txn Hash 對應到 CSV 的行，寫入交易資料
            for row in rows:
                for transaction in transactions:
                    if transaction['hash'] == row['Txn Hash']:
                        row.update({
                            'State': transaction['state'],
                            'Transaction Time': transaction['transactionTime'],
                            'Height': transaction['height'],
                            'Input Details': json.dumps(transaction['inputDetails']),
                            'Output Details': json.dumps(transaction['outputDetails'])
                        })

        # 寫入更新後的資料到 CSV 檔案
        with open(csv_path, 'w', newline='') as csvfile:
            fieldnames = ['Txn Hash', 'Txn Date', 'Input Volume', 'Output Volume', 'Input Count', 'Output Count', 'Fees', 'Total Txn Amount', 'Final Txn Date', 'State', 'Transaction Time', 'Height', 'Input Details', 'Output Details']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for row in rows:
                writer.writerow(row)

print("程式執行結束")