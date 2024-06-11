from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import requests
from datetime import datetime
import json
import time
import json
import csv
import subprocess
import sys
import os
csv.field_size_limit(2147483647)

# 針對交易狀態為invalid以及null的交易狀態來處理
# 假設有記錄到pending或是comfirmed要額外處理(oklink api? crawler?)

def timestamp_to_datetime(timestamp):
    timestamp /= 1000
    dt_object = datetime.fromtimestamp(timestamp)
    formatted_datetime = dt_object.strftime('%Y/%m/%d %I:%M:%S %p')
    return formatted_datetime

# 重啟程式，避免程式本身crash
def restart_program():
    print('RESTART')
    python = sys.executable
    subprocess.Popen([python] + sys.argv)
    sys.exit()

# 寫進csv
def update_csv(csv_file_path, txid, status, transaction_time, height, input_details, output_details):
    print('start write to csv')
    with open(csv_file_path, 'r', newline='') as file:
        reader = csv.DictReader(file)
        rows = list(reader)
    for row in rows:
        if row['Txn Hash'] == txid:
            row['State'] = status
            row['Transaction Time'] = transaction_time
            row['Height'] = height
            row['Input Details'] = input_details
            row['Output Details'] = output_details
    with open(csv_file_path, 'w', newline='') as file:
        fieldnames = ['Txn Hash', 'Txn Date', 'Input Volume', 'Output Volume', 'Input Count', 'Output Count',
                      'Fees', 'Total Txn Amount', 'Final Txn Date', 'State', 'Transaction Time', 'Height',
                      'Input Details', 'Output Details']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        print('done write to csv')


driver = webdriver.Chrome()
driver.maximize_window()
driver.get(f'https://www.blockchain.com/explorer/transactions/btc')
print('start crawler')

# csv_file_path = '實驗資料集(確認pending時間)/0320_2322/待確認pending資料.csv'
csv_file_path = 'test'
# 得到資料夾內所有csv名稱，並儲存為列表
csv_files = [file for file in os.listdir(csv_file_path) if file.endswith('.csv')]
for csv_file in csv_files:
    csv_path = os.path.join(csv_file_path, csv_file)
    with open(csv_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)
        start_time = time.time()
        print('START')

        for row in reader:
            # 重啟程式條件
            current_time = time.time()
            if current_time - start_time >= 900: # 15分鐘
                restart_program()
            try:
                if row[10] == '':
                    hash = row[0]
                    print('--------------------')
                    print(csv_path)
                    print(time.ctime(time.time()))
                    print(f'hash: {hash}')
                    driver.get(f'https://www.blockchain.com/explorer/transactions/btc/{hash}')
                    time.sleep(3)
                    # 假設抓不到交易的status，交易連查都查不到情況下
                    try:
                        status = WebDriverWait(driver, 20).until(
                            EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[2]/div[2]/main/div/div/section/div/section/div[1]/div[8]/section'))
                        )                                              
                        status = status.text
                    except Exception as ex:
                        status = 'null'
                        transaction_Time = 'null'
                        height = 'null'
                        input_details = '[{"inputHash": "Unknown", "isContract": "false", "amount": "Unknown"}]'
                        output_details = '[{"outputHash": "Unknown", "isContract": "false", "amount": "Unknown"}]'
                        update_csv(csv_path, row[0], status, transaction_Time, height, input_details, output_details)
                        continue

                    print(f'status: {status}')
                    time.sleep(1)
                    # 獲得網頁垂直高度，假設抓不到json_button會調整頁面高度，假設抓到json按鈕元素才會往下跳
                    vertical_height = driver.execute_script("return Math.max( document.body.scrollHeight, document.body.offsetHeight, document.documentElement.clientHeight, document.documentElement.scrollHeight, document.documentElement.offsetHeight );")
                    page_height = 0
                    while True:
                        page_height += 150
                        json_button = WebDriverWait(driver, 1).until(
                            EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[2]/div[2]/main/div/div/section/section/div[3]/div[1]/button[2]'))
                        )   
                        if json_button:
                            json_button.click()
                            driver.execute_script(f"window.scrollTo(0, {page_height+100});")
                            break
                        else:
                            driver.execute_script(f"window.scrollTo(0, {page_height});")
                    time.sleep(1)

                    json_txn_detail = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[2]/div[2]/main/div/div/section/section/div[3]/div[2]/div/div/div/pre'))
                    )
                    data = json.loads(json_txn_detail.text)
                    input_details = []
                    output_details = []
                    for inp in data['inputs']:
                        input_details.append({
                            "inputHash": inp["address"] if inp["address"] else "Unknown",
                            "isContract": "false",
                            "amount": "{:.8f}".format(inp["value"] / 10**8)
                        })
                    input_details = str(input_details).replace("'", '"')
                    for out in data['outputs']:
                        output_details.append({
                            "outputHash": out['address'] if out["address"] else "Unknown",
                            "isContract": "false",  # 沒有提供相關資訊，假設為False
                            "amount": "{:.8f}".format(out["value"] / 10**8)
                        })
                    output_details = str(output_details).replace("'", '"')

                    # 假設Confirmed，則打oklink api確認時間
                    if status == 'Confirmed':
                        payload = {
                        "chainShortName": "btc",
                            "txid": hash
                        }
                        headers = {
                        'Ok-Access-Key': 'b5bae772-4c29-49bd-912b-7005e275837a'
                        }
                        response = requests.get("https://www.oklink.com/api/v5/explorer/transaction/transaction-fills", headers=headers, params=payload)
                        transaction_dict = json.loads(response.text)
                        timestamp = timestamp_to_datetime(int(transaction_dict['data'][0]['transactionTime']))
                    else:
                        timestamp = 'null'

                    height = data['block'].get('height', 'null')
                    update_csv(csv_path, row[0], status, timestamp, height, input_details, output_details)
                elif row[2] == '':
                    continue
                else:
                    continue
            
            except Exception as ex:
                print(ex)
                continue

print('DONE!')