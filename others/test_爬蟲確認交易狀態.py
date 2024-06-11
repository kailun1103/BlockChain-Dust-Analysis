from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from datetime import datetime
import time
import json
import csv
import subprocess
import sys

# 需求:
# 讓所有檔案都跑過一輪
# 多寫一個判斷式，假設row[1]或row[2]為null，判斷該行為error，刪除該行，直接處理下一行

def restart_program():
    print('restart!!!!!')
    python = sys.executable
    subprocess.Popen([python] + sys.argv)
    sys.exit()

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
csv_file_path = '實驗資料集(確認pending時間)/test/test.csv'


with open(csv_file_path, newline='') as csvfile:
    reader = csv.reader(csvfile)
    next(reader, None)
    
    start_time = time.time()

    for row in reader:
        current_time = time.time()
        if current_time - start_time >= 15 * 60:  # 15分鐘為900秒
            restart_program()

        try:
            if row[10] == '' and row[2] != '':
                print(row[0])
                print('write!!!!!!!!!!!!!')
                hash = row[0]
                driver.get(f'https://www.blockchain.com/explorer/transactions/btc/{hash}')
                time.sleep(3)
                try:
                    # status
                    status = WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[2]/div[2]/main/div/div/section/div/section/div[1]/div[8]/section'))
                    )                                              
                    status = status.text
                    print(f'status:{status}')
                except Exception as ex:
                    status = 'null'
                    transaction_Time = 'null'
                    height = 'null'
                    input_details = '[{"inputHash": "Unknown", "isContract": "false", "amount": "Unknown"}]'
                    output_details = '[{"outputHash": "Unknown", "isContract": "false", "amount": "Unknown"}]'
                    update_csv(csv_file_path, row[0], status, transaction_Time, height, input_details, output_details)
                    continue

                time.sleep(1)
                # 獲得網頁垂直高度
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

                while True:
                    # 取得input/output、date資訊
                    json_txn_detail = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[2]/div[2]/main/div/div/section/section/div[3]/div[2]/div/div/div/pre'))
                    )
                    if json_txn_detail:
                        break
                data = json.loads(json_txn_detail.text)
                # 提取輸入 (inputs) 詳細資料
                input_details = []
                for inp in data['inputs']:
                    input_details.append({
                        "inputHash": inp["address"] if inp["address"] else "Unknown",
                        "isContract": "false",
                        "amount": "{:.8f}".format(inp["value"] / 10**8)
                    })
                input_details = str(input_details).replace("'", '"')

                # 提取輸出 (outputs) 詳細資料
                output_details = []
                for out in data['outputs']:
                    output_details.append({
                        "outputHash": out['address'] if out["address"] else "Unknown",
                        "isContract": "false",  # 沒有提供相關資訊，假設為False
                        "amount": "{:.8f}".format(out["value"] / 10**8)
                    })
                output_details = str(output_details).replace("'", '"')

                # 提取時間戳記並轉換為日期變數
                timestamp = data['time']
                transaction_Time = datetime.fromtimestamp(timestamp)
                height = data['block'].get('height', 'null')
                update_csv(csv_file_path, row[0], status, transaction_Time, height, input_details, output_details)
            elif row[2] == '':
                continue
            else:
                continue
        except IndexError:
            print(row[0])
            print('writing!!!!!!!!!!!!!')
            hash = row[0]
            driver.get(f'https://www.blockchain.com/explorer/transactions/btc/{hash}')
            time.sleep(1)
            try:
                # status
                status = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[2]/div[2]/main/div/div/section/div/section/div[1]/div[8]/section'))
                )
                status = status.text
            except Exception as ex:
                status = 'null'
                transaction_Time = 'null'
                height = 'null'
                input_details = '[{"inputHash": "Unknown", "isContract": "false", "amount": "Unknown"}]'
                output_details = '[{"outputHash": "Unknown", "isContract": "false", "amount": "Unknown"}]'
                update_csv(csv_file_path, row[0], status, transaction_Time, height, input_details, output_details)
                continue

            time.sleep(1)
            # 獲得網頁垂直高度
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

            while True:
                # 取得input/output、date資訊
                json_txn_detail = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[2]/div[2]/main/div/div/section/section/div[3]/div[2]/div/div/div/pre'))
                )
                if json_txn_detail:
                    break

            data = json.loads(json_txn_detail.text)
            # 提取輸入 (inputs) 詳細資料
            input_details = []
            for inp in data['inputs']:
                input_details.append({
                    "inputHash": inp["address"] if inp["address"] else "Unknown",
                    "isContract": "false",
                    "amount": "{:.8f}".format(inp["value"] / 10**8)
                })
            input_details = str(input_details).replace("'", '"')

            # 提取輸出 (outputs) 詳細資料
            output_details = []
            for out in data['outputs']:
                output_details.append({
                    "outputHash": out['address'] if out["address"] else "Unknown",
                    "isContract": "false",  # 沒有提供相關資訊，假設為False
                    "amount": "{:.8f}".format(out["value"] / 10**8)
                })
            output_details = str(output_details).replace("'", '"')

            # 提取時間戳記並轉換為日期變數
            timestamp = data['time']
            transaction_Time = datetime.fromtimestamp(timestamp)
            height = data['block'].get('height', 'null')
            update_csv(csv_file_path, row[0], status, transaction_Time, height, input_details, output_details)
        except Exception as ex:
            print(row[0])
            print('wrote!!!!!!!!!!!!!')
            hash = row[0]
            driver.get(f'https://www.blockchain.com/explorer/transactions/btc/{hash}')
            time.sleep(1)
            try:
                # status
                status = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[2]/div[2]/main/div/div/section/div/section/div[1]/div[8]/section'))
                )
                status = status.text
            except Exception as ex:
                status = 'null'
                transaction_Time = 'null'
                height = 'null'
                input_details = '[{"inputHash": "Unknown", "isContract": "false", "amount": "Unknown"}]'
                output_details = '[{"outputHash": "Unknown", "isContract": "false", "amount": "Unknown"}]'
                update_csv(csv_file_path, row[0], status, transaction_Time, height, input_details, output_details)
                continue

            time.sleep(1)
            # 獲得網頁垂直高度
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

            while True:
                # 取得input/output、date資訊
                json_txn_detail = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[2]/div[2]/main/div/div/section/section/div[3]/div[2]/div/div/div/pre'))
                )
                if json_txn_detail:
                    break

            data = json.loads(json_txn_detail.text)
            # 提取輸入 (inputs) 詳細資料
            input_details = []
            for inp in data['inputs']:
                input_details.append({
                    "inputHash": inp["address"] if inp["address"] else "Unknown",
                    "isContract": "false",
                    "amount": "{:.8f}".format(inp["value"] / 10**8)
                })
            input_details = str(input_details).replace("'", '"')

            # 提取輸出 (outputs) 詳細資料
            output_details = []
            for out in data['outputs']:
                output_details.append({
                    "outputHash": out['address'] if out["address"] else "Unknown",
                    "isContract": "false",  # 沒有提供相關資訊，假設為False
                    "amount": "{:.8f}".format(out["value"] / 10**8)
                })
            output_details = str(output_details).replace("'", '"')

            # 提取時間戳記並轉換為日期變數
            timestamp = data['time']
            transaction_Time = datetime.fromtimestamp(timestamp)
            height = data['block'].get('height', 'null')
            update_csv(csv_file_path, row[0], status, transaction_Time, height, input_details, output_details)


print(time.time()-start_time)
print('DONE!')