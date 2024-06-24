from multiprocessing import Process
import os
from concurrent.futures import ThreadPoolExecutor
import threading
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
from datetime import datetime
from collections import deque
import time 
import csv

BTC_final_date = ''

# 計算交易總筆數、查閱最終日期
def get_final_date(btc_driver_last):
    try:
        global BTC_final_date

        try:
            page_8 = WebDriverWait(btc_driver_last, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/li[8]'))
            )
            page_8_text = page_8.text
        except TimeoutException:
            page_8_text = 'null'
        try:
            page_9 = WebDriverWait(btc_driver_last, 2).until( # 等2秒，沒有找到就pass
                EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/li[9]'))
            )
            page_9_text = page_9.text
        except TimeoutException:
            page_9_text = 'null'


        # 判斷最後頁數的按鈕，並點擊以及紀錄頁數
        if page_9_text == '>':
            if page_8_text != '>':
                btc_driver_last.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                last_page_button = WebDriverWait(btc_driver_last, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/li[8]'))
                )
                last_page_button.click()
            else:
                print('can not find last button for web page')
        else:
            if page_8_text == '>':
                btc_driver_last.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                last_page_button = WebDriverWait(btc_driver_last, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/li[7]'))
                )
                last_page_button.click()
            else:
                print('can not find last button for web page')

        time.sleep(0.5)
        BTC_page_3 = BeautifulSoup(btc_driver_last.page_source, 'html.parser')
        BTC_pending_date = BTC_page_3.select('tr')[-1].select_one('td:nth-of-type(2) div')
        BTC_final_date = BTC_pending_date.text if BTC_pending_date else None # 總交易最後一筆的日期紀錄起來
    except Exception as ex:
        print(f'get_final_date fail, reason:{ex}')
        return 0,0
    



    
def btc_crawler(btc_driver_first, btc_driver_second, btc_driver_third, btc_driver_four, btc_driver_five, btc_driver_six, btc_driver_seven, btc_driver_eight, btc_driver_last, hashes_seen, header_written):
    try:
        BTC_soup_1 = BeautifulSoup(btc_driver_first.page_source, 'html.parser')
        BTC_soup_2 = BeautifulSoup(btc_driver_second.page_source, 'html.parser')
        BTC_soup_3 = BeautifulSoup(btc_driver_third.page_source, 'html.parser')
        BTC_soup_4 = BeautifulSoup(btc_driver_four.page_source, 'html.parser')
        BTC_soup_5 = BeautifulSoup(btc_driver_five.page_source, 'html.parser')
        BTC_soup_6 = BeautifulSoup(btc_driver_six.page_source, 'html.parser')
        BTC_soup_7 = BeautifulSoup(btc_driver_seven.page_source, 'html.parser')
        BTC_soup_8 = BeautifulSoup(btc_driver_eight.page_source, 'html.parser')

        BTC_rows_1 = BTC_soup_1.select('tr')
        BTC_data_to_write_1 = []
        BTC_rows_2 = BTC_soup_2.select('tr')
        BTC_data_to_write_2 = []
        BTC_rows_3 = BTC_soup_3.select('tr')
        BTC_data_to_write_3 = []
        BTC_rows_4 = BTC_soup_4.select('tr')
        BTC_data_to_write_4 = []
        BTC_rows_5 = BTC_soup_5.select('tr')
        BTC_data_to_write_5 = []
        BTC_rows_6 = BTC_soup_6.select('tr')
        BTC_data_to_write_6 = []
        BTC_rows_7 = BTC_soup_7.select('tr')
        BTC_data_to_write_7 = []
        BTC_rows_8 = BTC_soup_8.select('tr')
        BTC_data_to_write_8 = []

        btc_pending_txn_count = int(BTC_soup_1.find(class_='font-size-sm responsive-label nowrap secondary-text').text.replace("The total Number of ","").replace(" Txns","").replace(",",""))

        global BTC_final_date
        get_final_date(btc_driver_last)
        system_time = time.strftime('%Y/%m/%d %I:%M:%S %p', time.localtime(time.time()))

        # 本次迴圈計算的交易量
        this_time_rows = 0

        # driver_1迭代處理網頁每一行
        for row in BTC_rows_1:
            txn_link_1 = row.select_one('td:nth-of-type(1) a') # 從表格行中選擇特定的資料欄位
            if txn_link_1:
                txn_hash_1 = txn_link_1.get('href').split('/')[-1]
                if txn_hash_1 not in hashes_seen: # 利用deque排除重複的交易
                    hashes_seen.append(txn_hash_1)
                    txn_time_1 = row.select_one('td:nth-of-type(2) div').text
                    inputsVolume_1 = row.select_one('td:nth-of-type(3)')
                    outputsVolume_1 = row.select_one('td:nth-of-type(4)')
                    fees_1 = row.select_one('td:nth-of-type(6)')
                    inputsVolume_amt_1 = 0
                    inputsCount_amt_1 = 0
                    if inputsVolume_1:
                        inputsVolume_amt_1 = float(inputsVolume_1.text.split()[0])
                        inputsCount_amt_1 = int(inputsVolume_1.text.split()[1].replace('(',''))
                    outputsVolume_amt_1 = 0
                    outputsCount_amt_1 = 0
                    if outputsVolume_1:
                        outputsVolume_amt_1 = float(outputsVolume_1.text.split()[0])
                        outputsCount_amt_1 = int(outputsVolume_1.text.split()[1].replace('(',''))
                    fees_amt_1 = 0
                    if fees_1:
                        fees_amt_1 = float(fees_1.text.split()[0])
                    BTC_data_to_write_1.append([system_time, txn_hash_1, txn_time_1, inputsVolume_amt_1, outputsVolume_amt_1, inputsCount_amt_1, outputsCount_amt_1, fees_amt_1, btc_pending_txn_count, BTC_final_date])

        # driver_2迭代處理網頁每一行
        for row in BTC_rows_2:
            txn_link_2 = row.select_one('td:nth-of-type(1) a') 
            if txn_link_2:
                txn_hash_2 = txn_link_2.get('href').split('/')[-1]
                if txn_hash_2 not in hashes_seen: 
                    hashes_seen.append(txn_hash_2)
                    txn_time_2 = row.select_one('td:nth-of-type(2) div').text
                    inputsVolume_2 = row.select_one('td:nth-of-type(3)')
                    outputsVolume_2 = row.select_one('td:nth-of-type(4)')
                    fees_2 = row.select_one('td:nth-of-type(6)')
                    inputsVolume_amt_2 = 0
                    inputsCount_amt_2 = 0
                    if inputsVolume_2:
                        inputsVolume_amt_2 = float(inputsVolume_2.text.split()[0])
                        inputsCount_amt_2 = int(inputsVolume_2.text.split()[1].replace('(',''))
                    outputsVolume_amt_2 = 0
                    outputsCount_amt_2 = 0
                    if outputsVolume_2:
                        outputsVolume_amt_2 = float(outputsVolume_2.text.split()[0])
                        outputsCount_amt_2 = int(outputsVolume_2.text.split()[1].replace('(',''))
                    fees_amt_2 = 0
                    if fees_2:
                        fees_amt_2 = float(fees_2.text.split()[0])
                    BTC_data_to_write_2.append([system_time, txn_hash_2, txn_time_2, inputsVolume_amt_2, outputsVolume_amt_2, inputsCount_amt_2, outputsCount_amt_2, fees_amt_2, btc_pending_txn_count, BTC_final_date])


        # driver_3迭代處理網頁每一行
        for row in BTC_rows_3:
            txn_link_3 = row.select_one('td:nth-of-type(1) a') # 從表格行中選擇特定的資料欄位
            if txn_link_3:
                txn_hash_3 = txn_link_3.get('href').split('/')[-1]
                if txn_hash_3 not in hashes_seen: # 利用deque排除重複的交易
                    hashes_seen.append(txn_hash_3)
                    txn_time_3 = row.select_one('td:nth-of-type(2) div').text
                    inputsVolume_3 = row.select_one('td:nth-of-type(3)')
                    outputsVolume_3 = row.select_one('td:nth-of-type(4)')
                    fees_3 = row.select_one('td:nth-of-type(6)')
                    inputsVolume_amt_3 = 0
                    inputsCount_amt_3 = 0
                    if inputsVolume_3:
                        inputsVolume_amt_3 = float(inputsVolume_3.text.split()[0])
                        inputsCount_amt_3 = int(inputsVolume_3.text.split()[1].replace('(',''))
                    outputsVolume_amt_3 = 0
                    outputsCount_amt_3 = 0
                    if outputsVolume_3:
                        outputsVolume_amt_3 = float(outputsVolume_3.text.split()[0])
                        outputsCount_amt_3 = int(outputsVolume_3.text.split()[1].replace('(',''))
                    fees_amt_3 = 0
                    if fees_3:
                        fees_amt_3 = float(fees_3.text.split()[0])

                    BTC_data_to_write_3.append([system_time, txn_hash_3, txn_time_3, inputsVolume_amt_3, outputsVolume_amt_3, inputsCount_amt_3, outputsCount_amt_3, fees_amt_3, btc_pending_txn_count, BTC_final_date])

        # driver_4迭代處理網頁每一行
        for row in BTC_rows_4:
            txn_link_4 = row.select_one('td:nth-of-type(1) a') 
            if txn_link_4:
                txn_hash_4 = txn_link_4.get('href').split('/')[-1]
                if txn_hash_4 not in hashes_seen: 
                    hashes_seen.append(txn_hash_4)
                    txn_time_4 = row.select_one('td:nth-of-type(2) div').text
                    inputsVolume_4 = row.select_one('td:nth-of-type(3)')
                    outputsVolume_4 = row.select_one('td:nth-of-type(4)')
                    fees_4 = row.select_one('td:nth-of-type(6)')
                    inputsVolume_amt_4 = 0
                    inputsCount_amt_4 = 0
                    if inputsVolume_4:
                        inputsVolume_amt_4 = float(inputsVolume_4.text.split()[0])
                        inputsCount_amt_4 = int(inputsVolume_4.text.split()[1].replace('(',''))
                    outputsVolume_amt_4 = 0
                    outputsCount_amt_4 = 0
                    if outputsVolume_4:
                        outputsVolume_amt_4 = float(outputsVolume_4.text.split()[0])
                        outputsCount_amt_4 = int(outputsVolume_4.text.split()[1].replace('(',''))
                    fees_amt_4 = 0
                    if fees_4:
                        fees_amt_4 = float(fees_4.text.split()[0])
                    
                    BTC_data_to_write_4.append([system_time, txn_hash_4, txn_time_4, inputsVolume_amt_4, outputsVolume_amt_4, inputsCount_amt_4, outputsCount_amt_4, fees_amt_4, btc_pending_txn_count, BTC_final_date])


        # driver_5迭代處理網頁每一行
        for row in BTC_rows_5:
            txn_link_5 = row.select_one('td:nth-of-type(1) a') # 從表格行中選擇特定的資料欄位
            if txn_link_5:
                txn_hash_5 = txn_link_5.get('href').split('/')[-1]
                if txn_hash_5 not in hashes_seen: # 利用deque排除重複的交易
                    hashes_seen.append(txn_hash_5)
                    txn_time_5 = row.select_one('td:nth-of-type(2) div').text
                    inputsVolume_5 = row.select_one('td:nth-of-type(3)')
                    outputsVolume_5 = row.select_one('td:nth-of-type(4)')
                    fees_5 = row.select_one('td:nth-of-type(6)')
                    inputsVolume_amt_5 = 0
                    inputsCount_amt_5 = 0
                    if inputsVolume_5:
                        inputsVolume_amt_5 = float(inputsVolume_5.text.split()[0])
                        inputsCount_amt_5 = int(inputsVolume_5.text.split()[1].replace('(',''))
                    outputsVolume_amt_5 = 0
                    outputsCount_amt_5 = 0
                    if outputsVolume_5:
                        outputsVolume_amt_5 = float(outputsVolume_5.text.split()[0])
                        outputsCount_amt_5 = int(outputsVolume_5.text.split()[1].replace('(',''))
                    fees_amt_5 = 0
                    if fees_5:
                        fees_amt_5 = float(fees_5.text.split()[0])

                    BTC_data_to_write_5.append([system_time, txn_hash_5, txn_time_5, inputsVolume_amt_5, outputsVolume_amt_5, inputsCount_amt_5, outputsCount_amt_5, fees_amt_5, btc_pending_txn_count, BTC_final_date])

        # driver_6迭代處理網頁每一行
        for row in BTC_rows_6:
            txn_link_6 = row.select_one('td:nth-of-type(1) a') 
            if txn_link_6:
                txn_hash_6 = txn_link_6.get('href').split('/')[-1]
                if txn_hash_6 not in hashes_seen: 
                    hashes_seen.append(txn_hash_6)
                    txn_time_6 = row.select_one('td:nth-of-type(2) div').text
                    inputsVolume_6 = row.select_one('td:nth-of-type(3)')
                    outputsVolume_6 = row.select_one('td:nth-of-type(4)')
                    fees_6 = row.select_one('td:nth-of-type(6)')
                    inputsVolume_amt_6 = 0
                    inputsCount_amt_6 = 0
                    if inputsVolume_6:
                        inputsVolume_amt_6 = float(inputsVolume_6.text.split()[0])
                        inputsCount_amt_6 = int(inputsVolume_6.text.split()[1].replace('(',''))
                    outputsVolume_amt_6 = 0
                    outputsCount_amt_6 = 0
                    if outputsVolume_6:
                        outputsVolume_amt_6 = float(outputsVolume_6.text.split()[0])
                        outputsCount_amt_6 = int(outputsVolume_6.text.split()[1].replace('(',''))
                    fees_amt_6 = 0
                    if fees_6:
                        fees_amt_6 = float(fees_6.text.split()[0])
                    
                    BTC_data_to_write_6.append([system_time, txn_hash_6, txn_time_6, inputsVolume_amt_6, outputsVolume_amt_6, inputsCount_amt_6, outputsCount_amt_6, fees_amt_6, btc_pending_txn_count, BTC_final_date])


        # driver_7迭代處理網頁每一行
        for row in BTC_rows_7:
            txn_link_7 = row.select_one('td:nth-of-type(1) a') # 從表格行中選擇特定的資料欄位
            if txn_link_7:
                txn_hash_7 = txn_link_7.get('href').split('/')[-1]
                if txn_hash_7 not in hashes_seen: # 利用deque排除重複的交易
                    hashes_seen.append(txn_hash_7)
                    txn_time_7 = row.select_one('td:nth-of-type(2) div').text
                    inputsVolume_7 = row.select_one('td:nth-of-type(3)')
                    outputsVolume_7 = row.select_one('td:nth-of-type(4)')
                    fees_7 = row.select_one('td:nth-of-type(6)')
                    inputsVolume_amt_7 = 0
                    inputsCount_amt_7 = 0
                    if inputsVolume_7:
                        inputsVolume_amt_7 = float(inputsVolume_7.text.split()[0])
                        inputsCount_amt_7 = int(inputsVolume_7.text.split()[1].replace('(',''))
                    outputsVolume_amt_7 = 0
                    outputsCount_amt_7 = 0
                    if outputsVolume_7:
                        outputsVolume_amt_7 = float(outputsVolume_7.text.split()[0])
                        outputsCount_amt_7 = int(outputsVolume_7.text.split()[1].replace('(',''))
                    fees_amt_7 = 0
                    if fees_7:
                        fees_amt_7 = float(fees_7.text.split()[0])

                    BTC_data_to_write_7.append([system_time, txn_hash_7, txn_time_7, inputsVolume_amt_7, outputsVolume_amt_7, inputsCount_amt_7, outputsCount_amt_7, fees_amt_7, btc_pending_txn_count, BTC_final_date])

        # driver_8迭代處理網頁每一行
        for row in BTC_rows_8:
            txn_link_8 = row.select_one('td:nth-of-type(1) a') 
            if txn_link_8:
                txn_hash_8 = txn_link_8.get('href').split('/')[-1]
                if txn_hash_8 not in hashes_seen: 
                    hashes_seen.append(txn_hash_8)
                    txn_time_8 = row.select_one('td:nth-of-type(2) div').text
                    inputsVolume_8 = row.select_one('td:nth-of-type(3)')
                    outputsVolume_8 = row.select_one('td:nth-of-type(4)')
                    fees_8 = row.select_one('td:nth-of-type(6)')
                    inputsVolume_amt_8 = 0
                    inputsCount_amt_8 = 0
                    if inputsVolume_8:
                        inputsVolume_amt_8 = float(inputsVolume_8.text.split()[0])
                        inputsCount_amt_8 = int(inputsVolume_8.text.split()[1].replace('(',''))
                    outputsVolume_amt_8 = 0
                    outputsCount_amt_8 = 0
                    if outputsVolume_8:
                        outputsVolume_amt_8 = float(outputsVolume_8.text.split()[0])
                        outputsCount_amt_8 = int(outputsVolume_8.text.split()[1].replace('(',''))
                    fees_amt_8 = 0
                    if fees_8:
                        fees_amt_8 = float(fees_8.text.split()[0])

                    BTC_data_to_write_8.append([system_time, txn_hash_8, txn_time_8, inputsVolume_amt_8, outputsVolume_amt_8, inputsCount_amt_8, outputsCount_amt_8, fees_amt_8, btc_pending_txn_count, BTC_final_date])

        if BTC_data_to_write_1:
            with open(csv_file_name, 'a', newline='', encoding='utf-8') as csvfile:
                csv_writer = csv.writer(csvfile)
                if not header_written:
                    csv_writer.writerow(["System Time", "Txn Hash","Txn Date", "Input Volume", "Output Volume", "Input Count", "Output Count", "Fees", "Total Txn Amount", "Final Txn Date"])
                    header_written = True
                csv_writer.writerows(BTC_data_to_write_1)
            print(f"- 本次driver_1抓取了 {len(BTC_data_to_write_1)} 條交易")

        if BTC_data_to_write_2:
            with open(csv_file_name, 'a', newline='', encoding='utf-8') as csvfile:
                csv_writer = csv.writer(csvfile)
                if not header_written:
                    csv_writer.writerow(["System Time", "Txn Hash","Txn Date", "Input Volume", "Output Volume", "Input Count", "Output Count", "Fees", "Total Txn Amount", "Final Txn Date"])
                    header_written = True
                csv_writer.writerows(BTC_data_to_write_2)
            print(f"- 本次driver_2抓取了 {len(BTC_data_to_write_2)} 條交易")

        if BTC_data_to_write_3:
            with open(csv_file_name, 'a', newline='', encoding='utf-8') as csvfile:
                csv_writer = csv.writer(csvfile)
                if not header_written:
                    csv_writer.writerow(["System Time", "Txn Hash","Txn Date", "Input Volume", "Output Volume", "Input Count", "Output Count", "Fees", "Total Txn Amount", "Final Txn Date"])
                    header_written = True
                csv_writer.writerows(BTC_data_to_write_3)
            print(f"- 本次driver_3抓取了 {len(BTC_data_to_write_3)} 條交易")

        if BTC_data_to_write_4:
            with open(csv_file_name, 'a', newline='', encoding='utf-8') as csvfile:
                csv_writer = csv.writer(csvfile)
                if not header_written:
                    csv_writer.writerow(["System Time", "Txn Hash","Txn Date", "Input Volume", "Output Volume", "Input Count", "Output Count", "Fees", "Total Txn Amount", "Final Txn Date"])
                    header_written = True
                csv_writer.writerows(BTC_data_to_write_4)
            print(f"- 本次driver_4抓取了 {len(BTC_data_to_write_4)} 條交易")


        if BTC_data_to_write_5:
            with open(csv_file_name, 'a', newline='', encoding='utf-8') as csvfile:
                csv_writer = csv.writer(csvfile)
                if not header_written:
                    csv_writer.writerow(["System Time", "Txn Hash","Txn Date", "Input Volume", "Output Volume", "Input Count", "Output Count", "Fees", "Total Txn Amount", "Final Txn Date"])
                    header_written = True
                csv_writer.writerows(BTC_data_to_write_5)
            print(f"- 本次driver_5抓取了 {len(BTC_data_to_write_5)} 條交易")

        if BTC_data_to_write_6:
            with open(csv_file_name, 'a', newline='', encoding='utf-8') as csvfile:
                csv_writer = csv.writer(csvfile)
                if not header_written:
                    csv_writer.writerow(["System Time", "Txn Hash","Txn Date", "Input Volume", "Output Volume", "Input Count", "Output Count", "Fees", "Total Txn Amount", "Final Txn Date"])
                    header_written = True
                csv_writer.writerows(BTC_data_to_write_6)
            print(f"- 本次driver_6抓取了 {len(BTC_data_to_write_6)} 條交易")


        if BTC_data_to_write_7:
            with open(csv_file_name, 'a', newline='', encoding='utf-8') as csvfile:
                csv_writer = csv.writer(csvfile)
                if not header_written:
                    csv_writer.writerow(["System Time", "Txn Hash","Txn Date", "Input Volume", "Output Volume", "Input Count", "Output Count", "Fees", "Total Txn Amount", "Final Txn Date"])
                    header_written = True
                csv_writer.writerows(BTC_data_to_write_7)
            print(f"- 本次driver_7抓取了 {len(BTC_data_to_write_7)} 條交易")

        if BTC_data_to_write_8:
            with open(csv_file_name, 'a', newline='', encoding='utf-8') as csvfile:
                csv_writer = csv.writer(csvfile)
                if not header_written:
                    csv_writer.writerow(["System Time", "Txn Hash","Txn Date", "Input Volume", "Output Volume", "Input Count", "Output Count", "Fees", "Total Txn Amount", "Final Txn Date"])
                    header_written = True
                csv_writer.writerows(BTC_data_to_write_8)
            print(f"- 本次driver_8抓取了 {len(BTC_data_to_write_8)} 條交易")

        this_time_rows = len(BTC_data_to_write_1) + len(BTC_data_to_write_2) + len(BTC_data_to_write_3) + len(BTC_data_to_write_4) + len(BTC_data_to_write_5) + len(BTC_data_to_write_6) + len(BTC_data_to_write_7) + len(BTC_data_to_write_8) # 追蹤總行數

        if this_time_rows != 0:
            print(f"BTC八個視窗抓取了 {this_time_rows} 條交易")
            print(f"BTC總交易數量為 {btc_pending_txn_count} 條交易")
            print(f'BTC目前時間為: {system_time}')
            print("----------------------------------------")
    
    except Exception as ex:
        print(f"Failed reason: {ex}")






if __name__ == '__main__':

    #---- selenium參數設定 ----*
    btc_driver_first = webdriver.Chrome() # btc_driver_first 抓取 btc.com第一頁交易資訊
    btc_driver_first.get('https://explorer.btc.com/btc/unconfirmed-txs')
    btc_driver_first.execute_script("window.scrollTo(0, document.body.scrollHeight);") # 滑到最底下
    # try: # 點擊一次顯示100頁選項
    #     page_opthon = WebDriverWait(btc_driver_first, 20).until(
    #         EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/div'))
    #     )
    #     page_opthon.click()
    #     show_100 = WebDriverWait(btc_driver_first, 20).until(
    #         EC.presence_of_element_located((By.XPATH, '//*[@id="btccom-ui-dropdown"]/div/div/div[4]'))
    #     )
    #     show_100.click() 
    # except Exception as ex:
    #     print(f"btc_driver_first hit error: {ex}")
    time.sleep(2)


    btc_driver_second = webdriver.Chrome() # btc_driver_second 抓取 btc.com第二頁交易資訊
    btc_driver_second.get('https://explorer.btc.com/btc/unconfirmed-txs')
    btc_driver_second.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    try:
        # 點擊一次顯示100頁
        # page_opthon = WebDriverWait(btc_driver_second, 20).until(
        #     EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/div'))
        # )
        # page_opthon.click()
        # show_100 = WebDriverWait(btc_driver_second, 20).until(
        #     EC.presence_of_element_located((By.XPATH, '//*[@id="btccom-ui-dropdown"]/div/div/div[4]'))
        # )
        # show_100.click() 
        # btc_driver_second.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        page_2 = WebDriverWait(btc_driver_second, 20).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/li[3]/button'))
        )
        page_2.click() 
    except Exception as ex:
        print(f"btc_driver_second hit error: {ex}")
    time.sleep(2)


    btc_driver_third = webdriver.Chrome() # btc_driver_second 抓取 btc.com第三頁交易資訊
    btc_driver_third.get('https://explorer.btc.com/btc/unconfirmed-txs')
    btc_driver_third.execute_script("window.scrollTo(0, document.body.scrollHeight);") 
    page_3 = WebDriverWait(btc_driver_third, 20).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/li[4]/button'))
    )
    page_3.click() 
    time.sleep(2)

    btc_driver_four = webdriver.Chrome() # btc_driver_second 抓取 btc.com第四頁交易資訊
    btc_driver_four.get('https://explorer.btc.com/btc/unconfirmed-txs')
    btc_driver_four.execute_script("window.scrollTo(0, document.body.scrollHeight);") 
    page_4 = WebDriverWait(btc_driver_four, 20).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/li[5]/button'))
    )
    page_4.click() 
    time.sleep(2)

    btc_driver_five = webdriver.Chrome() # btc_driver_second 抓取 btc.com第五頁交易資訊
    btc_driver_five.get('https://explorer.btc.com/btc/unconfirmed-txs')
    btc_driver_five.execute_script("window.scrollTo(0, document.body.scrollHeight);") 
    page_5 = WebDriverWait(btc_driver_five, 20).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/li[5]/button'))
    )
    page_5.click()
    btc_driver_five.execute_script("window.scrollTo(0, document.body.scrollHeight);") 
    time.sleep(2)
    page_5 = WebDriverWait(btc_driver_five, 20).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/li[6]/button'))
    )
    page_5.click() 
    time.sleep(2)

    btc_driver_six = webdriver.Chrome() # btc_driver_second 抓取 btc.com第六頁交易資訊
    btc_driver_six.get('https://explorer.btc.com/btc/unconfirmed-txs')
    btc_driver_six.execute_script("window.scrollTo(0, document.body.scrollHeight);") 
    page_6 = WebDriverWait(btc_driver_six, 20).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/li[5]/button'))
    )
    page_6.click()
    time.sleep(2)
    btc_driver_six.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    page_6 = WebDriverWait(btc_driver_six, 20).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/li[6]/button'))
    )
    page_6.click()
    btc_driver_six.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    page_6 = WebDriverWait(btc_driver_six, 20).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/li[6]/button'))
    )
    page_6.click() 
    time.sleep(2)

    btc_driver_seven = webdriver.Chrome() # btc_driver_second 抓取 btc.com第七頁交易資訊
    btc_driver_seven.get('https://explorer.btc.com/btc/unconfirmed-txs')
    btc_driver_seven.execute_script("window.scrollTo(0, document.body.scrollHeight);") 
    page_7 = WebDriverWait(btc_driver_seven, 20).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/li[5]/button'))
    )
    page_7.click()
    time.sleep(2)
    btc_driver_seven.execute_script("window.scrollTo(0, document.body.scrollHeight);") 
    page_7 = WebDriverWait(btc_driver_seven, 20).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/li[6]/button'))
    )
    page_7.click()
    time.sleep(2)
    btc_driver_seven.execute_script("window.scrollTo(0, document.body.scrollHeight);") 
    page_7 = WebDriverWait(btc_driver_seven, 20).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/li[6]/button'))
    )
    page_7.click() 
    time.sleep(2)
    btc_driver_seven.execute_script("window.scrollTo(0, document.body.scrollHeight);") 
    page_7 = WebDriverWait(btc_driver_seven, 20).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/li[6]/button'))
    )
    page_7.click() 
    time.sleep(2)

    btc_driver_eight = webdriver.Chrome() # btc_driver_second 抓取 btc.com第八頁交易資訊
    btc_driver_eight.get('https://explorer.btc.com/btc/unconfirmed-txs')
    btc_driver_eight.execute_script("window.scrollTo(0, document.body.scrollHeight);") 
    page_8 = WebDriverWait(btc_driver_eight, 20).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/li[5]/button'))
    )
    page_8.click()
    time.sleep(2)
    btc_driver_eight.execute_script("window.scrollTo(0, document.body.scrollHeight);") 
    page_8 = WebDriverWait(btc_driver_eight, 20).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/li[6]/button'))
    )
    page_8.click()
    time.sleep(2)
    btc_driver_eight.execute_script("window.scrollTo(0, document.body.scrollHeight);")  
    page_8 = WebDriverWait(btc_driver_eight, 20).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/li[6]/button'))
    )
    page_8.click()
    time.sleep(2)
    btc_driver_eight.execute_script("window.scrollTo(0, document.body.scrollHeight);") 
    page_8 = WebDriverWait(btc_driver_eight, 20).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/li[6]/button'))
    )
    page_8.click()
    time.sleep(2)
    btc_driver_eight.execute_script("window.scrollTo(0, document.body.scrollHeight);") 
    page_8 = WebDriverWait(btc_driver_eight, 20).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/li[6]/button'))
    )
    page_8.click()
    time.sleep(2)

    btc_driver_last = webdriver.Chrome()
    btc_driver_last.get('https://explorer.btc.com/btc/unconfirmed-txs')
    btc_driver_last.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    page_last = WebDriverWait(btc_driver_second, 20).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/li[8]/button'))
    )
    page_last.click() 
    time.sleep(2)
    # try:
    #     一次顯示100頁
    #     page_opthon = WebDriverWait(btc_driver_last, 20).until(
    #         EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/div'))
    #     )
    #     page_opthon.click()
    #     show_100 = WebDriverWait(btc_driver_last, 20).until(
    #         EC.presence_of_element_located((By.XPATH, '//*[@id="btccom-ui-dropdown"]/div/div/div[4]'))
    #     )
    #     show_100.click() 
    # except Exception as ex:
    #     print(f"btc_driver_last hit error: {ex}")



    # ------------開始爬蟲程序------------

    split_count = 1000000000000000  # 輸入檔案切割數量
    interval = 60 # 單位為分鐘(多久儲存一次檔案)
    count = 1

    hashes_seen = deque(maxlen=1000000) # 初始化一個 deque 用來模擬 hashes_seen，儲存看過的交易hashe(設定 deque 的最大長度為 100000)
    header_written = False # 標題寫入的布林值

    while True:
        today_date = datetime.today().strftime('%Y_%m_%d')
        csv_file_name = f"BTX_Transaction_data_{today_date}_{count + 1}.csv"
        
        with open(csv_file_name, 'a', newline='', encoding='utf-8') as csvfile: # 'a' 代表 "append" 模式
            csv_writer = csv.writer(csvfile)
            if not header_written:
                csv_writer.writerow(["System Time", "Txn Hash","Txn Date", "Input Volume", "Output Volume", "Input Count", "Output Count", "Fees", "Total Txn Amount", "Final Txn Date"])
                header_written = True  # 設定為 True，表示標題已經被寫入

        timer = time.time() + interval * 60  # 設置計時器，以間隔時間為單位

        with ThreadPoolExecutor(max_workers=15) as executor:
            while time.time() < timer:
                try:
                    # 使用ThreadPoolExecutor來執行爬蟲函式，一次用五個pipeline去抓資料
                    executor.submit(btc_crawler, btc_driver_first, btc_driver_second, btc_driver_third, btc_driver_four, btc_driver_five, btc_driver_six, btc_driver_seven, btc_driver_eight, btc_driver_last, hashes_seen, header_written).result()
                except Exception as ex:
                    print(f"Error in threading: {ex}")

                if len(hashes_seen) > 1000: # 清系統記憶體deque快取
                    hashes_seen = deque(list(hashes_seen)[len(hashes_seen) // 2:], maxlen=1000000)
                    print('Cleaned hashes_seen')

        print(f'已經過 {count+1} 小時，第 {count+1} 次儲存')
        count += 1

print("Done BTX Web Data Scraping")
