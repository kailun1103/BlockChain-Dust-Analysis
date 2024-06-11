import requests
from bs4 import BeautifulSoup
import re

def extract_addresses_from_elements(elements):
    addresses = []
    for address_element in elements:
        href = address_element['href']
        match = re.search(r'/btc/address/([^"]+)', href)
        if match:
            address = match.group(1)
            addresses.append(address)
    return addresses

# 請求網頁內容
hash = '0100d3a0e13b6511ca0cf00a96286828e00a508f7eb38ba8baf0c0f92d669348'
url = f'https://explorer.btc.com/btc/transaction/{hash}'
response = requests.get(url)

# 使用BeautifulSoup解析HTML
BTC_page = BeautifulSoup(response.text, 'html.parser')

# 地址欄位的資訊
status_elements = BTC_page.select('.TxListItem_list-items__2Ganp')
input_elements = status_elements[0].find_all('a', class_='monospace')
output_elements = status_elements[1].find_all('a', class_='monospace')

# 拿到所有輸入和輸出地址
input_addresses = extract_addresses_from_elements(input_elements)
output_addresses = extract_addresses_from_elements(output_elements)

# 打印地址
print(input_addresses)
print('------------')
print(output_addresses)