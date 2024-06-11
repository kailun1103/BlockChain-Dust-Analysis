import requests


headers = {
    'Ok-Access-Key': 'ca50076b-f6e9-4799-9d8b-eca0a4c879ae'
}
payload = {
    "chainShortName": "btc",
    "txid": "81b754bb8f75490b499211065dd950b83ac2ffae7be00db9b0f0aa8a75294f52"
}

response = requests.get("https://www.oklink.com/api/v5/explorer/transaction/transaction-fills", headers=headers, params=payload)
print(response.status_code)
print(response.text)
response_data = response.json()  # 将响应数据解析为 JSON 格式

