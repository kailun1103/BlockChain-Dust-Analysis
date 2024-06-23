import requests


headers = {
    'Ok-Access-Key': 'b5bae772-4c29-49bd-912b-7005e275837a'
}
payload = {
    "chainShortName": "btc",
    "txid": "18a2c6d2fb2c1a23744007467c8990aea907be188602e3231d6f80b8a491aa6f"
}

response = requests.get("https://www.oklink.com/api/v5/explorer/transaction/transaction-fills", headers=headers, params=payload)
print(response.status_code)
print(response.text)
response_data = response.json()  # 将响应数据解析为 JSON 格式

