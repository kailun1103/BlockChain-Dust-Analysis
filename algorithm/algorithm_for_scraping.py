import requests
from selenium import webdriver
from bs4 import BeautifulSoup

driver = webdriver.Chrome()
driver.get('https://explorer.btc.com/btc/unconfirmed-txs')
soup = BeautifulSoup(driver.page_source, 'html.parser')

# step1. Get transaction information from mempool
txn = soup.select('tr')
txn_Detail = txn.select_one('td:nth-of-type(1) a')
Txn_Hash = txn_Detail.get('href').split('/')[-1]
Txn_Initiation_Date = txn.select_one('td:nth-of-type(2) div')
Txn_Input_Amount = txn.select_one('td:nth-of-type(3)')
Txn_Output_Amount = txn.select_one('td:nth-of-type(4)')
Txn_Fees = txn.select_one('td:nth-of-type(6)')

# step2. Confirm whether the transaction was verified by the miner
headers={"Ok-Access-Key":"your_ok_access_key"}
payload={"chainShortName":"btc","txid":Txn_Hash}
response = requests.get("https://www.oklink.com/api/v5/explorer/transaction/transaction-fills", headers=headers, params=payload)
if response.status_code == 200:
    txn_info=response.json()["data"][0]
    Txn_Block=txn_info.get("height")
    Txn_Time=txn_info.get("transactionTime")
    Txn_Confirm=txn_info.get("confirm")
    Txn_Input_Address=txn_info.get("inputDetails")
    Txn_Output_Address=txn_info.get("outputDetails")
    Txn_State=txn_info.get("state")
    Verification_Time=Txn_Time-Txn_Initiation_Date

# step3. If the transaction is verified by the miner, the block data of the transaction will be obtained.
response = requests.get(f"https://www.oklink.com/api/v5/explorer/block/block-fills?chainShortName=btc&height={Txn_Block}", headers=headers)
if response.status_code == 200:
    block_info = response.json()["data"][0]
    Block_Txn_Count = block_info.get("txnCount")
    Block_Total_Amount = block_info.get("amount")
    Block_Size = block_info.get("blockSize")
    Miner_Reward = block_info.get("mineReward")
    Block_Total_Fee = block_info.get("totalFee")