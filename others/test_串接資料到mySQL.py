import pandas as pd
import pymysql
from datetime import datetime

# 連線至 MySQL 資料庫
connection = pymysql.connect(
    host='127.0.0.1',
    user='chang',
    password='19940824nan.W',
    database='eth database'
)

# 建立游標物件
cursor = connection.cursor()
# 生成今天日期作為表名
today_date = datetime.today().strftime('%Y_%m_%d')
table_name = f"BTX_Transaction_{today_date}"

csv_file_name = f"Pending Transactions/BTX_Transaction_data_{today_date}.csv"
data = pd.read_csv(csv_file_name)

# 將資料寫入 MySQL 資料庫
try:
    # 為表格建立 SQL 表
    create_table_query = f'''CREATE TABLE IF NOT EXISTS {table_name} (
                        `Txn_Hash` VARCHAR(255),
                        `Time` DATETIME,
                        `Input_Volume(BTC)` FLOAT,
                        `Output_Volume(BTC)` FLOAT,
                        `Fees(BTC)` FLOAT
                        )'''
    cursor.execute(create_table_query)
    # 逐行將資料寫入資料庫
    for index, row in data.iterrows():
        sql_query = f"INSERT INTO {table_name} (`Txn_Hash`, `Time`, `Input_Volume(BTC)`, `Output_Volume(BTC)`, `Fees(BTC)`) VALUES (%s, %s, %s, %s, %s)"
        cursor.execute(sql_query, tuple(row))
    # 提交至資料庫執行
    connection.commit()
    print("Data is successfully written to the MySQL database")
    
except Exception as ex:
    print(f"Hit error ：{str(ex)}")
    # 如果發生錯誤，則回滾更改
    connection.rollback()

finally:
    # 關閉游標和資料庫連接
    cursor.close()
    connection.close()