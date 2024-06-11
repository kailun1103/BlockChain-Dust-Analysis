import csv
import os
import time
from datetime import datetime
csv.field_size_limit(2147483647)

# 定義多種日期時間格式
datetime_formats = [
    '%Y/%m/%d %H:%M',        # 例如：2024/1/18 00:00
    '%Y/%m/%d %I:%M:%S %p',  # 例如：2024/01/18 12:02:11 AM
    '%Y-%m-%d %H:%M:%S',     # 例如：2024-01-19 01:44:01
]

def parse_datetime(datetime_str):
    for fmt in datetime_formats:
        try:
            return datetime.strptime(datetime_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Time data '{datetime_str}' does not match any known formats.")


# csv_file_path = 'test'
csv_file_path = 'step4 資料清洗(invalid or amount為0)'

for root, dirs, files in os.walk(csv_file_path):
    csv_files = [file for file in files if file.endswith('.csv')]
    for csv_file in csv_files:
        time.sleep(0.5)
        csv_path = os.path.join(root, csv_file)
        temp_csv_path = os.path.join(root, 'temp_' + csv_file)

        with open(csv_path, mode='r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            header = next(reader)
            # header.append('Verification Time')
            # header.append('Dust Bool')

            # 打開臨時的 CSV 文件進行寫入
            with open(temp_csv_path, mode='w', newline='', encoding='utf-8') as outfile:
                writer = csv.writer(outfile)
                writer.writerow(header)  # 將標題行寫入臨時文件

            #     # # 第一步: 將過長欄位的文字數刪除後半段
            #     for row in reader:
            #         # 檢查 row[12] 和 row[13] 的內容長度
            #         if len(row) > 13:
            #             if len(row[13]) > 10000:
            #                 row[13] = row[13][:500]  # 將 row[13] 的長度減少到 500
            #             if len(row[12]) > 10000:
            #                 row[12] = row[12][:500]  # 將 row[12] 的長度減少到 500
            #         writer.writerow(row)  # 將處理後的行寫入臨時文件
          

          
                # 第二步: 計算打包時間
                # for row in reader:
                #     # 解析和打印時間
                #     # print(row)
                #     start_time_str = row[1]
                #     end_time_str = row[10]

                #     if end_time_str == 'null':
                #         time_diff = 0
                #     else:
                #         start_time = parse_datetime(start_time_str)
                #         end_time = parse_datetime(end_time_str)
                #         # 計算時間差
                #         time_diff = int((end_time - start_time).total_seconds())

                #     row.append(time_diff)  # 在當前行末尾新增 "Verification Time" 列
                #     writer.writerow(row)  # 將新行寫入輸出文件


                # # 第三步: 粉塵標籤
                for row in reader:
                    if row[3] == '' or row[6] == "":
                        continue

                    amount = float(row[3])
                    fee = float(row[6])
                    input_count = int(row[4])
                    output_count = int(row[5])
                    state = row[9]

                    # if amount == 0:
                    #     fee_rate = 100
                    # else:
                    #     fee_rate = (fee/amount) * 100

                    # if fee_rate >= 20 and state == 'Confirmed': # 確保沒有用到invalid
                    #     dust = 1
                    # else:
                    #     dust = 0

                    if amount <= 0.00001 and state == 'Confirmed': # 確保沒有用到invalid
                        dust = 1
                    else:
                        dust = 0

                    row.append(dust)  # 在當前行末尾新增"Dust Bool"列
                    writer.writerow(row)  # 將新行寫入輸出文件


        # 用臨時文件覆蓋原始文件
        os.replace(temp_csv_path, csv_path)

        print(f"已完成檢查並更新 CSV 文件：{csv_file}")
