import csv
import os
from datetime import datetime
csv.field_size_limit(2147483647)

csv_file_path = 'step4 資料清洗(invalid or amount為0)'
# csv_file_path = 'step4 資料清洗(invalid or amount為0)'
total = 0
Input_Volume = []
Output_Volume = []
Input_Count = []
Output_Count = []
Fees = []
Verification_Time = []
txnCount = []
amount = []
blockSize = []
mineReward = []
totalFee = []

total = 0


for root, dirs, files in os.walk(csv_file_path):
    csv_files = [file for file in files if file.endswith('.csv')]
    for csv_file in csv_files:
        # time.sleep(0.5)
        csv_path = os.path.join(root, csv_file)
        print(csv_path)

        with open(csv_path, mode='r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            header = next(reader)  # 讀取並丟棄標題行
            count = 0
            for row in reader:
                
                # 小於等於 3600 秒
                # if int(row[14]) > 3600:
                #     count += 1
                #     Input_Volume.append(float(row[2]))
                #     Output_Volume.append(float(row[3]))
                #     Input_Count.append(float(row[4]))
                #     Output_Count.append(float(row[5]))
                #     Fees.append(float(row[6]))
                #     Verification_Time.append(float(row[14]))
                #     txnCount.append(float(row[22]))
                #     amount.append(float(row[23]))
                #     blockSize.append(float(row[24]))
                #     mineReward.append(float(row[25]))
                #     totalFee.append(float(row[26]))

                # 大於 3600 秒
                if int(row[14]) <= 3600:
                    count += 1
                    Input_Volume.append(float(row[2]))
                    Output_Volume.append(float(row[3]))
                    Input_Count.append(float(row[4]))
                    Output_Count.append(float(row[5]))
                    Fees.append(float(row[6]))
                    Verification_Time.append(float(row[14]))
                    if row[22] == 'null':
                        txnCount.append(float(0))
                        amount.append(float(0))
                        blockSize.append(float(0))
                        mineReward.append(float(0))
                        totalFee.append(float(0))
                    else:
                        txnCount.append(float(row[22]))
                        amount.append(float(row[23]))
                        blockSize.append(float(row[24]))
                        mineReward.append(float(row[25]))
                        totalFee.append(float(row[26]))

                # 直接計算(無任何條件)
                # count += 1
                # Input_Volume.append(float(row[2]))
                # Output_Volume.append(float(row[3]))
                # Input_Count.append(float(row[4]))
                # Output_Count.append(float(row[5]))
                # Fees.append(float(row[6]))
                # Verification_Time.append(float(row[14]))
                # if row[22] == 'null':
                #     txnCount.append(float(0))
                #     amount.append(float(0))
                #     blockSize.append(float(0))
                #     mineReward.append(float(0))
                #     totalFee.append(float(0))
                # else:
                #     txnCount.append(float(row[22]))
                #     amount.append(float(row[23]))
                #     blockSize.append(float(row[24]))
                #     mineReward.append(float(row[25]))
                #     totalFee.append(float(row[26]))

            total += count

Input_Volume_average_value = sum(Input_Volume) / len(Input_Volume)
Output_Volume_average_value = sum(Output_Volume) / len(Output_Volume)
Input_Count_average_value = sum(Input_Count) / len(Input_Count)
Output_Count_average_value = sum(Output_Count) / len(Output_Count)
Fees_average_value = sum(Fees) / len(Fees)
Verification_Time_average_value = sum(Verification_Time) / len(Verification_Time)
txnCount_average_value = sum(txnCount) / len(txnCount)
amount_average_value = sum(amount) / len(amount)
blockSize_average_value = sum(blockSize) / len(blockSize)
mineReward_average_value = sum(mineReward) / len(mineReward)
totalFee_average_value = sum(totalFee) / len(totalFee)

print(f"total txn count:{total}")
print(f"Input Volume Average Value: {Input_Volume_average_value}")
print(f"Output Volume Average Value: {Output_Volume_average_value}")
print(f"Input Count Average Value: {Input_Count_average_value}")
print(f"Output Count Average Value: {Output_Count_average_value}")
print(f"Fees Average Value: {Fees_average_value}")
print(f"Verification Time Average Value: {Verification_Time_average_value}")
print(f"Transaction Count Average Value: {txnCount_average_value}")
print(f"Amount Average Value: {amount_average_value}")
print(f"Block Size Average Value: {blockSize_average_value}")
print(f"Mine Reward Average Value: {mineReward_average_value}")
print(f"Total Fee Average Value: {totalFee_average_value}")




# print(f"平均值: {average_value}")