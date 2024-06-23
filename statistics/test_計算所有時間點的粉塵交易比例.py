import csv
import os
from datetime import datetime

# Define multiple date-time formats
datetime_formats = [
    '%Y/%m/%d %H:%M',  # Example: 2024/1/18 00:00
    '%Y/%m/%d %I:%M:%S %p',  # Example: 2024/01/18 12:02:11 AM
    '%Y-%m-%d %H:%M:%S',  # Example: 2024-01-19 01:44:01
]

def parse_datetime(datetime_str):
    for fmt in datetime_formats:
        try:
            return datetime.strptime(datetime_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Time data '{datetime_str}' does not match any known formats.")

# Set the maximum field size to avoid field errors
csv.field_size_limit(2147483647)
csv_file_path = '2024_01_18-2024_01_24 - 資料集'

# Dictionary to hold hourly counts
hourly_counts = {hour: 0 for hour in range(24)}

# Traverse through files and folders
for root, dirs, files in os.walk(csv_file_path):
    csv_files = [file for file in files if file.endswith('.csv')]
    for csv_file in csv_files:
        print(csv_file)
        csv_path = os.path.join(root, csv_file)
        with open(csv_path, mode='r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            next(reader)  # Skip the header
            for row in reader:
                # if row[15] == "1":
                date = parse_datetime(row[1])
                hourly_counts[date.hour] += 1

# Print out the hourly counts
for hour, count in hourly_counts.items():
    print(count)

print('--------------------')
print('Total:', sum(hourly_counts.values()))
