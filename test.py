import os
from requests import get
from datetime import datetime, timedelta
from openpyxl import Workbook
from zipfile import ZipFile
import pandas as pd


today = datetime.now().strftime("%Y_%m_%d")
zip_filename = f"OptionsDaily_{today}.zip"
csv_filename = f"OptionsDaily_{today}.csv"

def get_week_code(date_str):
    date = datetime.strptime(date_str, "%Y_%m_%d")
    
    # 獲取當月的第一天
    first_day_of_month = date.replace(day=1) # EX: 2024-07-01 00:00:00
    
    # 計算該日期是當月的第幾週
    first_weekday = first_day_of_month.weekday()  # 第一週的第一天是星期幾（0=星期一, 6=星期日）
    
    # 計算從當月第一天到當前日期過去了多少天
    days_passed = date.day + first_weekday - 1

    # 計算該日期是當月的第幾週
    week_of_month = days_passed // 7 + 1
    
    # 格式化結果
    if week_of_month != 3:
        week_code = f"{date.year}{date.month:02d}W{week_of_month}"
    else:
        week_code = f"{date.year}{date.month:02d}"

    if date.weekday() == 2:
        is_Wednesday = True
    else:
        is_Wednesday = False

    return week_code, is_Wednesday

# url = f"https://www.taifex.com.tw/file/taifex/Dailydownload/OptionsDailydownloadCSV/{zip_filename}"

# print("Go!")
# res = get(url, timeout=10)
# print("Yo!")
# res.raise_for_status()
# with open(zip_filename, 'wb') as f:
#     f.write(res.content)
# print("文件下載成功")

# zip_file_path = zip_filename
# extract_dir = os.path.dirname(__file__)

# # 打開 ZIP 文件並解壓縮
# with ZipFile(zip_file_path, 'r') as zip_ref:
#     zip_ref.extractall(extract_dir)



df = pd.read_csv(csv_filename, encoding="big5", low_memory=False)

df.columns = df.columns.str.strip()  # 去除欄位名稱的空格
df = df.map(lambda x: x.strip() if isinstance(x, str) else x)  # 去除每個欄位值的空格

        
filtered_df = df[(df['商品代號'] == 'TXO') & (df['成交時間'] == 90000) & (df['到期月份(週別)'] == get_week_code(today)[0])]

Call_dict = {}
Put_dict = {}

for index, row in filtered_df.iterrows():
    strike_price = int(row["履約價格"])
    final_price = int(row["成交價格"])
    if row["買賣權別"] == "C":
        if strike_price in Call_dict:
            if final_price < Call_dict[strike_price]: # 同履約價，但成交價更小，則替換
                Call_dict[strike_price] = final_price
        else:
            Call_dict[strike_price] = final_price

    elif row["買賣權別"] == "P":
        if strike_price in Put_dict:
            if final_price > Put_dict[strike_price]: # 同履約價，但成交價更大，則替換
                Put_dict[strike_price] = final_price
        else:
            Put_dict[strike_price] = final_price

# 弄好 week 問題  差禮拜三 1330 不知怎麼取
# 取得大小依照 document