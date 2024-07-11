import os
from requests import get
from datetime import datetime, timedelta
from openpyxl import Workbook
from shutil import unpack_archive
import pandas as pd

TICKS = 50
OPEN_TIME = 90000
CLOSE_TIME = 133000
MAX_SEARCH_RANGE = 300

today = datetime.now().strftime("%Y_%m_%d")
#today = "2024_07_09"
zip_filename = f"OptionsDaily_{today}.zip"
csv_filename = f"Options/OptionsDaily_{today}.csv"


def get_option_daily_zip():
    url = f"https://www.taifex.com.tw/file/taifex/Dailydownload/OptionsDailydownloadCSV/{zip_filename}"
    print(f"開始下載 {url} ...")
    res = get(url, timeout=10)
    res.raise_for_status()
    with open(f"Options/{zip_filename}", 'wb') as f:
        f.write(res.content)
    print(f"{zip_filename} 下載成功 !")


def get_week_code(date_str, for_1330=True):
    date = datetime.strptime(date_str, "%Y_%m_%d")

    current_weekday = date.weekday()  # 星期幾（0=星期一, 6=星期日）
    delta_day_to_Wednesday = (2-current_weekday + 7) % 7 # 星期三為 2
    next_Wednesday = date + timedelta(days=delta_day_to_Wednesday)

    if next_Wednesday.day % 7 == 0: # 判斷是當月第幾個禮拜三
        week_num = next_Wednesday.day // 7
    else:
        week_num = next_Wednesday.day // 7 + 1

    if week_num != 3:
        week_code_for_0900 = f"{next_Wednesday.year}{next_Wednesday.month:02d}W{week_num}"
    else:
        week_code_for_0900 = f"{next_Wednesday.year}{next_Wednesday.month:02d}"

    if for_1330:
        if current_weekday == 2: # 如果是結算日則 1330 需要看下一期的
            next_Wednesday = (date + timedelta(days=7)).strftime("%Y_%m_%d")
            week_code_for_1330 = get_week_code(next_Wednesday, for_1330=False)
        else:
            week_code_for_1330 = week_code_for_0900
        return week_code_for_0900, week_code_for_1330
    else:
        return week_code_for_0900


def get_call_and_put(final_time, week_code):
    df = pd.read_csv(csv_filename, encoding="big5", low_memory=False)

    df.columns = df.columns.str.strip()  # 去除欄位名稱的空格
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)  # 去除每個欄位值的空格

    for _t in range(final_time, final_time+MAX_SEARCH_RANGE):
        print(f"當前成交時間 = {_t}")
        filtered_df = df[(df['商品代號'] == 'TXO') & (df['成交時間'] == _t) & (df['到期月份(週別)'] == week_code)]

        Call_dict = {}
        Put_dict = {}

        for index, row in filtered_df.iterrows():
            strike_price = int(row["履約價格"])
            final_price = row["成交價格"]
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

        common_strike_price = sorted(set(Put_dict.keys()) & set(Call_dict.keys())) # 把相同的履約價提出

        for sp in common_strike_price:
            if Call_dict[sp] > Put_dict[sp]: # 便宜履約價 case
                next_sp = sp+TICKS
                if all(next_sp in _dict for _dict in [Call_dict, Put_dict]): # 下一個 tick 有 Call 跟 Put
                    if Call_dict[next_sp] < Put_dict[next_sp]: # 如果昂貴履約價的 Call 小於 Put
                        print(f"{'C':<5}{Call_dict[sp]:<10}{sp:<10}{Put_dict[sp]:>5}{'P':>5}")
                        print(f"{'C':<5}{Call_dict[next_sp]:<10}{next_sp:<10}{Put_dict[next_sp]:>5}{'P':>5}")
                        break
                elif next_sp in Call_dict: # 單一確認昂貴履約價的 Call
                    if Call_dict[next_sp] <= Put_dict[sp]: # 昂貴履約價只有 C 成交，同時昂貴 C <= 便宜 P
                        print(f"{'C':<5}{Call_dict[sp]:<10}{sp:<10}{Put_dict[sp]:>5}{'P':>5}")
                        print(f"{'C':<5}{Call_dict[next_sp]:<10}{next_sp:<10}{'NaN':>5}{'P':>5}")
                        break

            elif Call_dict[sp] < Put_dict[sp]: # 昂貴履約價 case
                prev_sp = sp-TICKS
                if all(prev_sp in _dict for _dict in [Call_dict, Put_dict]): # 上一個 tick 有 Call 跟 Put
                    if Call_dict[prev_sp] > Put_dict[prev_sp]: # 如果昂貴履約價的 Call 大於 Put
                        print(f"{'C':<5}{Call_dict[prev_sp]:<10}{prev_sp:<10}{Put_dict[prev_sp]:>5}{'P':>5}")
                        print(f"{'C':<5}{Call_dict[sp]:<10}{sp:<10}{Put_dict[sp]:>5}{'P':>5}")
                        break
                elif prev_sp in Put_dict: # 單一確認便宜履約價的 Put
                    if Call_dict[sp] >= Put_dict[prev_sp]: # 便宜履約價只有 P 成交，同時昂貴 C >= 便宜 P
                        print(f"{'C':<5}{'NaN':<10}{prev_sp:<10}{Put_dict[prev_sp]:>5}{'P':>5}")
                        print(f"{'C':<5}{Call_dict[sp]:<10}{sp:<10}{Put_dict[sp]:>5}{'P':>5}")
                        break
        else:
            print(f"{_t} - 無法找到符合項目")
            continue

        break



if __name__ == "__main__":

    # get_option_daily_zip()
    # unpack_archive(f"Options/{zip_filename}", "Options") # 解壓縮
    week_code_for_0900, week_code_for_1330 = get_week_code(today)
    get_call_and_put(OPEN_TIME, week_code_for_0900)

    get_call_and_put(CLOSE_TIME, week_code_for_1330)



