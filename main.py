import os
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Border, Font, NamedStyle, Side
from openpyxl.styles.numbers import NumberFormat
from requests import get, post
from shutil import unpack_archive
from time import sleep

MAX_RETRIES = 5
TICKS = 50
OPEN_TIME = 90000
CLOSE_TIME = 133000
MAX_SEARCH_RANGE = 300

def find_latest_date_in_excel(sheet) -> datetime:
    latest_date = None
    for cell in sheet["G"]: # 使用 Call 成交價進行確認
        if cell.value:
            latest_date = sheet[f"A{cell.row}"].value
        else:
            write_row = cell.row
            break
    
    if not isinstance(latest_date, datetime):
        latest_date = datetime.strptime(latest_date, "%Y/%m/%d")
    
    return latest_date, write_row


def get_previous_30_trading_days() -> list:
    url = f"https://www.taifex.com.tw/cht/3/optPrevious30DaysSalesData"
    print(f"開始取得前 30 個交易日期 {url}...")
    for retry in range(MAX_RETRIES):
        try:
            res = get(url, timeout=10)
            res.raise_for_status()

            dates_30 = []
            soup = BeautifulSoup(res.text, "html.parser")
            datas = soup.select("table.table_f td:nth-of-type(2)")
            for data in datas:
                dates_30.append(data.text)
            print(f"{dates_30=}")

            # dates_30=['2024/07/12', '2024/07/11', ... , '2024/05/31'] 總共 30 天
            return dates_30

        except Exception as e:
            print(f"重新取得中 ... {retry+1}/{MAX_RETRIES}")
            sleep(3)
    else:
        print("資料取得失敗")
        os._exit(1)


def get_option_daily_zip(zip_filename):
    url = f"https://www.taifex.com.tw/file/taifex/Dailydownload/OptionsDailydownloadCSV/{zip_filename}"
    print(f"開始下載 {url} ...")
    for retry in range(MAX_RETRIES):
        try:
            res = get(url, timeout=10)
            res.raise_for_status()
            with open(f"Options/{zip_filename}", 'wb') as f:
                f.write(res.content)
            print(f"{zip_filename} 下載成功 !")
            return
        except Exception as e:
            print(f"重新下載中 ... {retry+1}/{MAX_RETRIES}")
            sleep(3)
    else:
        print("下載資料失敗")
        os._exit(1)


def get_week_code(date, for_1330=True):
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
            next_Wednesday = date + timedelta(days=7)
            week_code_for_1330 = get_week_code(next_Wednesday, for_1330=False)
        else:
            week_code_for_1330 = week_code_for_0900
        return week_code_for_0900, week_code_for_1330
    else:
        return week_code_for_0900


def get_call_and_put(final_time, week_code, csv_filename):
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

        call_strike = None
        call_deal = None         
        put_strike = None
        put_deal = None

        for sp in common_strike_price:
            if Call_dict[sp] > Put_dict[sp]: # 便宜履約價 case
                next_sp = sp+TICKS
                if all(next_sp in _dict for _dict in [Call_dict, Put_dict]): # 下一個 tick 有 Call 跟 Put
                    if Call_dict[next_sp] < Put_dict[next_sp]: # 如果昂貴履約價的 Call 小於 Put
                        print(f"{'C':<5}{Call_dict[sp]:<10}{sp:<10}{Put_dict[sp]:>5}{'P':>5}")
                        print(f"{'C':<5}{Call_dict[next_sp]:<10}{next_sp:<10}{Put_dict[next_sp]:>5}{'P':>5}")
                        call_strike = next_sp
                        call_deal = Call_dict[next_sp]
                        put_strike = sp
                        put_deal = Put_dict[sp]
                        break
                elif next_sp in Call_dict: # 單一確認昂貴履約價的 Call
                    if Call_dict[next_sp] <= Put_dict[sp]: # 昂貴履約價只有 C 成交，同時昂貴 C <= 便宜 P
                        print(f"{'C':<5}{Call_dict[sp]:<10}{sp:<10}{Put_dict[sp]:>5}{'P':>5}")
                        print(f"{'C':<5}{Call_dict[next_sp]:<10}{next_sp:<10}{'NaN':>5}{'P':>5}")
                        call_strike = next_sp
                        call_deal = Call_dict[next_sp]
                        put_strike = sp
                        put_deal = Put_dict[sp]
                        break

            elif Call_dict[sp] < Put_dict[sp]: # 昂貴履約價 case
                prev_sp = sp-TICKS
                if all(prev_sp in _dict for _dict in [Call_dict, Put_dict]): # 上一個 tick 有 Call 跟 Put
                    if Call_dict[prev_sp] > Put_dict[prev_sp]: # 如果昂貴履約價的 Call 大於 Put
                        print(f"{'C':<5}{Call_dict[prev_sp]:<10}{prev_sp:<10}{Put_dict[prev_sp]:>5}{'P':>5}")
                        print(f"{'C':<5}{Call_dict[sp]:<10}{sp:<10}{Put_dict[sp]:>5}{'P':>5}")
                        call_strike = sp
                        call_deal = Call_dict[sp]
                        put_strike = prev_sp
                        put_deal = Put_dict[prev_sp]
                        break
                elif prev_sp in Put_dict: # 單一確認便宜履約價的 Put
                    if Call_dict[sp] >= Put_dict[prev_sp]: # 便宜履約價只有 P 成交，同時昂貴 C >= 便宜 P
                        print(f"{'C':<5}{'NaN':<10}{prev_sp:<10}{Put_dict[prev_sp]:>5}{'P':>5}")
                        print(f"{'C':<5}{Call_dict[sp]:<10}{sp:<10}{Put_dict[sp]:>5}{'P':>5}")
                        call_strike = sp
                        call_deal = Call_dict[sp]
                        put_strike = prev_sp
                        put_deal = Put_dict[prev_sp]
                        break
        else:
            print(f"{_t} - 無法找到符合項目")
            continue

        break

    return call_strike, call_deal, put_strike, put_deal


def get_settlement_price(date):

    date_str = date.strftime("%Y/%m/%d")

    url = "https://www.taifex.com.tw/cht/5/optIndxFSP"
    payloads = {
        "commodityIds": "2",
        "_all": "on",
        "start_year": str(date.year),
        "start_month": f"{excel_latest_date.month:02d}",
        "end_year": str(date.year),
        "end_month": f"{excel_latest_date.month:02d}",
        "button": "送出查詢",
    }
    print(f"開始取得 {date_str} 結算價 - {url} ...")
    for retry in range(MAX_RETRIES):
        try:
            res = post(url, data=payloads, timeout=10)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "html.parser")
            datas = soup.select("table.table_f tbody tr")
            for data in datas:
                settlement_date = data.find_all("td")[0].text.strip()
                if date_str == settlement_date: # 確認是相同日期，則取往下得當週結算價
                    settlement_price = int(data.find_all("td")[2].text)
                    print(f"{date_str} 結算價為 {settlement_price}")
                    return settlement_price
 
        except Exception as e:
            print(f"重新取得 {date} 結算價中 ... {retry+1}/{MAX_RETRIES}")
            sleep(3)
    else:
        print(f"取得 {date} 結算價失敗")
        os._exit(1)


if __name__ == "__main__":

    wb = load_workbook("Options.xlsx")
    try:
        wb.save("Options.xlsx")
    except:
        print("關閉 Options.xlsx 後, 再次執行程式")
        os.system("pause")
        os._exit(1)
    sheet = wb["純紀錄"]

    alignment = Alignment(horizontal='center', vertical='center')
    font = Font(name='Microsoft JhengHei UI', size=10)

    excel_latest_date, write_row = find_latest_date_in_excel(sheet)
    print(f"Excel 最新日期為 {excel_latest_date}")

    dates_30 = get_previous_30_trading_days()
    # dates_30 = ['2024/07/12', '2024/07/11', '2024/07/10', '2024/07/09', '2024/07/08', '2024/07/05', '2024/07/04', '2024/07/03', '2024/07/02', '2024/07/01', '2024/06/28', '2024/06/27', '2024/06/26', '2024/06/25', '2024/06/24', '2024/06/21', '2024/06/20', '2024/06/19', '2024/06/18', '2024/06/17', '2024/06/14', '2024/06/13', '2024/06/12', '2024/06/11', '2024/06/07', '2024/06/06', '2024/06/05', '2024/06/04', '2024/06/03', '2024/05/31']
    if excel_latest_date.strftime("%Y/%m/%d") not in dates_30:
        print(f"Excel 最新日期為 {excel_latest_date}, 已超過 30 天內能取得之資料")
        os._exit(1)

    for _d in dates_30[::-1]:
        current_date = datetime.strptime(_d, "%Y/%m/%d")
        if current_date <= excel_latest_date:
            continue
        current_date_str = current_date.strftime("%Y_%m_%d")
        print(f"執行日期: {current_date_str}".ljust(80, "="))

        zip_filename = f"OptionsDaily_{current_date_str}.zip"
        get_option_daily_zip(zip_filename)
        unpack_archive(f"Options/{zip_filename}", "Options") # 解壓縮

        week_code_for_0900, week_code_for_1330 = get_week_code(current_date)

        csv_filename = f"Options/OptionsDaily_{current_date_str}.csv"
        call_strike_0900, call_deal_0900, put_strike_0900, put_deal_0900 = get_call_and_put(OPEN_TIME, week_code_for_0900, csv_filename)
        call_strike_1330, call_deal_1330, put_strike_1330, put_deal_1330 = get_call_and_put(CLOSE_TIME, week_code_for_1330, csv_filename)

        print(f"{put_strike_0900=}")
        print(f"{put_deal_0900=}")
        print(f"{call_strike_0900=}")
        print(f"{call_deal_0900=}")
        print(f"{put_strike_1330=}")
        print(f"{put_deal_1330=}")
        print(f"{call_strike_1330=}")
        print(f"{call_deal_1330=}")

        sheet[f"A{write_row}"].value = current_date
        sheet[f"A{write_row}"].font = Font(name='微軟正黑體', size=8)
        sheet[f"A{write_row}"].number_format = "YYYY/M/D"

        sheet[f"A{write_row+1}"].value = current_date
        sheet[f"A{write_row+1}"].font = Font(name='微軟正黑體', size=8)
        sheet[f"A{write_row+1}"].number_format = "YYYY/M/D"

        sheet[f"E{write_row}"].value = week_code_for_0900
        sheet[f"E{write_row}"].alignment = Alignment(horizontal='right', vertical='center')
        sheet[f"E{write_row}"].font = Font(name='Microsoft JhengHei UI', size=8)
        sheet[f"E{write_row}"].number_format = "General"

        sheet[f"E{write_row+1}"].value = week_code_for_1330
        sheet[f"E{write_row+1}"].alignment = Alignment(horizontal='right', vertical='center')
        sheet[f"E{write_row+1}"].font = Font(name='Microsoft JhengHei UI', size=8)
        sheet[f"E{write_row+1}"].number_format = "General"

        sheet[f"G{write_row}"].value = call_deal_0900
        sheet[f"G{write_row}"].alignment = alignment
        sheet[f"G{write_row}"].font = font
        sheet[f"G{write_row}"].number_format = "0.0"
        
        sheet[f"G{write_row+1}"].value = call_deal_1330
        sheet[f"G{write_row+1}"].alignment = alignment
        sheet[f"G{write_row+1}"].font = font
        sheet[f"G{write_row+1}"].number_format = "0.0"

        sheet[f"H{write_row}"].value = call_strike_0900
        sheet[f"H{write_row}"].alignment = alignment
        sheet[f"H{write_row}"].font = font
        sheet[f"H{write_row}"].number_format = "General"

        sheet[f"H{write_row+1}"].value = call_strike_1330
        sheet[f"H{write_row+1}"].alignment = alignment
        sheet[f"H{write_row+1}"].font = font
        sheet[f"H{write_row+1}"].number_format = "General"

        sheet[f"I{write_row}"].value = put_strike_0900
        sheet[f"I{write_row}"].alignment = alignment
        sheet[f"I{write_row}"].font = font
        sheet[f"I{write_row}"].number_format = "General"

        sheet[f"I{write_row+1}"].value = put_strike_1330
        sheet[f"I{write_row+1}"].alignment = alignment
        sheet[f"I{write_row+1}"].font = font
        sheet[f"I{write_row+1}"].number_format = "General"

        sheet[f"J{write_row}"].value = put_deal_0900
        sheet[f"J{write_row}"].alignment = alignment
        sheet[f"J{write_row}"].font = font
        sheet[f"J{write_row}"].number_format = "0.0"

        sheet[f"J{write_row+1}"].value = put_deal_1330
        sheet[f"J{write_row+1}"].alignment = alignment
        sheet[f"J{write_row+1}"].font = font
        sheet[f"J{write_row+1}"].number_format = "0.0"

        if current_date.weekday() == 2: # 如果是星期三則要多寫回 結算價
            row_for_settlement = write_row
            settlement_price = get_settlement_price(current_date)
            while sheet[f"E{row_for_settlement}"].value == week_code_for_0900:
                C_settlement_price = settlement_price - int(sheet[f"H{row_for_settlement}"].value)
                if C_settlement_price <= 0: C_settlement_price = 0

                P_settlement_price = int(sheet[f"I{row_for_settlement}"].value) - settlement_price
                if P_settlement_price <= 0: P_settlement_price = 0

                sheet[f"F{row_for_settlement}"].value = C_settlement_price
                sheet[f"F{row_for_settlement}"].alignment = alignment
                sheet[f"F{row_for_settlement}"].font = font
                sheet[f"F{row_for_settlement}"].number_format = "General"

                sheet[f"K{row_for_settlement}"].value = P_settlement_price
                sheet[f"K{row_for_settlement}"].alignment = alignment
                sheet[f"K{row_for_settlement}"].font = font
                sheet[f"K{row_for_settlement}"].number_format = "General"


                print(current_date)
                print(type(current_date))
                print(sheet[f"A{row_for_settlement}"].value)
                print(type(sheet[f"A{row_for_settlement}"].value))                
                if current_date == sheet[f"A{row_for_settlement}"].value: # 加入下邊框
                    for col in range(1, 13):
                        sheet.cell(row=row_for_settlement, column=col).border = Border(bottom=Side(style='thin'))

                row_for_settlement = row_for_settlement - 1

        wb.save("Options.xlsx")

        write_row = write_row + 2

        print(f"".ljust(80, "="))
    
    print("已寫入完成 !")
    os.system("pause")