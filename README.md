# option_stock

# C, P 結算價規則
K欄 : Put - 結算價
F欄 : 結算價 - Call
如果為負值，則記為 0

# 取價規則
1. Put 在同一履約價格出現不同成交價時，選擇最大值
2. Call 在同一履約價格出現不同成交價時，選擇最小值
3. 依照加權指數的開盤或收盤，取的前後六個 ticks 的 Call 與 Put，
如過在下一個 tick 缺失數據的話，將會沿用數據，
如果完全無符合則為 0

# 合約名稱規則
1. 給予指定日期，不為禮拜三的話往後推到禮拜三的日期
    * 確認此禮拜三日期是第幾個禮拜三就是 W 多少