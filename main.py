from yahoo_finance_api2 import share
from yahoo_finance_api2.exceptions import YahooFinanceError
from datetime import date, datetime
from make_codes_list import get_codes
import pandas as pd
import time
import os

#東証コードのリストを取得
codes = get_codes()

# テスト用codeを使用し、periodを決定
test_code = '6501'
period = 1

for num in range(1, 15):
    time.sleep(2)
    my_share = share.Share(test_code + ".T")
    symbol_data = None

    try:
        symbol_data = my_share.get_historical(share.PERIOD_TYPE_DAY, num, share.FREQUENCY_TYPE_DAY, 1)
    except YahooFinanceError as e:
        print(e.message)
    
    if (symbol_data == None):
        continue

    if (len(symbol_data['timestamp']) == 2):
        if (datetime.fromtimestamp(symbol_data['timestamp'][0] / 1000).strftime("%Y-%m-%d") != \
        datetime.fromtimestamp(symbol_data['timestamp'][1] / 1000).strftime("%Y-%m-%d")):
            period = num
            break
print(period)


# 本番全銘柄取得処理実行
column=['code', 'datetime', 'open', 'high', 'low', 'close', 'volume']
formmer_df = pd.DataFrame(columns=column)
latter_df = pd.DataFrame(columns=column)

for code in codes:
    time.sleep(2)
    my_share = share.Share(code + ".T")
    symbol_data = None

    try:
        symbol_data = my_share.get_historical(share.PERIOD_TYPE_DAY, period, share.FREQUENCY_TYPE_DAY, 1)
    except YahooFinanceError as e:
        print(e.message)
    
    # symbol_dataがなかったor長さが2より小さい時点でその銘柄は抜け落ちる
    if (symbol_data == None):
        continue
    if (len(symbol_data['timestamp']) < 2):
        continue
    print(code)

    # 前々日のdataframeに追加
    formmer_add_df = {'code' : code, 'datetime' : datetime.fromtimestamp(symbol_data['timestamp'][0] / 1000),\
        'open' : symbol_data['open'][0], 'high' : symbol_data['high'][0], 'low' : symbol_data['low'][0],\
        'close' : symbol_data['close'][0], 'volume' : symbol_data['volume'][0]}
    formmer_df = formmer_df.append(formmer_add_df, ignore_index=True)

    # 前日のdataframeに追加
    latter_add_df = {'code' : code, 'datetime' : datetime.fromtimestamp(symbol_data['timestamp'][1] / 1000),\
        'open' : symbol_data['open'][1], 'high' : symbol_data['high'][1], 'low' : symbol_data['low'][1],\
        'close' : symbol_data['close'][1], 'volume' : symbol_data['volume'][1]}
    latter_df = latter_df.append(latter_add_df, ignore_index=True)

    print('ok')

# フォルダ作成
new_dir_path = 'C:/work/stock/result/' + datetime.now().strftime('%Y%m%d')
os.mkdir(new_dir_path)

# csv形式で保存 
formmer_df.to_csv('C:/work/stock/result/' + datetime.now().strftime('%Y%m%d') + '/' + datetime.now().strftime('%Y%m%d') + '2dago' + '.csv', index=False)
latter_df.to_csv('C:/work/stock/result/' + datetime.now().strftime('%Y%m%d') + '/' + datetime.now().strftime('%Y%m%d') + '1dago' + '.csv', index=False)
