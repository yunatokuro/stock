import pandas as pd
import math
from datetime import date, datetime

# CSV読み込み
day2_pd = pd.read_csv('C:/work/stock/result/' + '20210928' + '/' + '20210928' + '2dago' + '.csv', encoding='shift_jis')
day1_pd = pd.read_csv('C:/work/stock/result/' + '20210928' + '/' + '20210928' + '1dago' + '.csv', encoding='shift_jis')

# 2日分のデータ結合
df=pd.merge(day2_pd, day1_pd, on=['code'], suffixes=['_2d', '_1d'], how='outer')

# 空のdataframe用意
column=['code', 'high_diff', 'high_diff_ratio', 'low_diff', 'low_diff_ratio']
outside_day_hidden_positive_df = pd.DataFrame(columns=column)
inside_day_hidden_positive_df = pd.DataFrame(columns=column)
entity_outside_day_hidden_positive_df = pd.DataFrame(columns=column)
entity_inside_day_hidden_positive_df = pd.DataFrame(columns=column)

# 楽天証券import用
column=['code', 'page']
rakutenn = pd.DataFrame(columns=column)

# 包み足（陰-陽）
for row in df.itertuples():
    if (row.open_2d < 200):
        continue
    if (row.volume_1d < 20000):
        continue
    if (row.volume_1d < row.volume_2d):
        continue
    if (row.open_2d >= row.close_2d and row.open_1d <= row.close_1d):
        if (row.high_2d <= row.high_1d and row.low_1d <= row.low_2d):

            # 高値と低値の差算出
            high_diff = math.floor(row.high_1d - row.high_2d)
            high_diff_ratio = high_diff / row.high_2d * 100
            low_diff = math.floor(row.low_2d - row.low_1d)
            low_diff_ratio = low_diff / row.low_2d * 100

            if (high_diff == 0 and low_diff == 0):
                continue

            # データ追加
            outside_add_df = {'code' : math.floor(row.code), 'high_diff' : high_diff,\
                'high_diff_ratio' : high_diff_ratio, 'low_diff' : low_diff, 'low_diff_ratio' : low_diff_ratio}
            outside_day_hidden_positive_df = outside_day_hidden_positive_df.append(outside_add_df, ignore_index=True)

            if (row.open_2d <= row.close_1d and row.close_2d >= row.open_1d):
                # データ追加
                entity_outside_add_df = {'code' : math.floor(row.code), 'high_diff' : high_diff,\
                    'high_diff_ratio' : high_diff_ratio, 'low_diff' : low_diff, 'low_diff_ratio' : low_diff_ratio}
                entity_outside_day_hidden_positive_df = entity_outside_day_hidden_positive_df.append(entity_outside_add_df, ignore_index=True)
                rakutenn_add_df = {'code' : math.floor(row.code), 'page' : datetime.now().strftime('%Y%m%d')}
                rakutenn = rakutenn.append(rakutenn_add_df, ignore_index=True)
        
        elif (row.high_1d <= row.high_2d and row.low_2d <= row.low_1d):

            # 高値と低値の差算出
            high_diff = math.floor(row.high_2d - row.high_1d)
            high_diff_ratio = high_diff / row.high_2d * 100
            low_diff = math.floor(row.low_1d - row.low_2d)
            low_diff_ratio = low_diff / row.low_2d * 100

            if (high_diff == 0 and low_diff == 0):
                continue
            
            # データ追加
            inside_add_df = {'code' : math.floor(row.code), 'high_diff' : high_diff,\
                'high_diff_ratio' : high_diff_ratio, 'low_diff' : low_diff, 'low_diff_ratio' : low_diff_ratio}
            inside_day_hidden_positive_df = inside_day_hidden_positive_df.append(inside_add_df, ignore_index=True)

            if (row.open_2d <= row.close_1d and row.open_2d <= row.close_1d):
                # データ追加
                entity_inside_add_df = {'code' : math.floor(row.code), 'high_diff' : high_diff,\
                    'high_diff_ratio' : high_diff_ratio, 'low_diff' : low_diff, 'low_diff_ratio' : low_diff_ratio}
                entity_inside_day_hidden_positive_df = entity_inside_day_hidden_positive_df.append(entity_inside_add_df, ignore_index=True)
    else:
        continue

# csv形式で保存 
outside_day_hidden_positive_df.to_csv('C:/work/stock/result/' + datetime.now().strftime('%Y%m%d') + '/' + 'out' + datetime.now().strftime('%Y%m%d') + '.csv', index=False)
entity_outside_day_hidden_positive_df.to_csv('C:/work/stock/result/' + datetime.now().strftime('%Y%m%d') + '/' + 'entityout' + datetime.now().strftime('%Y%m%d') + '.csv', index=False)
inside_day_hidden_positive_df.to_csv('C:/work/stock/result/' + datetime.now().strftime('%Y%m%d') + '/' + 'in' + datetime.now().strftime('%Y%m%d') + '.csv', index=False)
entity_inside_day_hidden_positive_df.to_csv('C:/work/stock/result/' + datetime.now().strftime('%Y%m%d') + '/' + 'entityin' + datetime.now().strftime('%Y%m%d') + '.csv', index=False)
rakutenn.to_csv('C:/work/stock/result/' + datetime.now().strftime('%Y%m%d') + '/' + 'rakutenn' + datetime.now().strftime('%Y%m%d') + '.csv', index=False)