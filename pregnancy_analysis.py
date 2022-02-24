import os
import time
import pandas as pd
import datetime
import pickle


def date_convert(x):
    if x:
        try:
            n_x = datetime.datetime.strptime(str(x).replace('-', ''), '%Y%m%d').date()
        except:
            x = int(x) - 1
            n_x = datetime.datetime.strptime(str(x).replace('-', ''), '%Y%m%d').date()
        return n_x
    else:
        return x


def make_idno_csv():
    df = pd.DataFrame()
    for i in os.listdir(os.path.join('.', 'sftp_download_folder', 'ICDX_sftp')):
        if i != 'ICDX_20210410.txt':
            temp_df = pd.read_csv(os.path.join('.', 'sftp_download_folder', 'ICDX_sftp', i), header=[0], low_memory=False)
            # temp_df = temp_df.loc[:, ('身分證號', '出生日期')].drop_duplicates()

            df = df.append(temp_df)

    for j in os.listdir(os.path.join('.', 'sftp_download_folder', 'ICPREG_sftp')):
        if j != 'ICPREG_20210410.txt':
            temp_df = pd.read_csv(os.path.join('.', 'sftp_download_folder', 'ICPREG_sftp', j), header=[0], low_memory=False)
            temp_df = temp_df.loc[:, ('身分證號', '出生日期')].drop_duplicates()
            df = df.append(temp_df)

    df = df.drop_duplicates(subset=['身分證號', '出生日期']).reset_index(drop=True)
    df.loc[:, '流水號'] = df.index + 1
    df = df[['流水號', '身分證號', '出生日期']]

    df.loc[:, '出生日期'] = df.apply(lambda x: date_convert(x['出生日期']), axis=1)
    df.to_csv(os.path.join('.', 'others', f"ACT_NHI_{datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d')}.csv"), index=False)
    # with open(os.path.join('.', 'others/ACT_NHI_20210520.pickle'), 'wb') as pl:
    #     pickle.dump(df, pl)

    # df.to_csv(os.path.join('.', 'others', f"ACT_NHI_20210520.csv"), index=False)
    # df.to_csv(os.path.join('.', 'others', f"df_preg_20210520.csv"), index=False)


if __name__ == "__main__":
    # make_idno_csv()
    # df = pd.read_pickle(os.path.join('.', 'others', 'event_selection_20210519.pickle'))

    df_1 = pd.DataFrame()
    for j in os.listdir(os.path.join('.', 'sftp_download_folder', 'ICPREG_sftp')):
        temp_df = pd.read_csv(os.path.join('.', 'sftp_download_folder', 'ICPREG_sftp', j), header=[0], low_memory=False)
        df_1 = df_1.append(temp_df)

    df_1 = df_1.fillna('')
    df_1_missed_abortion = pd.DataFrame()
    for c in ['主診斷碼', '次診斷碼1', '次診斷碼2', '次診斷碼3', '次診斷碼4', '次診斷碼5']:
        temp = df_1.loc[df_1[c].apply(lambda x: True if x[0:4] == 'O021' else False) == True]
        df_1_missed_abortion = df_1_missed_abortion.append(temp)
    df_1_missed_abortion = df_1_missed_abortion.drop_duplicates()
    df_1_missed_abortion = df_1_missed_abortion.reset_index(drop=True)

    df_2 = pd.DataFrame()
    total_num = 0
    for j in os.listdir(os.path.join('.', 'sftp_download_folder', 'ICDX_sftp')):

        temp_df = pd.read_csv(os.path.join('.', 'sftp_download_folder', 'ICDX_sftp', j), header=[0], low_memory=False)
        total = temp_df.shape[0]
        end = 0
        for n in range(0, total, 1000000):
            end += 1000000
            if end < total:
                tmp = temp_df[n:end]
                total_num += tmp.shape[0]
                tmp = tmp.fillna('')
                df_2_missed_abortion = pd.DataFrame()
                for c in ['主診斷碼', '次診斷碼1', '次診斷碼2', '次診斷碼3', '次診斷碼4', '次診斷碼5']:
                    tt = tmp.loc[tmp[c].apply(lambda x: True if x[0:4] == 'O021' else False) == True]
                    for cc in ['主診斷碼', '次診斷碼1', '次診斷碼2', '次診斷碼3', '次診斷碼4', '次診斷碼5']:
                        ttt = tt.loc[tt[cc].apply(lambda x: True if x[0:3] in ['Z32', 'Z33', 'Z34'] else False) == True]
                        df_2_missed_abortion = df_2_missed_abortion.append(ttt)
                df_2_missed_abortion = df_2_missed_abortion.drop_duplicates()
                df_2 = df_2.append(df_2_missed_abortion)

            if end >= total:
                tmp = temp_df[n:total]
                total_num += tmp.shape[0]
                tmp = tmp.fillna('')
                df_2_missed_abortion = pd.DataFrame()
                for c in ['主診斷碼', '次診斷碼1', '次診斷碼2', '次診斷碼3', '次診斷碼4', '次診斷碼5']:
                    tt = tmp.loc[tmp[c].apply(lambda x: True if x[0:4] == 'O021' else False) == True]
                    for cc in ['主診斷碼', '次診斷碼1', '次診斷碼2', '次診斷碼3', '次診斷碼4', '次診斷碼5']:
                        ttt = tt.loc[tt[cc].apply(lambda x: True if x[0:3] in ['Z32', 'Z33', 'Z34'] else False) == True]
                        df_2_missed_abortion = df_2_missed_abortion.append(ttt)
                df_2_missed_abortion = df_2_missed_abortion.drop_duplicates()
                df_2 = df_2.append(df_2_missed_abortion)

    df_2 = df_2.drop_duplicates()
    df_2 = df_2.reset_index(drop=True)

    df_3 = df_2[list(df_1.columns)]
    df_4 = pd.concat([df_1, df_3]).drop_duplicates().reset_index(drop=True)

    # for d in ['出生日期', '就醫日期']:
    #     df.loc[:, d] = df.apply(lambda x: date_convert(x[d]), axis=1)
    # # df = df.loc[(df['syndrome'] == 'Spontaneous abortion or Stillbirth') | (df['syndrome'] == 'Preterm birth') | (df['syndrome'] == 'Full-term birth')]
    #
    # idno = pd.read_csv(os.path.join('.', 'others', 'RACT_NHI_20210520_20210519150232.csv'), header=None)
    #
    # idno = idno.rename(columns={0: '流水號', 1: '廠牌', 2: '劑次', 3: '接種日', 4: '批號'})
    # # print(idno)
    #
    # idno = idno.dropna(subset=['接種日']).reset_index(drop=True)
    #
    # df_ = pd.read_csv(os.path.join('.', 'others', 'ACT_NHI_20210520.csv'))
    #
    # df_merge_1 = df_.copy().merge(idno, how='left', on='流水號')
    #
    # df_vac = df_merge_1.dropna(subset=['接種日']).reset_index(drop=True).drop(['流水號'], axis=1)
    # for dd in ['出生日期', '接種日']:
    #     df_vac.loc[:, dd] = df_vac.apply(lambda x: date_convert(x[dd]), axis=1)
    # df_s = df.merge(df_vac, how='left', on=['身分證號', '出生日期'])
    # # print(idno)
    # df_s = df_s.dropna(subset=['接種日']).reset_index(drop=True)
    #
    # df_ss = df_s.loc[df_s.apply(lambda x: True if (x['接種日'] - x['就醫日期']).days >= 180 else False, axis=1) != False].reset_index(drop=True)
    # idno_preg_vac = list(set(df_ss.loc[:, '身分證號'].values.tolist()))
    #
    # # # 半年內兩次產檢
    # preg_vac_b_180 = 0
    # for i in idno_preg_vac:
    #     if df_ss.loc[df_ss['身分證號'] == i].shape[0] >= 2:
    #         preg_vac_b_180 += 1
    #
    # # # 接種疫苗後有一次產檢
    #
    # df_sa = df_s.loc[df_s.apply(lambda x: True if x['接種日'] < x['就醫日期'] else False, axis=1) != False].reset_index(drop=True)
    # one_time_exam = 0
    # for i in idno_preg_vac:
    #     if not df_sa.loc[df_sa['身分證號'] == i].empty:
    #         one_time_exam += 1
    #
    # print(one_time_exam)

    # # I200374028 注射日期：20210326, 劑次：1, 廠牌：AZ, 批號：509
    # # O021, 20210513, 20210505流產
