import datetime
import pickle
from dateutil import relativedelta
import pandas as pd
import os
import time
import asyncio
from sqlalchemy import create_engine
import pysftp
import multiprocessing as mp
import re


def get_sftp(sftp_icdx_folder_path, sftp_icpreg_folder_path):
    print('run_sftp')
    sftp_raw_data_path = os.path.join('/', 'take', 'nhi_aesi')
    sftp_icdx_list = []
    sftp_icpreg_list = []

    run_times = 0
    while run_times < 3:
        try:
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            with pysftp.Connection(host='IP', username='username', password='pwd', cnopts=cnopts, port='port') as sftp:
                sftp.cwd(sftp_raw_data_path)
                for filename in sftp.listdir():
                    if 'ICDX_' in filename:
                        sftp.get(os.path.join(sftp_raw_data_path, filename), os.path.join(sftp_icdx_folder_path, filename))
                        print(filename)
                        sftp_icdx_list.append(filename)
                    if 'ICPREG_' in filename:
                        sftp.get(os.path.join(sftp_raw_data_path, filename), os.path.join(sftp_icpreg_folder_path, filename))
                        print(filename)
                        sftp_icpreg_list.append(filename)
            break

        except Exception as es:
            time.sleep(5)
            run_times += 1
            if run_times >= 3:
                print(es)

    sftp_filename_list = [sorted(sftp_icdx_list, reverse=True), sorted(sftp_icpreg_list, reverse=True)]
    try:
        log_record_and_check(sftp_filename_list)
    except Exception as ee:
        print(ee)

    if sftp_filename_list[0] and sftp_filename_list[1]:
        with open(os.path.join('.', 'others/sftp_download.pickle'), 'wb') as pl:
            pickle.dump(sftp_filename_list, pl)
        print('sftp download completed')
        return sftp_filename_list

    elif sftp_filename_list[0] and not sftp_filename_list[1]:
        raise SyntaxError('Without new ICPREG sftp data')
    elif not sftp_filename_list[1] and sftp_filename_list[0]:
        raise SyntaxError('Without new ICDX sftp data')
    else:
        raise SyntaxError('Without new ICDX and ICPREG sftp data')


def log_record_and_check(all_download_list):
    log_filename_list = []

    with open(os.path.join('.', 'nhi_data_download_log.txt'), 'w+') as log:
        for download_list in all_download_list:
            if download_list:
                for download_file in download_list:
                    log.write(download_file + f" DLD:{str(datetime.datetime.now().date())}" + '\n')
                    log_filename_list.append(download_file)

        log.seek(0)
        log.close()
    return log_filename_list


def age_calculation(b_date, exam_date):
    try:
        diff = relativedelta.relativedelta(b_date, exam_date)
        if diff:
            return f'{abs(diff.years)}, {abs(diff.months)}, {abs(diff.days)}', int(abs(diff.years))
        else:
            if b_date == exam_date:
                return f'0, 0, 0', 0
            else:
                raise SyntaxError('Something wrong in age calculation ')
    except:
        return ''

    # return f'{abs(diff.years)}, {abs(diff.months)}, {abs(diff.days)}', int(abs(diff.years))


def gender_classify(x):
    male_codes = ['1', '8', 'A', 'C']
    female_codes = ['2', '9', 'B', 'D']
    if str.upper(x[1]) in male_codes:
        return 'male'
    elif str.upper(x[1]) in female_codes:
        return 'female'
    else:
        return ''


def age_group(x):
    age_value = int(x.split(', ')[0])
    a_g = ''
    if age_value <= 17:
        a_g = '0-17'
    elif 18 <= age_value <= 49:
        a_g = '18-49'
    elif 50 <= age_value <= 64:
        a_g = '50-64'
    elif age_value >= 65:
        a_g = '65+'
    return a_g


def select_func(x, v):
    flag = False
    # # determine the length of character to search adr_icd_code
    for n in list(set([len(l) for l in v['adr_icd_code']])):
        if x[0:n] in v['adr_icd_code']:
            flag = True
            break
    # # determine the length of character to search exception
    for m in list(set([len(j) for j in v['exception']])):
        if m:
            if x[0:m] in v['exception']:
                flag = False
                break
        else:
            break
    return flag


def aesi_diag_code(path):
    df = pd.read_csv(path)
    df = df.fillna('')
    columns = list(df.columns)
    aesi_dict = {}
    for ae in df['aesi_diag_name'].values.tolist():
        if ae not in aesi_dict:
            aesi_dict[ae] = {}
            for col in columns[1:]:
                if col not in aesi_dict[ae]:
                    aesi_dict[ae][col] = ''

    for k, v in aesi_dict.items():
        for col in columns[1:]:
            for value in df[col].loc[(df['aesi_diag_name'] == k)]:
                if type(value) == str:
                    v[col] = str(value).replace('.', '').split(', ')
                else:
                    v[col] = value
    return aesi_dict


def date_convert(x, idno):
    if x:
        try:
            n_x = datetime.datetime.strptime(str(x).replace('-', ''), '%Y%m%d').date()
        except:
            print(f"Error case ID: {idno}, wrong date is {x}")
            x = int(x) - 1
            n_x = datetime.datetime.strptime(str(x).replace('-', ''), '%Y%m%d').date()
        return n_x
    else:
        return x


def df_processing_fuc(df_):
    # # convert str to datetime format
    for date in ['出生日期', '就醫日期']:
        try:
            # df_.loc[:, date] = df_.loc[:, date].apply(lambda x: date_convert(x))
            df_.loc[:, date] = df_.apply(lambda x: date_convert(x[date], x['身分證號']), axis=1)
        except EOFError as e:
            print('EOFError', e)
        except Exception as Ex:
            print('Exception', Ex)

    try:
        # # gender classification
        df_.loc[:, 'gender'] = df_.loc[:, '身分證號'].apply(lambda x: gender_classify(x))
        # # determine age (year, month, days)
        df_.loc[:, 'age_ymd'] = df_.apply(lambda x: age_calculation(x['出生日期'], x['就醫日期'])[0], axis=1)
        # # determine age group(0-17, 18-49, 49-64, 65+)
        df_.loc[:, 'age_group'] = df_.loc[:, 'age_ymd'].apply(lambda x: age_group(x))

    except EOFError as e:
        print(e)
    except Exception as Ex:
        print(Ex)

    aesi_diag_dict = aesi_diag_code(os.path.join('.', 'covid_death_icd_code.csv'))
    df_select = pd.DataFrame()
    try:
        for col in ['主診斷碼', '次診斷碼1', '次診斷碼2', '次診斷碼3', '次診斷碼4', '次診斷碼5']:
            for k, v in aesi_diag_dict.items():
                temp_df = df_.loc[df_[col].apply(lambda x: select_func(x, v))]
                temp_df['syndrome'] = k
                temp_df['clean_period_days'] = v['clean_period_days']
                df_select = df_select.append(temp_df)
    except EOFError as e:
        print(e)
    except Exception as Ex:
        print(Ex)
    df_select = df_select.reset_index(drop=True)

    return df_select


def data_divide_func(sftp_filename_list, division_name):
    num_processes = int(mp.cpu_count())

    df_list = []
    for f_n in sftp_filename_list:
        print(f'{f_n} is loading')
        df_ = pd.read_csv(os.path.join('.', 'sftp_download_folder', f'{division_name}_sftp', f_n), header=[0], low_memory=False)
        df_ = df_.fillna('')
        df_ = df_.astype(str)
        if division_name == 'ICPREG':
            df_.loc[:, '醫療院所代碼'] = ''
            df_.loc[:, '就醫類別'] = ''
            df_ = df_[['身分證號', '出生日期', '醫療院所代碼', '就醫日期', '就醫類別', '主診斷碼', '次診斷碼1', '次診斷碼2', '次診斷碼3', '次診斷碼4', '次診斷碼5']]
        print('Start to splice data to parts and process')
        df_final = pd.DataFrame()
        n = 0
        for i in range(0, len(df_), 1000000):
            n += 1000000
            if n < len(df_):
                print(f"Data processing from {i}-{n}")
                select_1s = time.time()
                p_df = df_[i:n].reset_index(drop=True)
                chunk_size = int(p_df.shape[0] / num_processes)
                chunks = [p_df.loc[p_df.index[i:i + chunk_size]] for i in range(0, p_df.shape[0], chunk_size)]
                pool = mp.Pool(processes=num_processes)
                df_partition_list = pool.map(df_processing_fuc, chunks)
                df_partition = pd.concat(df_partition_list).reset_index(drop=True)
                select_1e = time.time()
                print(f"Processing time: {select_1e - select_1s}\nData length: {len(df_partition)}\n" + '-' * 100)

            else:
                print(f"Data processing from {i}-{len(df_)}")
                select_2s = time.time()
                p_df = df_[i:len(df_)].reset_index(drop=True)
                chunk_size = int(p_df.shape[0] / num_processes)
                chunks = [p_df.loc[p_df.index[i:i + chunk_size]] for i in range(0, p_df.shape[0], chunk_size)]
                pool = mp.Pool(processes=num_processes)
                # df_partition = df_processing_fuc(p_df)
                df_partition_list = pool.map(df_processing_fuc, chunks)
                df_partition = pd.concat(df_partition_list).reset_index(drop=True)
                select_2e = time.time()
                print(f"Processing time: {select_2e - select_2s}\nData length: {len(df_partition)}\n" + '-' * 100)

            df_final = df_final.append(df_partition)
        df_final = df_final.reset_index(drop=True)
        df_list.append(df_final)

    if df_list:
        if len(df_list) <= 1:
            with open(os.path.join('.', 'daily_pickle_store_folder', f'{division_name}_pickle', f"df_final_{division_name}_{datetime.datetime.today().strftime('%Y%m%d')}.pickle"), 'wb') as pl:
                pickle.dump(df_list[0], pl)
            # with open(os.path.join('.', 'debug.pickle'), 'wb') as pl:
            #     pickle.dump(df_list[0], pl)

            print(f"Final data of {division_name} merging completed\nData length: {len(df_list[0])}")
        else:
            df_concat = pd.concat(df_list, ignore_index=True)
            with open(os.path.join('.', 'daily_pickle_store_folder', f'{division_name}_pickle', f"df_final_{division_name}_{datetime.datetime.today().strftime('%Y%m%d')}.pickle"), 'wb') as pl:
                pickle.dump(df_concat, pl)

            print(f"Final data of {division_name} merging completed\nData length: {len(df_concat)}")
    else:
        print('Empty df in final df.')


def main():
    # # # Terminate pandas loc. Warning
    pd.options.mode.chained_assignment = None
    
    # # Access sftp and download file ------------------------------------------------------------------------------
    sftp_icdx_folder_path = os.path.join(os.path.abspath('.'), 'sftp_download_folder', 'ICDX_sftp')
    sftp_icpreg_folder_path = os.path.join(os.path.abspath('.'), 'sftp_download_folder', 'ICPREG_sftp')
    sftp_filename_list = get_sftp(sftp_icdx_folder_path, sftp_icpreg_folder_path)
    
    sftp_icdx_filename_list = sftp_filename_list[0]
    sftp_icpreg_filename_list = sftp_filename_list[1]

#     test_icdx_filename_list = ['ICDX_20210501.txt', 'ICDX_20210502.txt', 'ICDX_20210503.txt',
#                                'ICDX_20210504.txt', 'ICDX_20210505.txt', 'ICDX_20210506.txt', 'ICDX_20210507.txt', 'ICDX_20210508.txt', 'ICDX_20210509.txt',
#                                'ICDX_20210510.txt', 'ICDX_20210511.txt', 'ICDX_20210512.txt', 'ICDX_20210513.txt', 'ICDX_20210514.txt', 'ICDX_20210515.txt',
#                                'ICDX_20210516.txt', 'ICDX_20210517.txt', 'ICDX_20210518.txt', 'ICDX_20210519.txt'
#                                ]
#     test_icpreg_filename_list = ['ICPREG_20210501.txt', 'ICPREG_20210502.txt', 'ICPREG_20210503.txt', 'ICPREG_20210504.txt',
#                                  'ICPREG_20210505.txt', 'ICPREG_20210506.txt', 'ICPREG_20210507.txt', 'ICPREG_20210508.txt', 'ICPREG_20210509.txt',
#                                  'ICPREG_20210510.txt', 'ICPREG_20210511.txt', 'ICPREG_20210512.txt', 'ICPREG_20210513.txt', 'ICPREG_20210514.txt',
#                                  'ICPREG_20210515.txt', 'ICPREG_20210516.txt', 'ICPREG_20210517.txt', 'ICPREG_20210518.txt', 'ICPREG_20210519.txt'
#                                  ]

    data_divide_func(sftp_icdx_filename_list, 'ICDX')
    data_divide_func(sftp_icpreg_filename_list, 'ICPREG')


if __name__ == "__main__":
    mp.freeze_support()
    # Error case ID: T102399801, wrong date is 19470229
    print("=" * 50 + ' Start to run main_multiprocess.py ' + '=' * 50)
    main()
    print("=" * 50)
    print("=" * 50 + ' main_multiprocess.py run success ' + '=' * 50)
