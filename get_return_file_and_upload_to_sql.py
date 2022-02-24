import pysftp
import os
import time
import pickle
import datetime
from sqlalchemy import create_engine
import pandas as pd


def sftp_get_return():
    print('run_sftp')
    sftp_raw_data_path = os.path.join('/', 'take', 'ACTNHI')
    run_times = 0
    status = ''
    while run_times < 3:
        try:
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            with pysftp.Connection(host='IP', username='username', password='pwd', cnopts=cnopts, port='port') as sftp:
                sftp.cwd(sftp_raw_data_path)
                if sftp.listdir():
                    for filename in sftp.listdir():
                        sftp.get(os.path.join(sftp_raw_data_path, filename), os.path.join('.', 'sftp_download_folder', 'return_vaccine_list', filename))
                        print(f"{filename} is successfully download")
                else:
                    print('There is no new returned vaccination file in sftp.')
            status = True
            break

        except Exception as es:
            time.sleep(5)
            run_times += 1
            if run_times >= 3:
                print(es)
                status = False
                break
    return status


def sql_upload(filename):
    df = pd.read_csv(os.path.join('.', 'sftp_download_folder', 'vaccination_list', filename))
    engine = create_engine('postgresql://postgres:1qaz@name@IP:PORT/NHI_DATA')
    con = engine.connect()
    sql_table_name = ''
    try:
        if not df.empty:
            df.to_sql(name=sql_table_name, con=con, if_exists='replace', index=False)
            print(f"Data uploading. Length is {df.shape[0]}\n" + '-' * 100)
        else:
            print('There is no data for uploading. Please check the file.')
    except Exception as e:
        print(e)


def merge_func():
    send_file = sorted(os.listdir(os.path.join('.', 'sftp_upload_folder')), reverse=True)[0]
    df_send = pd.read_csv(os.path.join('.', 'sftp_upload_folder', send_file), index_col=False)
    get_file = sorted(os.listdir(os.path.join('.', 'sftp_download_folder', 'vaccination_list')))[0]
    df_get = pd.read_csv(os.path.join('.', 'sftp_download_folder', 'vaccination_list', get_file), index_col=False, header=None)
    # ['流水號', '廠牌_疫苗代碼', '劑次', '接種日', '批號']
    df_get = df_get.rename(columns={0: '流水號', 1: '廠牌_疫苗代碼', 2: '劑次', 3: '接種日', 4: '批號'})
    df_get = df_get.dropna(subset=['接種日']).reset_index(drop=True)
    df_merge = df_get.merge(df_send[['流水號', '身分證號']])

    df = pd.read_pickle(os.path.join('.', 'others/event_selection_20210505.pickle'))
    df_merge_2 = df.merge(df_merge)
    print(df_merge_2.columns)
    print(df_merge_2.loc[(df_merge_2['syndrome'] == 'Anaphylaxis')].loc[:, ('身分證號', '就醫日期', '接種日')])
    df_merge_2.loc[(df_merge_2['syndrome'] == 'Anaphylaxis')].to_excel(os.path.join('.', 'others/Anaphylaxis.xlsx'))

    
def check_file_date(file_path):
    file_dir = sorted(os.listdir(file_path), reverse=True)
    today = datetime.datetime.today().strftime('%Y%m%d')
    file_date = file_dir[0].split('_')[2]

    if today == file_date:
        return True, file_dir[0]
    else:
        return False, file_dir[0]


if __name__ == "__main__":
    print("=" * 50 + 'get_return_file_and_upload_to_sql.py' + '=' * 50)
    get_return = sftp_get_return()
    if get_return:
        file_status, file_name = check_file_date(os.path.join('.', 'sftp_download_folder', 'vaccination_list'))
        if file_status:
            merge_func()
            while True:
                t_date = datetime.datetime.today().strftime('%Y%m%d')
                t_time = f'{t_date}0750'
                now = datetime.datetime.now().strftime('%Y%m%d%H%M')
                if now == t_time:
                    sql_upload(file_name)
                    break
        else:
            print('There is no new_vaccination_list in sftp_download_folder.')
    else:
        print('sftp connecting failed.')
    merge_func()
