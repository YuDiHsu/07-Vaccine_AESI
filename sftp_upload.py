import pysftp
import os
import time
import pickle
import datetime
from sqlalchemy import create_engine
import pandas as pd


def produce_idno_csv():
    engine = create_engine('postgresql://postgres:1qaz@WSX@192.168.171.108:5432/NHI_DATA')
    con = engine.connect()
    # query = f"SELECT * FROM selected_nhi_vaccine_aesi_all"

    df = pd.DataFrame()
    # query_all = '''SELECT
    # "idno" as "身分證號",
    # min("serial_number") as "流水號",
    # min("b_date") as "出生日期"
    # FROM selected_nhi_vaccine_aesi_all
    # group by "idno"'''
    #
    # query_pregnancy = '''SELECT
    # "idno" as "身分證號",
    # min("serial_number") as "流水號",
    # min("b_date") as "出生日期"
    # FROM selected_nhi_vaccine_aesi_pregnancy
    # group by "idno"'''
    query_all = '''SELECT 
    "idno" as "身分證號",
    max("serial_number") as "流水號",
    max("b_date") as "出生日期" 

    FROM selected_nhi_vaccine_aesi_all
    where 
    "serial_number" >
    (
    select 
    max(serial_number) as "serial_number" 
    from selected_nhi_vaccine_aesi_all
    where "sys_time" < current_date	
    )
    group by "idno"'''

    query_pregnancy = '''SELECT 
    "idno" as "身分證號",
    max("serial_number") as "流水號",
    max("b_date") as "出生日期" 

    FROM selected_nhi_vaccine_aesi_pregnancy
    where 
    "serial_number" >
    (
    select 
    max(serial_number) as "serial_number" 
    from selected_nhi_vaccine_aesi_pregnancy
    where "sys_time" < current_date	
    )
    group by "idno"'''

    for query in [query_all, query_pregnancy]:
        temp_df = pd.read_sql(sql=query, con=engine)
        df = df.append(temp_df)

    t_date = datetime.datetime.now().date().strftime('%Y%m%d')
    df = df.drop_duplicates(subset=['身分證號']).reset_index(drop=True)
    df = df[['流水號', '身分證號', '出生日期']]
    if not df.empty:
        df.to_csv(os.path.join('.', 'sftp_upload_folder', f'ACT_NHI_{t_date}.csv'), index=False)
        return True
    else:
        return False


def sftp_upload():
    run_times = 0
    while run_times < 3:
        try:
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            log_path = os.path.abspath(os.path.join('.', 'sftp_upload_log.txt'))
            sftp_upload_path = os.path.join('/', 'give', 'ACTNHI')
            with pysftp.Connection(host='192.168.171.160', username='cdc_liuyl', password='rQza1fq3895c', cnopts=cnopts, port=22, log=log_path) as sftp:
                sftp.cwd(sftp_upload_path)
                dir_list = sorted(os.listdir(os.path.abspath(os.path.join('.', 'sftp_upload_folder'))), reverse=True)
                if '.ipynb_checkpoints' in dir_list:
                    dir_list.remove('.ipynb_checkpoints')
                today = datetime.datetime.today().strftime('%Y%m%d')
                file_date = dir_list[0].split('.csv')[0].split('ACT_NHI_')[1]
                if today == file_date:
                    sftp.put(os.path.abspath(os.path.abspath(os.path.join('.', 'sftp_upload_folder', dir_list[0]))), os.path.abspath(os.path.join(sftp_upload_path, dir_list[0])))
                else:
                    print('There is no new idno csv today. Or the date of the csv is not today.')
                    print('So there is no csv uploaded to sftp.')
                    break
            print(f"{dir_list[0]} successfully upload to sftp")
            break
        except Exception as e:
            time.sleep(5)
            run_times += 1
            if run_times >= 3:
                print(e)


if __name__ == "__main__":
    print("=" * 50 + 'Start to run sftp_upload.py' + '=' * 50)
    csv_status = produce_idno_csv()
    if csv_status:
        print('idno csv is produced successfully')
        sftp_upload()
        print('=' * 50)
        print("=" * 50 + ' sftp_upload.py run success ' + '=' * 50)
    else:
        print('There are no new selected idno in postgreSQL')
