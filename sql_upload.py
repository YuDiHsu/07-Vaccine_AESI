from sqlalchemy import create_engine
import pandas as pd
import os
import datetime


def check_upload_date(pickle_read_path, division_name):
    file_dir = sorted(os.listdir(pickle_read_path), reverse=True)
    today = datetime.datetime.today().strftime('%Y%m%d')
    file_date = file_dir[0].split('.pickle')[0].split(f'df_final_{division_name}_')[1]

    if today == file_date:
        return True, file_dir[0]
    else:
        return False, file_dir[0]


def upload_fuc(pickle_read_path, division_name, sql_table_name):
    check_status, file_path = check_upload_date(pickle_read_path, division_name)

    if check_status:
        df = pd.read_pickle(os.path.join(pickle_read_path, file_path))
        df = df.rename(columns={'身分證號': 'idno', '出生日期': 'b_date', '醫療院所代碼': 'hos_code', '就醫日期': 'exam_date',
                                '就醫類別': 'exam_class', '主診斷碼': 'main_diag_code', '次診斷碼1': 'sub_diag_code_1',
                                '次診斷碼2': 'sub_diag_code_2', '次診斷碼3': 'sub_diag_code_3', '次診斷碼4': 'sub_diag_code_4',
                                '次診斷碼5': 'sub_diag_code_5'})

        # # SQL loading-----------------------------------------------------------------------------------------------
        engine = create_engine('postgresql://postgres:1qaz@WSX@192.168.171.108:5432/NHI_DATA')
        con = engine.connect()
        n = 0
        for i in range(0, len(df), 100000):
            n += 100000
            if n < len(df):
                df[i:n].to_sql(name=sql_table_name, con=con, if_exists='append', index=False)
                print(f"data uploading from {i}-{n}\n" + '-' * 100)

            else:
                df[i:len(df)].to_sql(name=sql_table_name, con=con, if_exists='append', index=False)
                print(f"data uploading from {i}-{len(df)}\n" + '-' * 100)
        return True
    else:
        print(f'Date of {division_name}_pickle is not today.')
        return False


def main():
    # # Upload ICDX data
    icdx_upload = upload_fuc(os.path.join('.', 'daily_pickle_store_folder', 'ICDX_pickle'), 'ICDX', 'selected_nhi_vaccine_aesi_all')
    # # Upload ICPREG data
    icpreg_upload = upload_fuc(os.path.join('.', 'daily_pickle_store_folder', 'ICPREG_pickle'), 'ICPREG', 'selected_nhi_vaccine_aesi_pregnancy')
    if icdx_upload and icpreg_upload:
        print('Pickles are successfully uploaded to postgreSQL')
    else:
        raise SyntaxError('Data of pickle uploading failed')


if __name__ == "__main__":
    print("=" * 50 + ' Start to run sql_upload.py ' + '=' * 50)
    main()
    print('=' * 50)
    print("=" * 50 + ' sql_upload.py run success ' + '=' * 50)
