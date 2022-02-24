import pandas as pd
import os
import glob
import cx_Oracle
import datetime
import time
import xlsxwriter
import pysftp
import requests
import pygsheets
from smtp import SMTP
from subprocess import Popen


def get_raw_data(file_name, code):
    dsn = cx_Oracle.makedsn('192.168.170.52', '1561', service_name='DW')

    conn = cx_Oracle.connect(
        user='sas',
        password='ueCr5brAD6u4rAs62t9a',
        dsn=dsn,
        encoding='UTF8',
        nencoding='UTF8'
    )

    c = conn.cursor()

    c.execute(code)

    desc = c.description
    col_name_list = []
    for s in desc:
        col_name_list.append(s[0])

    data_list = c.fetchall()
    conn.close()

    temp_df = pd.DataFrame(data_list, columns=col_name_list)
    #     temp_df.to_csv(os.path.abspath(os.path.join('.', f'{file_name}.csv')), index=False)
    data_list.append(temp_df)

    return temp_df


def sftp_upload(file_path):
    run_times = 0
    while run_times < 3:
        try:
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            sftp_upload_path = os.path.abspath(os.path.join('/', 'give', 'Confirm'))
            with pysftp.Connection(host='192.168.171.160', username='cdc_liuyl', password='rQza1fq3895c', cnopts=cnopts,
                                   port=22) as sftp:
                sftp.cwd(sftp_upload_path)
                file_name = os.path.basename(file_path)
                sftp.put(file_path, os.path.abspath(os.path.join(sftp_upload_path, file_name)))
                print(f"{file_name} upload successfully.")
                print('-' * 100)
            break
        except Exception as e:
            time.sleep(5)
            run_times += 1
            if run_times >= 3:
                print(e)


def immigration_func(x):
    if x == '0':
        x = 'Domestic'
    elif x == '1':
        x = 'Imported'
    else:
        x = 'Unknown'

    return x


def sftp_download(save_folder):
    run_times = 0
    re_path_list = []
    while run_times < 3:
        try:
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            sftp_download_path = os.path.abspath(os.path.join('/', 'take', 'Confirm'))

            with pysftp.Connection(host='192.168.171.160', username='cdc_liuyl', password='rQza1fq3895c', cnopts=cnopts,
                                   port=22) as sftp:
                sftp.cwd(sftp_download_path)
                for filename in sftp.listdir():
                    sftp.get(os.path.join(sftp_download_path, filename), os.path.join(save_folder, filename))
                    re_path = os.path.abspath(os.path.join(save_folder, filename))
                    re_path_list.append(re_path)
                    print(re_path)
                    print(f"{filename} download completely.")
                    print('-' * 100)
            break

        except Exception as es:
            time.sleep(5)
            run_times += 1
            if run_times >= 3:
                print(es)

    if re_path_list:
        return sorted(re_path_list, reverse=True)[0]
    else:
        print('There is no returned file from sftp.')
        print('-' * 100)


def age_group(x):
    a_g = ''
    # if int(x) <= 17:
    #     a_g = '0-17'
    if 18 <= int(x) <= 49:
        a_g = '18-49'
    elif 50 <= int(x) <= 64:
        a_g = '50-64'
    elif int(x) >= 65:
        a_g = '65+'
    return a_g


def interval_group(x, cons_value):
    if int(x) <= cons_value:
        return f'0-{cons_value}'
    if cons_value < int(x):
        return f'>={cons_value + 1}'


def table_data(df, interval_days, interval_col, interval_dose, syn_list, brand_list, brand_col):
    data_set = []
    # # length for xlsx merge col
    list_len_1 = 0
    # # data analysis
    for age in ['all', '18-49', '50-64', '65+']:
        if age == 'all':
            df_dd = df.copy()
        else:
            df_dd = df.copy().loc[df['age_group'] == age]

        # print(df_d.loc[df_d['Brand_1st'] == 'CoV_Moderna'])
        for brand in brand_list:
            df_d = df_dd.loc[df_dd[brand_col] == brand]
            t1_data_1 = [f'0-{interval_days}']
            t1_data_2 = [f'>={interval_days + 1}']
            t1_data_3 = ['總計']
            t1_title_2 = ''
            s_list = ''
            for syn in syn_list:
                s_list = syn_list.copy()
                t1_title_2 = [interval_dose, 'WHO_severity', f'Onset_age: {age}', f'{brand_col}: {brand}']
                s_list.insert(0, '')
                s_list.insert(len(s_list), '總計')
                temp_t1_data_1 = [df_d.loc[(df_d['WHO_classification'] == syn) & (df_d[interval_col] == f'0-{interval_days}')].shape[0]]
                t1_data_1 += temp_t1_data_1

                temp_t1_data_2 = [df_d.loc[(df_d['WHO_classification'] == syn) & (df_d[interval_col] == f'>={interval_days + 1}')].shape[0]]
                t1_data_2 += temp_t1_data_2

                temp_t1_data_3 = [df_d.loc[(df_d['WHO_classification'] == syn)].shape[0]]
                t1_data_3 += temp_t1_data_3

            t1_data_1.append(df_d.loc[(df_d['WHO_classification'] != '') & (df_d[interval_col] == f'0-{interval_days}')].shape[0])
            t1_data_2.append(df_d.loc[(df_d['WHO_classification'] != '') & (df_d[interval_col] == f'>={interval_days + 1}')].shape[0])
            t1_data_3.append(df_d.loc[(df_d['WHO_classification'] != '')].shape[0])

            data_set.append(t1_title_2)
            data_set.append(s_list)
            data_set.append(t1_data_1)
            data_set.append(t1_data_2)
            data_set.append(t1_data_3)
            blank = []
            for ll in range(len(t1_data_1)):
                blank.append('')
            data_set.append(blank)
            list_len_1 = len(blank)

    t2_title_1 = [f'Table of Onset_age by {interval_dose}']

    data_set.append(t2_title_1)

    list_len_2 = 0
    for brand in brand_list:
        t2_title_2 = ['Onset_age', interval_dose, f'{brand_col}: {brand}']
        t2_title_3 = ['', f'0-{interval_days}', f'>={interval_days + 1}', '總計']
        data_set.append(t2_title_2)
        data_set.append(t2_title_3)
        df_d = df.copy()
        df_d = df_d.loc[df_d[brand_col] == brand]
        for age in ['18-49', '50-64', '65+', '總計']:
            if age != '總計':
                t2_data_1 = [age,
                             df_d.loc[(df_d['age_group'] == age) & (df_d[interval_col] == f'0-{interval_days}')].shape[0],
                             df_d.loc[(df_d['age_group'] == age) & (df_d[interval_col] == f'>={interval_days + 1}')].shape[0],
                             df_d.loc[df_d['age_group'] == age].shape[0]]
                data_set.append(t2_data_1)
                # # length for xlsx merge col
                list_len_2 = len(t2_data_1)
            if age == '總計':
                t2_data_1 = [age,
                             df_d.loc[df_d[interval_col] == f'0-{interval_days}'].shape[0],
                             df_d.loc[df_d[interval_col] == f'>={interval_days + 1}'].shape[0],
                             df_d.loc[(df_d['WHO_classification'] != '')].shape[0]]
                data_set.append(t2_data_1)
        bb = []
        for b in range(len(t2_title_3)):
            bb.append('')
        data_set.append(bb)

    copy_t2_title_1 = t2_title_1.copy()
    for cc in range(list_len_2 - len(t2_title_1)):
        copy_t2_title_1.append('')

    data_set[data_set.index(t2_title_1)] = copy_t2_title_1

    columns = [f'Table of {interval_dose} by WHO_severity']
    merge_info = [columns.copy()[0], 0, list_len_1, t2_title_1[0], data_set.index(copy_t2_title_1), list_len_2]

    for c in range(list_len_1 - len(columns)):
        columns.append('')

    df_returned = pd.DataFrame(data_set, columns=columns)
    df_returned = df_returned.fillna('')

    return df_returned, merge_info


def write_xlsx(data_list: list, workbook_name, worksheet_name: list, merge_info_list, col_len_width=None):
    all_col_len_list = []
    for single_data_pd in data_list:
        data_col_len_list = []
        for col_name in list(single_data_pd):
            if len(single_data_pd[col_name]):
                data_col_len_list.append(max(single_data_pd[col_name].apply(lambda x: len(str(x)))))
            else:
                data_col_len_list.append(1)
        all_col_len_list.append(data_col_len_list)

    workbook = xlsxwriter.Workbook(workbook_name)
    for idx, d in enumerate(zip(data_list, worksheet_name)):
        if type(d[0]) is pd.DataFrame:
            pd_list = d[0].fillna('').values.tolist()
            pd_col_list = list(d[0])
            col_param_list = []
            header = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'size': 12})
            for col_name in pd_col_list:
                col_param_list.append({'header': col_name, 'format': header})
            sheet = workbook.add_worksheet(d[1])
            # # freeze the rows and columns
            if d[1] == 'Covid_vaccination':
                sheet.freeze_panes(1, 10)
                fil = True
            else:
                fil = False
            if len(pd_list):
                sheet.add_table(0, 0, len(pd_list), len(pd_list[0]) - 1,
                                {'data': pd_list, 'autofilter': fil, 'columns': col_param_list})
            for _ in range(len(pd_list) + 1):
                sheet.set_row(_, 25, cell_format=header)
            for idxx, l in enumerate(zip(all_col_len_list[idx], pd_col_list)):
                if d[1] == 'Covid_vaccination':
                    sheet.set_column(idxx, idxx, max(l[0], len(l[1])) * 1.5)
                else:
                    sheet.set_column(idxx, idxx, max(16, 16) * 1.5)

            if d[1] != 'Covid_vaccination':
                merge_format = workbook.add_format({'bold': 1, 'align': 'center',
                                                    'valign': 'vcenter', 'fg_color': 'yellow'})
                if merge_info_list[idx]:
                    merge_tile_1 = merge_info_list[idx][0]
                    merge_tile_2 = merge_info_list[idx][3]
                    l_1 = merge_info_list[idx][1]
                    l_2 = merge_info_list[idx][2]
                    l_3 = merge_info_list[idx][-2]
                    l_4 = merge_info_list[idx][-1]
                    sheet.merge_range(0, 0, l_1, l_2 - 1, merge_tile_1, merge_format)
                    sheet.merge_range(l_3 + 1, 0, l_3 + 1, l_4 - 1, merge_tile_2, merge_format)
    workbook.close()


def download_comparison_data():
    url = "http://192.168.92.29:8080/share.cgi?ssid=0mUU3e7&fid=0mUU3e7&path=%2F02_%E7%A2%BA%E8%A8%BA%E5%80%8B%E6%A1%88%E7%96%AB%E8%AA%BF&filename=" \
          "%E5%8D%80%E7%AE%A1_%E5%80%8B%E6%A1%88%E6%B3%95%E5%82%B3%E7%B7%A8%E8%99%9F%E5%8F%8A%E6%A1%88%E6%AC%A1%E8%99%9F%E5%B0%8D%E7%85%A7%E8%A1%A8.xlsx&" \
          "openfolder=forcedownload&ep="
    with open(os.path.join('.', '法傳編號與案次號對照表.xlsx'), 'wb') as f:
        f.write(requests.get(url, verify=False).content)


def interval_vac_day(x, vac_type):
    d = ''
    try:
        if x[vac_type]:
            d = (x['SICK_DATE'] - x[vac_type]).days
        return d
    except Exception as e:
        print(type(x[vac_type]), vac_type)


if __name__ == "__main__":

    # # delete file
    delete_folder_list = ['sftp_upload_folder', 'sftp_download_folder']
    for f in delete_folder_list:
        delete_file_list = os.listdir(os.path.abspath(os.path.join('.', f)))
        for f_n in delete_file_list:
            full_path = os.path.abspath(os.path.join('.', f, f_n))
            if os.path.isdir(full_path):
                os.rmdir(full_path)
            if os.path.isfile(full_path):
                os.remove(full_path)

    # # sql data download
    sql_code = dict(confirm_case='''
    select
    t1.IDNO,
    t2.NAME,
    t1.REPORT,
    t1.SICK_AGE,
    t1.BIRTHDAY,
    t1.GENDER,
    t1.REPORT_DATE,t1.SICK_DATE,
    case t1.IMMIGRATION when 0 then 'domestic'
    when 1 then 'imported'
    when 8 then 'unknown' end as IMMIGRATION,
    t3.DETERMINED_STATUS_DESC,
    t4.COUNTRY_NAME as NATIONALITY,
    t5.COUNTY_NAME as RESIDENCE_COUNTY,
    t6.COUNTY_NAME as REPORT_COUNTY,
    t7.OCCUPATION_DESC

    from CDCDW.USV_DWS_REPORT_DETAIL_EIC_UTF8 t1
    left join CDCDW.USV_INDIVIDUAL_SAS t2 on t1.INDIVIDUAL = t2.INDIVIDUAL
    left join CDCDW.DIM_DETERMINED_STATUS t3 on t1.DETERMINED_STATUS = t3.DETERMINED_STATUS
    left join CDCDW.DIM_COUNTRY t4 on t1.NATIONALITY = t4.COUNTRY
    left join CDCDW.DIM_TOWN t5 on t1.RESIDENCE_TOWN = t5.TOWN
    left join CDCDW.DIM_TOWN t6 on t1.REPORT_TOWN = t6.TOWN
    left join CDCDW.DIM_OCCUPATION t7 on t1.OCCUPATION = t7.OCCUPATION
    where t1.DISEASE = '19CoV'
    and t1.DETERMINED_STATUS = 5
    and t1.REPORT_DATE >= TO_DATE('2020/1/1', 'YYYY/MM/DD')
    ''')

    df_confirm_case = get_raw_data('confirm_case', sql_code['confirm_case'])
    df_confirm_case = df_confirm_case.drop_duplicates(subset=['IDNO', 'REPORT'])
    for date_col in ['BIRTHDAY', 'REPORT_DATE', 'SICK_DATE']:
        df_confirm_case.loc[:, date_col] = df_confirm_case.loc[:, date_col].apply(lambda x: pd.to_datetime(x).date())
    df_confirm_case.loc[:, 'REPORT'] = df_confirm_case.loc[:, 'REPORT'].astype(str)
    df_confirm_case.to_csv(os.path.abspath(os.path.join('.', 'confirm_case.csv')), index=False)

    # # 法傳案次號對照表
    download_comparison_data()
    case_n_data = pd.read_excel(
        os.path.join('.', '法傳編號與案次號對照表.xlsx'), header=[1], usecols=['傳染病報告單編號', '案號'], index_col=False).rename(
        columns={'傳染病報告單編號': 'REPORT', '案號': 'CASE_NUMBER'}).astype(str)

    df_merge = case_n_data.merge(df_confirm_case, how='left', on='REPORT')

    # # df for sftp uploading
    df_upload = df_merge[['IDNO', 'BIRTHDAY']]
    df_upload = df_upload.drop_duplicates()
    df_upload.loc[:, 'SERIAL_NUMBER'] = df_upload.index + 1
    df_upload = df_upload[['SERIAL_NUMBER', 'IDNO', 'BIRTHDAY']].rename(
        columns={'SERIAL_NUMBER': '流水號', 'IDNO': '身分證號', 'BIRTHDAY': '出生日期'})
    t = datetime.datetime.now().strftime('%Y%m%d%H')
    upload_file_path = os.path.abspath(os.path.join('.', 'sftp_upload_folder', f"Confirm{t}.csv"))
    df_upload.to_csv(upload_file_path, index=False)

    if os.path.exists(upload_file_path):
        sftp_upload(upload_file_path)
    else:
        print(f"{os.path.basename(upload_file_path)} doesn't exist. Please check the file.")

    print('Waiting for returned file.')
    time.sleep(2100)

    # # download the returned vaccination data
    save_folder = os.path.abspath(os.path.join('.', 'sftp_download_folder'))
    r_times = 0
    save_path = ''
    while r_times < 3:
        save_path = sftp_download(save_folder)
        if save_path:
            break
        else:
            r_times += 1
            time.sleep(300)

    #     save_path = os.path.abspath(os.path.join('.', 'sftp_download_folder', 'RConfirm2021062106_20210621070606.csv'))

    col_list = [
        'SERIAL_NUMBER', 'NIIS_Category_I', 'NIIS_Category_II', 'Brand_1st', 'Dose_number',
        'Vaccination_date_1st', 'Batch_1st']
    df_vac = pd.read_csv(os.path.join(save_path), header=None, names=col_list)
    df_vac = df_vac.dropna(subset=['Vaccination_date_1st'])
    df_upload = df_upload.rename(columns={'流水號': 'SERIAL_NUMBER', '身分證號': 'IDNO', '出生日期': 'BIRTHDAY'})
    df_upload.loc[:, 'SERIAL_NUMBER'] = df_upload.loc[:, 'SERIAL_NUMBER'].astype(int)
    df_vac.loc[:, 'SERIAL_NUMBER'] = df_vac.loc[:, 'SERIAL_NUMBER'].astype(int)

    df_vac_1st = df_vac.loc[df_vac['Dose_number'] == '1']
    df_vac_2nd = df_vac.loc[df_vac['Dose_number'] == '2'][['SERIAL_NUMBER', 'Brand_1st', 'Vaccination_date_1st', 'Batch_1st']]

    df_vac_2nd = df_vac_2nd.rename(columns={'Vaccination_date_1st': 'Vaccination_date_2nd', 'Batch_1st': 'Batch_2nd', 'Brand_1st': 'Brand_2nd'})
    df_vac_new = df_vac_1st.merge(df_vac_2nd, how='left', on='SERIAL_NUMBER')
    df_vac_new = df_vac_new.drop(columns='Dose_number')
    df_vac_new = df_vac_new[['SERIAL_NUMBER', 'NIIS_Category_I', 'NIIS_Category_II',
                             'Brand_1st', 'Vaccination_date_1st', 'Brand_2nd', 'Vaccination_date_2nd',
                             'Batch_1st', 'Batch_2nd']]

    df_vac_up_merge = df_upload.merge(df_vac_new, how='left', on=['SERIAL_NUMBER'])
    df_vac_up_merge.loc[:, 'BIRTHDAY'] = df_vac_up_merge.loc[:, 'BIRTHDAY'].apply(lambda x: pd.to_datetime(x).date())
    df_merge.loc[:, 'BIRTHDAY'] = df_merge.loc[:, 'BIRTHDAY'].apply(lambda x: pd.to_datetime(x).date())
    df_confirm_merge = df_merge.merge(df_vac_up_merge, how='left', on=['IDNO', 'BIRTHDAY'])
    df_confirm_merge = df_confirm_merge.fillna('')
    df_confirm_merge.loc[:, 'Interval_1st'] = ''
    df_confirm_merge.loc[:, 'Interval_2nd'] = ''

    for d_vac in ['SICK_DATE', 'Vaccination_date_1st', 'Vaccination_date_2nd']:
        df_confirm_merge.loc[:, d_vac] = df_confirm_merge.loc[:, d_vac].apply(lambda x: pd.to_datetime(x).date())
    df_confirm_merge = df_confirm_merge.fillna('')
    df_confirm_merge.loc[:, 'Interval_1st'] = df_confirm_merge.apply(
        lambda x: interval_vac_day(x, 'Vaccination_date_1st'), axis=1)
    df_confirm_merge.loc[:, 'Interval_2nd'] = df_confirm_merge.apply(
        lambda x: interval_vac_day(x, 'Vaccination_date_2nd'), axis=1)
    df_confirm_merge = df_confirm_merge.drop(columns='SERIAL_NUMBER')

    # # access gsheet and merge who classification
    gc = pygsheets.authorize(
        service_file='/home/rserver/task/DR_SU_COVID_VACCINE/quickstart-1579071412520-c699fadd5464.json')
    survey_url = 'https://docs.google.com/spreadsheets/d/1FmH-KfivHhtIiQvgJ2jTrXmKRHbDqJi75oGLO5ulDGQ/'
    sh = gc.open_by_url(survey_url)
    wks = sh.worksheet_by_title("工作表1")
    wks.export(filename='df_gsheet', path=os.path.abspath(os.path.join('.')))
    df_gsheet = pd.read_csv(os.path.abspath(os.path.join('.', 'df_gsheet.csv')), skiprows=6, usecols=['法傳編號', 'WHO分類'])
    df_gsheet = df_gsheet.rename(columns={'法傳編號': 'REPORT', 'WHO分類': 'WHO_classification'})
    df_gsheet.loc[:, 'REPORT'] = df_gsheet.loc[:, 'REPORT'].astype(str)

    # # final data
    df_final = df_confirm_merge.merge(df_gsheet, how='left', on='REPORT')
    df_final = df_final.fillna('')
    df_final.loc[:, 'Interval_1st'] = df_final.loc[:, 'Interval_1st'].apply(lambda x: int(x) if x else x)
    df_final.loc[:, 'Interval_2nd'] = df_final.loc[:, 'Interval_2nd'].apply(lambda x: int(x) if x else x)
    df_final.loc[:, 'SICK_AGE'] = df_final.loc[:, 'SICK_AGE'].apply(lambda x: int(x) if x else x)
    for c_d in ['BIRTHDAY', 'REPORT_DATE', 'SICK_DATE', 'Vaccination_date_1st', 'Vaccination_date_2nd']:
        df_final.loc[:, c_d] = df_final.loc[:, c_d].astype(str)
    df_final.to_csv(os.path.abspath(os.path.join('.', 'df_final.csv')), index=False)

    # # Create required table: interval days and WHO severity
    df_dataset = df_final.copy()
    # # interval 1st
    df_dataset = df_dataset.loc[df_dataset['Interval_1st'] != '']
    df_dataset.loc[:, 'Interval_1st'] = df_dataset.loc[:, 'Interval_1st'].apply(lambda x: int(x) if x else x)
    df_dataset = df_dataset.loc[df_dataset['Vaccination_date_1st'] != '']
    df_dataset = df_dataset.loc[df_dataset['Interval_1st'] >= 0]
    df_dataset = df_dataset.loc[df_dataset['WHO_classification'] != ''].reset_index(drop=True)
    df_dataset.loc[:, 'age_group'] = df_dataset.apply(lambda x: age_group(x['SICK_AGE']), axis=1)
    df_dataset.loc[:, 'interval_1_13'] = df_dataset.apply(lambda x: interval_group(x['Interval_1st'], 13), axis=1)
    df_dataset.loc[:, 'interval_1_20'] = df_dataset.apply(lambda x: interval_group(x['Interval_1st'], 20), axis=1)
    syndrome_list = sorted(list(set(df_dataset.loc[:, 'WHO_classification'].values.tolist())))
    brand_1st_list = sorted(list(set(df_dataset.loc[:, 'Brand_1st'].values.tolist())))
    data_1st_13 = table_data(df_dataset, 13, 'interval_1_13', 'Interval_1st', syndrome_list, brand_1st_list, 'Brand_1st')
    df_d_1st_13, merge_info_1_13 = data_1st_13[0], data_1st_13[1]
    data_1st_20 = table_data(df_dataset, 20, 'interval_1_20', 'Interval_1st', syndrome_list, brand_1st_list, 'Brand_1st')
    df_d_1st_20, merge_info_1_20 = data_1st_20[0], data_1st_20[1]

    # # interval 2nd
    df_dataset_2nd = df_dataset.loc[df_dataset['Interval_2nd'] != '']
    df_dataset_2nd = df_dataset_2nd.loc[df_dataset_2nd['Interval_2nd'] >= 0]
    df_dataset_2nd.loc[:, 'interval_2_6'] = df_dataset_2nd.apply(lambda x: interval_group(x['Interval_2nd'], 6), axis=1)
    df_dataset_2nd.loc[:, 'interval_2_13'] = df_dataset_2nd.apply(lambda x: interval_group(x['Interval_2nd'], 13), axis=1)
    brand_2nd_list = sorted(list(set(df_dataset_2nd.loc[:, 'Brand_2nd'].values.tolist())))
    data_2nd_6 = table_data(df_dataset_2nd, 6, 'interval_2_6', 'Interval_2nd', syndrome_list, brand_2nd_list, 'Brand_2nd')
    df_d_2nd_6, merge_info_2_6 = data_2nd_6[0], data_2nd_6[1]
    data_2nd_13 = table_data(df_dataset_2nd, 13, 'interval_2_13', 'Interval_2nd', syndrome_list, brand_2nd_list, 'Brand_2nd')
    df_d_2nd_13, merge_info_2_13 = data_2nd_13[0], data_2nd_13[1]

    merge_info_list = [[], merge_info_1_13, merge_info_1_20, merge_info_2_6, merge_info_2_13]

    # write to excel
    work_book_path = os.path.abspath(os.path.join('.', 'Test_Covid_vaccination.xlsx'))
    write_xlsx([df_final, df_d_1st_13, df_d_1st_20, df_d_2nd_6, df_d_2nd_13], work_book_path,
               ['Covid_vaccination', '14 days after dose 1', '21 days after dose 1', '7 days after dose 2',
                '14 days after dose 2'], merge_info_list)

    # # await for xlsx file update
    time.sleep(15)
    # # zip file
    zip_name = os.path.abspath(os.path.join('.', "Covid_vaccination.zip"))
    Popen(['zip', '-j', zip_name, work_book_path, '-P', '1922']).communicate()

    # # await for zip file update
    time.sleep(15)
    # # send e-mail
    e_mail_list = ['wei-ju@cdc.gov.tw', 'liuyl@cdc.gov.tw', 'sfchen@cdc.gov.tw', 'yudihsu@cdc.gov.tw',
                   'yusheng02@cdc.gov.tw']
    test_mail = ['yudihsu@cdc.gov.tw']
    mail = SMTP(receiver=e_mail_list, attachment=[zip_name],
                subject=f'{os.path.splitext(os.path.basename(zip_name))[0]}',
                content=f'{os.path.splitext(os.path.basename(zip_name))[0]}.zip.\n confirm cases with vaccination',
                sender='e-covid_vaccination@service.cdc.gov.tw')
    mail.send()
