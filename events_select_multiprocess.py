import os
import pandas as pd
import pickle
import datetime
import time
import multiprocessing as mp


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


def visit_type_select(val_dict: dict, syndrome: str):
    visit_type_dict = {'Anaphylaxis': {'門診_天數': [0, 0], '急診_天數': [1, 0], '住院_天數': [0, 0]},
                       'Immune thrombocytopenic purpura': {'門診_天數': [2, 90], '急診_天數': [0, 0], '住院_天數': [1, 0]},
                       'Deep vein thrombosis': {'門診_天數': [2, 30], '急診_天數': [0, 0], '住院_天數': [1, 0]},
                       'Pulmonary embolus': {'門診_天數': [2, 30], '急診_天數': [0, 0], '住院_天數': [1, 0]},
                       'Guillain-Barré syndrome and Miller Fisher syndrome': {'門診_天數': [0, 0], '急診_天數': [0, 0], '住院_天數': [1, 0]},
                       'Cerebrovascular stroke (including CVST)': {'門診_天數': [0, 0], '急診_天數': [0, 0], '住院_天數': [1, 0]},
                       'Transverse myelitis': {'門診_天數': [0, 0], '急診_天數': [0, 0], '住院_天數': [1, 0]},
                       'Acute disseminated encephalomyelitis (ADEM)': {'門診_天數': [0, 0], '急診_天數': [0, 0], '住院_天數': [1, 0]},
                       'Spontaneous abortion or Stillbirth': {'門診_天數': [0, 0], '急診_天數': [0, 0], '住院_天數': [0, 0]},
                       'Preterm birth': {'門診_天數': [0, 0], '急診_天數': [0, 0], '住院_天數': [0, 0]},
                       'Full-term birth': {'門診_天數': [0, 0], '急診_天數': [0, 0], '住院_天數': [0, 0]},
                       'Myocarditis_pericarditis': {'門診_天數': [0, 0], '急診_天數': [0, 0], '住院_天數': [1, 0]}
                       }

    # val_dict = {f"{exam_date_list[0]}": []}
    val_dict_return = val_dict.copy()
    for k, v in val_dict.items():
        for v_type in ['門診_天數', '急診_天數', '住院_天數']:
            visit_times = visit_type_dict[syndrome][v_type][0]
            visit_period = visit_type_dict[syndrome][v_type][1]
            if visit_times:
                if len(v) >= visit_times:
                    if len(v) >= 2 and visit_period:
                        for i in range(1, len(v)):
                            if abs((v[i] - v[i-1]).days) > visit_period:
                                print(k, v)
                                try:
                                    val_dict_return[str(v[i])] = val_dict_return[str(v[i-1])].remove(v[i-1])
                                    # print(val_dict_return[str(v[i])])
                                    del val_dict_return[str(v[i-1])]
                                except Exception as e:
                                    print(e)
                                    print('-'*100)
                            else:
                                break
                else:
                    print('-'*100)
                    val_dict_return.pop(k)

    return val_dict_return


def sort_dict_fun(target_dict: dict):
    sorted_dict = {}
    for d in sorted(target_dict.items(), key=lambda x: x[0]):
        if d[0] not in sorted_dict:
            sorted_dict[d[0]] = d[1]
    return sorted_dict


def processing_func(idno_list):
    df_select = get_processing_rg_func(os.path.join('.', 'daily_pickle_store_folder', 'ICDX_pickle'))[1]
    df_select = df_select.reset_index(drop=True)
    df_select = df_select[df_select['clean_period_days'] != '']

    df_ = pd.DataFrame()
    for idno in idno_list:
        df_temp = df_select[df_select['身分證號'] == idno]

        syndrome_list = sorted(set(df_temp.loc[:, 'syndrome'].values.tolist()))
        for syndrome in syndrome_list:
            exam_date_list = sorted(set(df_temp.loc[(df_temp['syndrome'] == syndrome)].loc[:, '就醫日期'].values.tolist()))
            clean_period_days = next(iter(set(df_temp.loc[(df_temp['syndrome'] == syndrome)].loc[:, 'clean_period_days'].values.tolist())))
            vaccine_injection_day = ''
            val_dict = {f"{exam_date_list[0]}": [exam_date_list[0]]}
            val_date = exam_date_list[0]
            
            for exam_date in exam_date_list:
                if abs((exam_date - val_date).days) >= clean_period_days:
                    val_date = exam_date
                    if str(exam_date) not in val_dict:
                        val_dict[str(exam_date)] = [exam_date]
                        # val_dict[str(exam_date)].append(exam_date)
                    else:
                        if exam_date not in val_dict[str(exam_date)]:
                            val_dict[str(exam_date)].append(exam_date)
                else:
                    if exam_date not in val_dict[str(val_date)]:
                        val_dict[str(val_date)].append(exam_date)

            val_dict = visit_type_select(val_dict, syndrome)
            val_dict = sort_dict_fun(val_dict)

            for k, v in val_dict.items():
                if vaccine_injection_day:
                    if v[0] >= vaccine_injection_day:
                        temp_df_ = df_temp.loc[(df_temp['就醫日期'] == v[0]) & (df_temp['syndrome'] == syndrome)]
                        temp_df_ = temp_df_.drop_duplicates(subset=['身分證號', '就醫日期', 'syndrome'])
                        if not temp_df_.empty:
                            df_ = df_.append(temp_df_)
                else:
                    if v:
                        temp_df_ = df_temp.loc[(df_temp['就醫日期'] == v[0]) & (df_temp['syndrome'] == syndrome)]
                        temp_df_ = temp_df_.drop_duplicates(subset=['身分證號', '就醫日期', 'syndrome'])
                        if not temp_df_.empty:
                            df_ = df_.append(temp_df_)

    df_ = df_.reset_index(drop=True)

    with open(os.path.join('.', 'df_clean_period.pickle'), 'wb') as pl:
        pickle.dump(df_, pl)
    return df_


def get_processing_rg_func(dir_path):  # loading data and get unique id
    df = pd.DataFrame()
    for file in os.listdir(dir_path):
        _df = pd.read_pickle(os.path.join(dir_path, file))
        df = df.append(_df)
    df = df.reset_index(drop=True)

    df_select = pd.DataFrame()
    # # Terminate pandas loc. Warning
    pd.options.mode.chained_assignment = None
    aesi_diag_dict = aesi_diag_code(os.path.join('.', 'adr_AESI_diagnosis_code.csv'))
    for k, v in aesi_diag_dict.items():
        temp_df = df.loc[(df['syndrome'] == k)]
        temp_df.loc[:, 'clean_period_days'] = v['clean_period_days']
        df_select = df_select.append(temp_df)

    idno_list = df_select.drop_duplicates(subset=['身分證號']).reset_index(drop=True).loc[:, '身分證號'].values.tolist()

    return idno_list, df_select


# def incidence_rate_func(df):


def main():
    # # divide df to numbers of num_processes to chunk list
    # # Terminate pandas loc. Warning
    s1 = time.time()
    idno_list, df_select = get_processing_rg_func(os.path.join('.', 'daily_pickle_store_folder', 'ICDX_pickle'))

    # calculate processing core for multiprocess
    num_processes = mp.cpu_count()
    chunk_size = int(len(idno_list) / num_processes)

    chunks = [idno_list[i:i + chunk_size] for i in range(0, len(idno_list), chunk_size)]

    pool = mp.Pool(processes=num_processes)
    result = pool.map(processing_func, chunks)
    df_f = pd.concat(result).reset_index(drop=True)
    s3 = time.time()
    print(s3 - s1)
    print('-' * 100)

    with open(os.path.join('.', f"event_selection_{datetime.datetime.today().date().strftime('%Y%m%d')}.pickle"), 'wb') as pl:
        pickle.dump(df_f, pl)
        

if __name__ == "__main__":
    main()
