import os
import pandas as pd

import logging

import gc

def length_check(chunks_folder, out_folder, num_chunks, logger):
    log_lst = []
    for index in range(0,num_chunks):
        in_length=len(pd.read_csv(os.path.join(chunks_folder,f'chunk_{index}.csv')))
        if os.path.exists(os.path.join(out_folder,f'out_{index}.csv')):
            out_length = len(pd.read_csv(os.path.join(out_folder,f'out_{index}.csv')))
            log_lst.append([in_length,out_length])
        else:
            log_lst.append([in_length, None])
    log_str = '\nvalidation report:\n'
    for index, element in enumerate(log_lst):
        if element[1] is not None:
            log_str += f"- {index}: {element[0]} -> {element[1]} {'✅' if int(element[0])==int(element[1]) else '⚠️'}\n"
        else:
            log_str += f"- {index}: output is missing 🚨\n"
    log_str += f"total loss: {sum(row[0] for row in log_lst) - sum(row[1] if isinstance(row[1], int) else 0 for row in log_lst)}"
    logger.info(log_str)
    del chunks_folder, out_folder, num_chunks, log_lst, in_length, log_str
    gc.collect()

def check_sus_rows(source_data, out_data, logger, log_csv, col_name='text'):
    source_data = pd.read_csv(source_data)
    out_data = pd.read_csv(out_data)
    
    out = out_data[~out_data[col_name].isin(source_data[col_name])]
    
    if len(out)==0:
        logger.info(f'Suspicious rows count: 0 ✅')
    else:
        pd.DataFrame(out).to_csv(log_csv)
        logger.error(f'Suspicious rows count: {len(out)} 🚨 -> saved to {log_csv}')

def check_duplicated_rows(out_data, logger, log_csv):
    out_data = pd.read_csv(out_data)
    
    out_bool = True
    out_str = "Duplicates:\n"
    
    for col_name in out_data.columns:
        out = out_data[out_data[col_name].duplicated()]
        out_name = f'{log_csv[:-3]}_{col_name}_dup.csv'
        if len(out)!=0:
            pd.DataFrame(out).to_csv(out_name)
            out_bool = False
            out_str+=f"- {col_name}: {len(out)} 🚨 saved to {out_name}\n"
        else:
            out_str+=f"- {col_name}: {len(out)} ✅\n"
    out = out_data[out_data.duplicated()]
    if len(out)!=0:
        pd.DataFrame(out).to_csv(f'{log_csv[:-4]}_overall_dup.csv')
        out_str+=f"- overall: {len(out)} saved to {log_csv[:-4]}_overall_dup.csv"
    else:
        out_str+=f"- overall: {len(out)} ✅\n"
    if out_bool:
        logger.info(f'No duplicate rows found ✅')
    else:
        logger.error(out_str)

def check_none_rows(out_data, logger, log_csv):
    out_data = pd.read_csv(out_data)

    out_bool = True
    out_str = "Nones:\n"
    
    for col_name in out_data.columns:
        out = out_data[out_data[col_name].isna()]
        out_name = f'{log_csv[:-3]}_{col_name}_nones.csv'
        if len(out)!=0:
            pd.DataFrame(out).to_csv(out_name)
            out_bool = False
            out_str+=f"- {col_name}: {len(out)} 🚨 saved to {out_name}\n"
        else:
            out_str+=f"- {col_name}: {len(out)} ✅\n"
    out = out_data[out_data.duplicated()]
    if len(out)!=0:
        pd.DataFrame(out).to_csv(f'{log_csv[:-4]}_overall_nones.csv')
        out_str+=f"- overall: {len(out)} saved to {log_csv[:-4]}_overall_nones.csv"
    else:
        out_str+=f"- overall: {len(out)} ✅\n"
    
    if out_bool:
        logger.info(f'No none rows found ✅')
    else:
        logger.error(out_str)

def check_out_length(source_data, out_data, logger):
    source_data = pd.read_csv(source_data)
    out_data = pd.read_csv(out_data)
    if len(source_data) == len(out_data):
        logger.info(f"Row's count is matching ✅ {len(source_data)} : {len(out_data)}")
    else:
        logger.error(f"Row's count is not matching 🚨{len(source_data)} : {len(out_data)} -> {len(source_data) == len(out_data)}")
    
    
def check_missing_rows(source_data, out_data, logger, log_csv, col_name='text'):
    source_data = pd.read_csv(source_data)
    out_data = pd.read_csv(out_data)
    
    out = source_data[~source_data[col_name].isin(out_data[col_name])]
    
    if len(out)==0:
        logger.info(f'Missing rows count: 0 ✅')
    else:
        pd.DataFrame(out).to_csv(log_csv)
        logger.error(f'Missing rows count: {len(out)} 🚨 -> saved to {log_csv}')
