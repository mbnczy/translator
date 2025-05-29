import os
import pandas as pd
from tqdm import tqdm

import logging

import gc

def split_dataframe(df_path, num_chunks, out_path, logger, row_start=-1, row_end=-1):
    df = pd.read_csv(df_path)
    
    if row_start != -1 and row_end != -1:
        df = df[row_start:row_end]
    elif row_start != -1:
        df = df[row_start:]
    elif row_end != -1:
        df = df[:row_end]

    del row_start, row_end, df_path
    
    total_rows = len(df)
    rows_per_chunk = total_rows // num_chunks
    remainder = total_rows % num_chunks

    start_idx = 0
    with tqdm(total=num_chunks, desc='splitage', file=open(os.devnull, 'w')) as progress_bar:
        logger.info(str(progress_bar))
        for i in range(num_chunks):
            try:
                path= os.path.join(out_path,f'chunk_{i}.csv')
                end_idx = start_idx + rows_per_chunk + (1 if i < remainder else 0)
                df.iloc[start_idx:end_idx].to_csv(path)
                start_idx = end_idx
                progress_bar.update(1)
                if progress_bar.n % 3 == 0:
                    logger.info(str(progress_bar))
            except Exception as e:
                logger.exception(f'🚨 split & write failed at {i} chunk - {type(e)}: {e.args}')
        logger.info(str(progress_bar))
    del path, start_idx, end_idx, rows_per_chunk, remainder, progress_bar, num_chunks, total_rows, df
    gc.collect()
