import os
import pandas as pd
from tqdm import tqdm 

import logging

import gc

def concat(out_folder, result_name, num_chunks, logger):
    with tqdm(total=num_chunks, desc='concatenation', file=open(os.devnull, 'w')) as progress_bar:
        logger.info(str(progress_bar))
        for i in range(num_chunks):
            try:
                data = pd.read_csv(os.path.join(out_folder,f'out_{i}.csv'))
                if 'Unnamed: 0' in data.columns:
                    data.drop(columns=['Unnamed: 0'], inplace=True)
                if 'Unnamed: 0.1' in data.columns:
                    data.drop(columns=['Unnamed: 0.1'], inplace=True)
                data.to_csv(result_name,mode='a',header=(i == 0), index=False)
                
                progress_bar.update(1)
                if progress_bar.n % 3 == 0:
                    logger.info(str(progress_bar))
            except Exception as e:
                logger.exception(f'concatenation failed at chunk {i} - {type(e)}: {e.args}')
        logger.info(str(progress_bar))
    del out_folder, result_name, num_chunks,progress_bar
    gc.collect()
        