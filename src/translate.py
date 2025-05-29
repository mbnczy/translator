import os
import warnings
warnings.filterwarnings("ignore")
import pandas as pd
#import torch
from torch.utils.data import DataLoader
#from easynmt import EasyNMT
from tqdm import tqdm 
import gc
import time

from utils.datasets import TranslateDataset

import logging

import gc

import threading

import nltk
nltk.download('punkt_tab')

def translate(
    model,
    data_name:str,
    data_path:str,
    out_path:str,
    source_lang:str,
    target_lang:str,
    delimiter = ',',
    num_workers = 1,
    write_step = 1,
    column_name = 'text',
    translated_column_name = 'translated_text',
    row_start = -1,
    row_end = -1,
    log_interval=2,
    no_header = False,
    batch_state = 0
    ):
    #torch.multiprocessing.set_start_method('spawn') #https://github.com/pytorch/pytorch/issues/40403
    
    start_time = time.time()
    
    logger = logging.getLogger(f"{data_path.split('/')[-1].split('.')[0]}")
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(f"../logs/{data_name}/chunks/{data_path.split('/')[-1].split('.')[0]}.log")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(fh)

    logger.info(f"Threads: {num_workers} Process ID: {os.getpid()}, Thread ID: {threading.get_ident()}")
    #model = EasyNMT('opus-mt')

    data = pd.read_csv(data_path, delimiter=delimiter)
    data[column_name] = data[column_name].astype(str)

    if row_start != -1 and row_end != -1:
        data = data[row_start:row_end]
    elif row_start != -1:
        data = data[row_start:]
    elif row_end != -1:
        data = data[:row_end]
    
    del row_start, row_end

    header = list(data.columns)
    header.append(translated_column_name)
    dataset = TranslateDataset(model,data,source_lang,target_lang, logger, column_name, translated_column_name)
    dataloader = DataLoader(dataset,batch_size=write_step,shuffle=False, num_workers=num_workers)
    total_batches = len(dataloader)

    with tqdm(total=total_batches, desc="processing batches", file=open(os.devnull, 'w')) as progress_bar:
        logger.info(str(progress_bar))
        if no_header and batch_state>0:
            progress_bar.update(batch_state)
            #for _ in range(batch_state+1):
            #    next(iter(dataloader))
            logger.info('♻️ continue from the last checkpoint ♻️')
            for i, batch in enumerate(dataloader):
                if i >= batch_state:
                    df = pd.DataFrame(batch)
                    df.to_csv(out_path, index=False, mode='a', header=False, encoding="utf-8", sep=delimiter)
                    progress_bar.update(1)
                    if progress_bar.n % log_interval == 0:
                        progress_bar.set_postfix_str(f'lines ready: {len(df)*progress_bar.n}/{len(data)}')
                        logger.info(str(progress_bar))
        else:
            for i, batch in enumerate(dataloader):
                df = pd.DataFrame(batch)
                df.to_csv(out_path, index=False, mode='a', header=(i == 0), encoding="utf-8", sep=delimiter)
                progress_bar.update(1)
                if progress_bar.n % log_interval == 0:
                    progress_bar.set_postfix_str(f'lines ready: {len(df)*progress_bar.n}/{len(data)}')
                    logger.info(str(progress_bar))
        del df
        gc.collect()
        progress_bar.set_postfix_str('')
        logger.info(str(progress_bar))

    hours, remainder = divmod(time.time()-start_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    logger.info(f'Successfully translated\n- from: {source_lang}\n- into: {target_lang}\n- input: {data_path}\n- output:{out_path}\n- runtime: {int(hours):02d}:{int(minutes):02d}:{seconds:05.2f}')
    
    del data, data_name, data_path, out_path, source_lang, target_lang, delimiter, num_workers, write_step, column_name, translated_column_name, log_interval, header, dataset, dataloader, total_batches, progress_bar, hours, remainder, minutes, seconds, logger, start_time, model
    gc.collect()