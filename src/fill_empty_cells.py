import torch
import os
import warnings
warnings.filterwarnings("ignore")
import pandas as pd
from torch.utils.data import DataLoader
from easynmt import EasyNMT
from tqdm import tqdm 
import gc
import time

from utils.datasets import TranslateDataset

import logging

import gc

import threading

import nltk
nltk.download('punkt_tab')

model = EasyNMT('opus-mt')
data_name = 'es'
data_path = "../data/es_not_translated_translated.csv"
out_path = 'es_not_translated_translated_filled.csv'
source_lang = 'es'
target_lang = 'en'
delimiter = ','
num_workers = 0#1
write_step = 1
column_name = 'text'
translated_column_name = 'english_text'
log_interval=2


logger = logging.getLogger("tr")
logger.setLevel(logging.INFO)
#fh = logging.FileHandler(f"../logs/{data_name}/{data_name}.log")
fh = logging.FileHandler(f"{data_name}.log")
fh.setLevel(logging.INFO)
fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(fh)

data = pd.read_csv(data_path)

nones = data[data[translated_column_name].isna()]
logger.info(f"None values count: {len(nones)}")

dataset = TranslateDataset(model,nones,source_lang,target_lang, logger, column_name, translated_column_name)
dataloader = DataLoader(dataset,batch_size=write_step,shuffle=False, num_workers=num_workers)
total_batches = len(dataloader)
with tqdm(total=total_batches, desc="processing batches", file=open(os.devnull, 'w')) as progress_bar:
    logger.info(str(progress_bar))
    for i, batch in enumerate(dataloader):
        df = pd.DataFrame(batch)
        df.to_csv(out_path, index=False, mode='a', header=(i == 0), encoding="utf-8", sep=delimiter)
        progress_bar.update(1)
        if progress_bar.n % log_interval == 0:
            progress_bar.set_postfix_str(f'lines ready: {len(df)*progress_bar.n}/{len(nones)}')
            logger.info(str(progress_bar))


nones = pd.read_csv(out_path)
data.set_index('id', inplace=True)
nones.set_index('id', inplace=True)

data.update(nones)

data.reset_index(inplace=True)

data.to_csv(out_path, index=False)
#logger.info(f"New file saved")

nones = 0
nones = data[data[translated_column_name].isna()]
if len(nones)>0:
    logger.info(f"New file saved, {len(nones)} none values left:\n{pd.DataFrame(nones).iloc[:,:]}")
else:
    logger.info(f"New file saved")