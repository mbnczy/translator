import warnings
warnings.filterwarnings("ignore")
import torch
#torch.multiprocessing.set_start_method('spawn')

import os
from utils import is_model_available
from utils.preproc import split_dataframe
from easynmt import EasyNMT
from translate import translate
from utils.monitor import active_logging
from utils.validation import length_check, check_sus_rows, check_duplicated_rows, check_none_rows, check_out_length, check_missing_rows
from utils.postproc import concat


import time

import concurrent.futures
import threading

import logging

import json

import gc

import queue
import threading

tr_futures = []

if __name__ == "__main__":
    torch.multiprocessing.set_start_method('spawn') #https://github.com/pytorch/pytorch/issues/40403
    try:
        try:
            with open('config.json', 'r') as file:
                config_json = json.load(file)
                data_path = config_json["data_path"]
                delimiter = config_json["delimiter"]
                source_lang = config_json["source_lang"]
                target_lang = config_json["target_lang"]
                num_chunks = config_json["num_chunks"]
                column_name = config_json["column_name"]
                translated_column_name = config_json["translated_column_name"]
                row_start = config_json["row_start"]
                row_end = config_json["row_end"]
                write_step = config_json["write_step"]
                active_logging_minutes = config_json["active_logging_minutes"]
                log_interval = config_json["log_interval"]
                patience = config_json["patience"]
        except FileNotFoundError:
            print(f"Config is not found.")
        except json.JSONDecodeError:
            print(f"Error decoding Config JSON.")
        del config_json
        #limit threads
        if num_chunks > os.cpu_count()-1:
            num_chunks = os.cpu_count()-1
        
        start_time = time.time()
        
        data_name = data_path.split('/')[-1].split('.')[0]
    
        os.makedirs(f"../logs/{data_name}/chunks", exist_ok=True)
        logger = logging.getLogger(f"{data_name}")
        logger.setLevel(logging.INFO)
        fh = logging.FileHandler(f"../logs/{data_name}/{data_name}.log")
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(fh)
        
        logger.info(f'''config
        data_path = {data_path}
        delimiter = {delimiter}
        source_lang = {source_lang}
        target_lang = {target_lang}
        num_chunks = {num_chunks}
        column_name = {column_name}
        translated_column_name = {translated_column_name}
        row_start = {row_start}
        row_end = {row_end}
        write_step = {write_step}
        active_logging_minutes = {active_logging_minutes}
        log_interval = {log_interval}
        patience = {patience}''')
        
        #check if the model exists or not
        is_available, model_url, commit_hash = is_model_available(source_lang,target_lang,logger)
        if not is_available:
            logger.error('model can not be loaded')
            raise Exception('model can not be loaded')
        
        #make tmp dirs
        os.makedirs(f"../data/tmp_{data_name}/chunks", exist_ok=True)
        os.makedirs(f"../data/tmp_{data_name}/out", exist_ok=True)
        os.makedirs(f"../logs/{data_name}/chunks", exist_ok=True)
        out_path = f"../data/{data_name}_translated.csv"
        
        #splitage
        split_dataframe(df_path = data_path,
                        num_chunks=num_chunks,
                        out_path=f"../data/tmp_{data_path.split('/')[-1].split('.')[0]}/chunks",
                        logger=logger,
                        row_start = row_start,
                        row_end = row_end
                        )
        
        logger.info('🔮 translation started 🔮')
    
        #translate chunks
        model = EasyNMT('opus-mt')
        output_queue = queue.Queue()
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_chunks+1) as executor:
            for index in range(num_chunks):
                tr_futures.append(executor.submit(
                    translate,
                    model,
                    data_name,
                    f"../data/tmp_{data_name}/chunks/chunk_{index}.csv",
                    f"../data/tmp_{data_name}/out/out_{index}.csv",
                    source_lang,target_lang,
                    delimiter,
                    1,
                    write_step,
                    column_name,
                    translated_column_name,
                    -1,
                    -1,
                    log_interval
                    ))
                
            time.sleep(10)
            
            logging_thread = threading.Thread(
                target=active_logging,
                args=(
                    [os.path.join(f"../logs/{data_name}/chunks", file) for file in os.listdir(f"../logs/{data_name}/chunks") if file.endswith('.log')],
                    active_logging_minutes,
                    logger,
                    tr_futures,
                    output_queue,  #pass the queue
                    patience
                ),
                daemon=True
            )
            logging_thread.start()
    
            #get stopped threads attributes
            while logging_thread.is_alive():
                if not output_queue.empty():
                    stopped_thread_index, line_state, batch_state = output_queue.get(timeout=log_interval * 60)
                    #print(stopped_thread_index, line_state, batch_state)
                    tr_futures[stopped_thread_index] = executor.submit(
                                                                translate,
                                                                model,
                                                                data_name,
                                                                f"../data/tmp_{data_name}/chunks/chunk_{stopped_thread_index}.csv",
                                                                f"../data/tmp_{data_name}/out/out_{stopped_thread_index}.csv",
                                                                source_lang,target_lang,
                                                                delimiter,
                                                                1,
                                                                write_step,
                                                                column_name,
                                                                translated_column_name,
                                                                -1,
                                                                -1,
                                                                log_interval,
                                                                no_header = True,
                                                                batch_state=batch_state
                                                                )
            concurrent.futures.wait(tr_futures)
            logging_thread.join()
    
        logger.info('🔮 translation finished 🔮')
        del model
        
        #concatenation
        concat(out_folder = f"../data/tmp_{data_name}/out",
               result_name = f"../data/{data_name}_translated.csv",
               num_chunks=num_chunks,
               logger=logger)
    
        
        #validation methods
        
        length_check(
            chunks_folder = f"../data/tmp_{data_name}/chunks",
            out_folder = f"../data/tmp_{data_name}/out",
            num_chunks = num_chunks,
            logger = logger
        )
    
        check_sus_rows(
            source_data = f"../data/{data_name}.csv",
            out_data = f"../data/{data_name}_translated.csv",
            logger = logger,
            log_csv = f'errorlog_{data_name}.csv',
            col_name = 'text'
        )
        
        check_duplicated_rows(
            out_data = f"../data/{data_name}_translated.csv",
            logger = logger,
            log_csv = f'errorlog_{data_name}.csv'
        )
        
        check_none_rows(
            out_data = f"../data/{data_name}_translated.csv",
            logger = logger,
            log_csv = f'errorlog_{data_name}.csv'
        )
        
        check_out_length(
            source_data = f"../data/{data_name}.csv",
            out_data = f"../data/{data_name}_translated.csv",
            logger = logger
        )
        
        check_missing_rows(
            source_data = f"../data/{data_name}.csv",
            out_data = f"../data/{data_name}_translated.csv",
            logger = logger,
            log_csv = f'errorlog_{data_name}_missing.csv',
            col_name = 'text'
        )
        
        
        hours, remainder = divmod(time.time()-start_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        logger.info(f'Translation finished\n- from: {source_lang}\n- into: {target_lang}\n- input: {data_path}\n- output: {out_path}\n- model: {model_url}\n- commit hash: {commit_hash}\n- runtime: {int(hours):02d}:{int(minutes):02d}:{seconds:05.2f}')
    
    
        del data_path, delimiter, source_lang, target_lang, num_chunks, column_name, translated_column_name, row_start, row_end, write_step, active_logging_minutes, log_interval, hours, remainder, minutes, seconds, data_name, out_path
    except Exception as e:
        print(e)
        gc.collect()