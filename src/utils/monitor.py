import os
import time
import gc

import re

import queue

def read_last_line_with_processing(log_file_path):
    with open(log_file_path, 'rb') as f:
        try:
            f.seek(-2, os.SEEK_END)
            while True:
                try:
                    f.seek(-2, os.SEEK_CUR)
                except OSError:
                    f.seek(0)
                    break
                if f.read(1) == b'\n':
                    line = f.readline().decode()
                    if 'processing' in line:
                        return line
                f.seek(-2, os.SEEK_CUR)
        except OSError:
            f.seek(0)
            line = f.readline().decode()
            if 'processing' in line:
                return line
    return None

def read_last_line(log_file_path):
    with open(log_file_path, 'rb') as f:
        try:
            f.seek(-2, os.SEEK_END)
            while f.read(1) != b'\n':
                f.seek(-2, os.SEEK_CUR)
        except OSError:
            f.seek(0)
        return f.readline().decode()

def check_all_successful(sublog_files,logger):
    for sublog_file in sublog_files:
        if os.path.exists(sublog_file):
            if not read_last_line(sublog_file).startswith("-"):
                return False
        else:
            return False
    return True

def active_logging(
    sublog_files,
    interval_minutes,
    logger,
    futures,
    output_queue,
    patience = 3
    ):
    
    sorted_sublog_files = sorted(sublog_files, key=lambda x: int(str(os.path.basename(x)).split('.')[0].split('_')[1]))
    
    patience_list = []
    for index in range(len(sublog_files)):
        patience_list.append(0)
        
    while True:
        time.sleep(interval_minutes * 60)
        report_str = 'feedback report'
        frozen = False
        for sublog_file, future in zip(sorted_sublog_files, futures):
            if os.path.exists(sublog_file):
                last_line = read_last_line(sublog_file)  
                index = int(sublog_file.split('/')[-1].split('.')[0].split('_')[1])
                if 'processing batches' in last_line:
                    if ((future.cancelled() or future.done()) and 
                        last_line.split('processing batches:')[1][:5] != '100%'):
                        
                        patience_list[index]+=1
                        
                        report_str += f"\n{index}: {last_line.split('processing batches:')[1][:5]} 🧊 patience: {patience_list[index]}/{patience}"
                        #restart batch after patience rounds
                        if patience_list[index]>=patience:
                            patience_list[index] = 0
                            try:
                                lines_ready = int(re.search(r'lines ready:\s*(\d+)/\d+', last_line).group(1)) #int(last_line.split('lines ready: ')[1].split('/')[0].strip()),
                                batches_ready = int(re.search(r'\|\s*(\d+)/\d+\s*\[', last_line).group(1))
                                
                            except:
                                #logger.exception(f'🚨 Could not restart batch {index}.')
                                lines_ready = 0
                                batches_ready = 0
                            output_queue.put(
                                    (
                                        index,
                                        lines_ready,
                                        batches_ready
                                    )
                                )
                        frozen = True
                    else:
                        report_str += f"\n{index}: {last_line.split('processing batches:')[1][:5]} 🫧"
                elif 'continue' in last_line:
                    last_line = read_last_line_with_processing(sublog_file)
                    if ((future.cancelled() or future.done()) and 
                        last_line.split('processing batches:')[1][:5] != '100%'):
                        
                        patience_list[index]+=1
                        
                        report_str += f"\n{index}: {last_line.split('processing batches:')[1][:5]} 🧊 patience: {patience_list[index]}/{patience}"
                        #restart batch after patience rounds
                        if patience_list[index]>=patience:
                            patience_list[index] = 0
                            try:
                                lines_ready = int(re.search(r'lines ready:\s*(\d+)/\d+', last_line).group(1)) #int(last_line.split('lines ready: ')[1].split('/')[0].strip()),
                                batches_ready = int(re.search(r'\|\s*(\d+)/\d+\s*\[', last_line).group(1))
                                
                            except:
                                #logger.exception(f'🚨 Could not restart batch {index}.')
                                lines_ready = 0
                                batches_ready = 0
                            output_queue.put(
                                    (
                                        index,
                                        lines_ready,
                                        batches_ready
                                    )
                                )
                        frozen = True
                    else:
                        report_str += f"\n{index}:       🧼"
                elif 'runtime' in last_line:
                    report_str += f"\n{index}:  100% ✨"
                del last_line
            else:
                report_str += f"\n{sublog_file.split('/')[-1].split('.')[0].split('_')[1]}: not exists 🚨"
        if frozen:
            logger.warning(report_str)
        else:
            logger.info(report_str)
        if check_all_successful(sublog_files,logger):
            logger.info("stopping active logging")
            break
    del sublog_files, interval_minutes, report_str
    gc.collect()
    