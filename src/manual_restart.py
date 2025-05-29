from translate import translate
from easynmt import EasyNMT
if __name__ == '__main__':
    source_lang = "pl"
    target_lang = "en"
    index = 26
    data_name = 'Szejm_scraped_2011-2023_bábelre_second'
    data_path = f'../data/tmp_{data_name}/chunks/chunk_{index}.csv'
    row_start = 7000+1773 #the same as available now 22 1631
    row_end = -1 #the result will be 5001
    
    translate(
        EasyNMT('opus-mt'),
        data_name=data_name,
        data_path=data_path,
        out_path= f'../data/tmp_{data_name}/out/out_{index}.csv',
        source_lang=source_lang,
        target_lang=target_lang,
        delimiter = ',',
        num_workers = 1,
        write_step = 1,
        column_name = 'text',
        translated_column_name = 'translated_text',
        row_start = row_start,
        row_end = row_end,
        log_interval=2,
        no_header = True,
        batch_state = 0
    )
    
    print('finished')