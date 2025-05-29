import pandas as pd
from torch.utils.data import Dataset

import logging

class TranslateDataset(Dataset):
    def __init__(self, model,
                 data:pd.DataFrame,
                 source_lang:str, target_lang:str,
                 logger,
                 column_name:str='text',translated_column_name:str='translated_text'):
        self.data = data
        self.column_name = column_name
        self.source_lang = source_lang
        self.target_lang = target_lang
        self.translated_column_name = translated_column_name
        self.model = model  
        self.logger = logger
              
    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        out = self.data.iloc[idx].to_dict()
        out[self.translated_column_name]=self._translate(out[self.column_name])
        return out
    
    def _translate(self,text):
        if not isinstance(text, (str, list)):
            self.logger.error(f'Invalid type for translation: {type(text)}, value: {text}')
            return ""
        try:
            translated_text = self.model.translate(text,target_lang=self.target_lang, source_lang = self.source_lang)
        except Exception as e:
            self.logger.exception(f'Translation failed - {type(e)}: {e.args}')
            return ""
        return translated_text
