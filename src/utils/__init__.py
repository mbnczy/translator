import os
import requests

import gc

def is_model_available(source_lang:str, target_lang:str, logger):
    url = f"https://huggingface.co/api/models/Helsinki-NLP/opus-mt-{source_lang}-{target_lang}/commits/main"
    response = requests.get(url)
    
    if response.status_code == 200:
        commit_hash = response.json()[0]['id']
        del response
        logger.info(f'🤗 model is available 🤗')
        gc.collect()
        return True, url[:-13], commit_hash
    else:
        logger.warning(f'🤗 model is not available {url}')
        del url, response
        gc.collect()
        return False, "", ""