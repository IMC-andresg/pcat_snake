import requests
import pandas as pd
import os.path
from os import path
import logging
import sys

BASE_URL = 'https://api-free.deepl.com/v2/translate'
DEFAULT_SOURCE_LANG = 'EN'

class DeepLTranslator:
    GERMAN = 'DE'
    SPANISH = 'ES'
    FRENCH = 'FR'
    ITALIAN = 'IT'
    PORTUGUESE = 'PT'

    def __init__(self, config):
        self.auth_key = config['DEEPL_AUTH_KEY']

    def translate(self, text, target_lang):
        try:
            params = {
                'auth_key': self.auth_key,
                'text': text,
                'source_lang': DEFAULT_SOURCE_LANG,
                'target_lang': target_lang
                }
            response = requests.post(BASE_URL, data=params).json()
            return response['translations'][0]['text']
        except:
            logging.error("Error translating text: ", sys.exc_info()[0])
            return "Translation Error"

