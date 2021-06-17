"""
This script is used to translate product description for Microsoft products using DeepL API.

USAGE:
1. Install required dependencies using pip install: requests, argparse, pandas
2. Prepare DeepL API key. You will pass it as a command line arguement
3. Prepare input file. See deepl_translator_sample_input.xlsx for an example input file.
4. Run the tool: python deepl_translator.py -i input.xlsx -o output.xlsx -apikey API-KEY
"""

import requests
import pandas as pd
import os.path
from os import path
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-i", "--input", dest="inputfile", required=True,
                    help="input file with strings to translate")
parser.add_argument("-o", "--output", dest="outputfile", required=True,
                    help="output file")
parser.add_argument("-apikey", "--apikey", dest="apikey", required=True,
                    help="DeepL API key")
args = parser.parse_args()

TRANS_COL = 'SKU Description - Translation'
SOURCE_COL = 'SKU Description - EN'

SOURCE_FILE = args.inputfile
OUTPUT_FILE = args.outputfile

BASE_URL = 'https://api-free.deepl.com/v2/translate'
AUTH_KEY = args.apikey

LANGS = [
    {'label':'German','key':'DE'},
    {'label':'Spanish','key':'ES'},
    {'label':'French','key':'FR'},
    {'label':'Italian','key':'IT'},
    {'label':'Portuguese','key':'PT'}
]

def call_deepl(text, target_lang):
    params = {
        'auth_key': AUTH_KEY,
        'text': text,
        'source_lang': 'EN',
        'target_lang': target_lang
        }
    response = requests.post(BASE_URL, data=params).json()
    return response['translations'][0]['text']

def translate_row(row, lang):
    if pd.isnull(row[TRANS_COL]):
        print('#START')
        print('#Translating: '+ row[SOURCE_COL])
        print('#To: '+ lang)
        trans = call_deepl(row[SOURCE_COL], lang)
        print('#Translation: '+ trans)
        print('#DONE')
        return trans
    else:
        return row[TRANS_COL]

for lang in LANGS:
    sheet = lang['label']
    target_lang = lang['key']
    df = pd.read_excel(SOURCE_FILE, sheet_name=sheet, index_col=0)
    df[TRANS_COL] = df.apply(translate_row, args=(target_lang,), axis=1)
    if os.path.exists(OUTPUT_FILE):
        f_mode = 'a'
    else:
        f_mode = 'w'
    with pd.ExcelWriter(OUTPUT_FILE, mode=f_mode) as writer:  
        df.to_excel(writer,sheet_name=sheet)
