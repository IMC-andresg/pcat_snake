import json
import logging
import os
import time
import pandas
import requests
from datetime import date
from datetime import datetime
from dateutil.relativedelta import relativedelta
from generators.csp_generator import CSPGenerator
from generators.sw_generator import SWGenerator
from loaders.past_months_loader import PastMonthsLoader

logging.basicConfig(filename="snake_{0}.log".format(time.strftime("%Y%m%d-%H%M%S")), level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logging.info("Starting Snake!")

# Read config file
with open('config.json') as json_data_file:
	config = json.load(json_data_file)

# Load Connect items
logging.info("Loading Connect items")
cached_connect_items = f'./cache/connectitems_{date.today().strftime("%Y%m%d")}.pkl'
if os.path.exists(cached_connect_items):
	logging.info("Loading Connect items from Cache")
	connect_items = pandas.read_pickle(cached_connect_items)
else:
	connect_items = pandas.DataFrame()
	headers = {'Authorization': config['CONNECT_TOKEN']}
	for product in config['CONNECT_PRODUCTS']:
		url = config['CONNECT_URLS']['PRODUCT'].replace('<PRODUCT_ID>', product)
		connect_product = requests.get(url, headers=headers).json()
		url = config['CONNECT_URLS']['ITEMS'].replace('<PRODUCT_ID>', product)
		offset = 0
		params = {'limit': config['CONNECT_PARAMS']['LIMIT'], 'offset': offset}
		response = requests.get(url, headers=headers, params=params).json()
		while response:
			for item_in_product in response:
				sku = item_in_product['mpn'][:item_in_product['mpn'].find('_')].lower() if '_' in item_in_product['mpn'] else item_in_product['mpn'].lower()
				if sku not in connect_items.index:
					connect_items.loc[sku + '_' + config['CONNECT_PRODUCTS'][product]['License type'], 'Connect product'] = product
					connect_items.loc[sku + '_' + config['CONNECT_PRODUCTS'][product]['License type'], 'Product name'] = connect_product['name']
					connect_items.loc[sku + '_' + config['CONNECT_PRODUCTS'][product]['License type'], 'Product category'] = connect_product['category']['name']
				if item_in_product['period'] == config['DEFAULT_VALUES']['YEARLY']:
					connect_items.loc[sku + '_' + config['CONNECT_PRODUCTS'][product]['License type'], 'Annual name'] = item_in_product['name']
					connect_items.loc[sku + '_' + config['CONNECT_PRODUCTS'][product]['License type'], 'Annual item'] = item_in_product['id']
				elif item_in_product['period'] == config['DEFAULT_VALUES']['MONTHLY']:
					connect_items.loc[sku + '_' + config['CONNECT_PRODUCTS'][product]['License type'], 'Monthly name'] = item_in_product['name']
					connect_items.loc[sku + '_' + config['CONNECT_PRODUCTS'][product]['License type'], 'Monthly item'] = item_in_product['id']
				else:
					connect_items.loc[sku + '_' + config['CONNECT_PRODUCTS'][product]['License type'], 'Onetime name'] = item_in_product['name']
					connect_items.loc[sku + '_' + config['CONNECT_PRODUCTS'][product]['License type'], 'Onetime item'] = item_in_product['id']
			offset = offset + config['CONNECT_PARAMS']['LIMIT']
			params = {'limit': config['CONNECT_PARAMS']['LIMIT'], 'offset': offset}
			response = requests.get(url, headers=headers, params=params).json()
	connect_items.to_pickle(cached_connect_items)

loader = PastMonthsLoader(config)
loader.init_paths()
loader.load_files()

CSPGenerator(config, loader, connect_items).generate()
SWGenerator(config, loader, connect_items).generate()

logging.info("Snake Completed Successfully!")