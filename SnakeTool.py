import json
import logging
import time
from generators.csp_generator import CSPGenerator
from generators.sw_generator import SWGenerator
from loaders.connect_items_loader import ConnectItemsLoader
from loaders.past_months_loader import PastMonthsLoader

logging.basicConfig(filename="snake_{0}.log".format(time.strftime("%Y%m%d-%H%M%S")), level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logging.info("Starting Snake!")

# Read config file
with open('config.json') as json_data_file:
	config = json.load(json_data_file)

loader = PastMonthsLoader(config).load_files()
connect_items = ConnectItemsLoader.load(config)
CSPGenerator(config, loader, connect_items).generate()
SWGenerator(config, loader, connect_items).generate()

logging.info("Snake Completed Successfully!")