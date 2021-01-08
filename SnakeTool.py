import json
import logging
import os
import time
from pathlib import Path

import pandas
import requests
import tabulate
from tinydb import Query, TinyDB

# Load local db and logging 
db = TinyDB('db.json')
logging.basicConfig(filename="snake_{0}.log".format(time.strftime("%Y%m%d-%H%M%S")), level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

# Read config file ##########################################################################################################################################################################
with open('config.json') as json_data_file:
	config = json.load(json_data_file)
#############################################################################################################################################################################################

# Functions definition ######################################################################################################################################################################

def lookup_sku_group(sku):
	info_query = Query()
	info_db_result = db.search((info_query.sku == sku) & (info_query.group.exists()))
	if info_db_result:
		return info_db_result[0]['group']

def lookup_sku_shortname(sku):
	info_query = Query()
	info_db_result = db.search((info_query.sku == sku) & (info_query.sku_short_name.exists()))
	if info_db_result:
		return info_db_result[0]['sku_short_name']

def save_sku_group(sku, sku_name, group):
	upsert_query = Query()
	db.upsert({'sku': sku, 'sku_name': sku_name, 'group': group}, upsert_query.sku == sku)

def save_sku_shortname(sku, sku_name, sku_short_name):
	upsert_query = Query()
	db.upsert({'sku': sku, 'sku_name': sku_name, 'sku_short_name': sku_short_name}, upsert_query.sku == sku)

def request_info(name, choices=None, sku=None, sku_name=None, sku_pl_two_months_ago=None, sku_pl_last_month=None, sku_pl_current=None, sku_pl_next_month=None, sku_license_type=None):
	if name in ['Group', 'CMP Category', 'Parent Category', 'Sales Category']:
		info = lookup_sku_group(sku)
		if info: return info

		choices.sort()
		info = ""
		while info not in choices:
			# os.system('cls')
			os.system('clear')
			if info != "":
				print("ERROR: Incorrect provided value. Please, select one from the list", end='\n\n')
			print("Enter \"{0}\" value for".format(name), end='\n\n')
			print(tabulate.tabulate([[sku, sku_name]], headers=['SKU', 'Name'], stralign="center"), end='\n\n')
			print(tabulate.tabulate([[sku_license_type, sku_pl_two_months_ago, sku_pl_last_month, sku_pl_current, sku_pl_next_month]], headers=['SKU license type', 'In Two Months Ago PL', 'In Last Month PL', 'In current PL', 'In Next Month PL'], tablefmt="psql", stralign="center"), end='\n\n')
			print("Available options are:\n\n- {0}".format("\n- ".join(choices)), end='\n\n')
			info = input("Selection: ").strip()
		save_sku_group(sku, sku_name, info)
		return info
	elif name in ['Shortened Names']:
		short_name = lookup_sku_shortname(sku)
		if short_name: return short_name

		short_name = ""
		# while short_name == "" or len(short_name) > config['MAX_LENGTH_SHORT_NAME']:
		while short_name == "":
			# os.system('cls')
			os.system('clear')
			# if len(short_name) > config['MAX_LENGTH_SHORT_NAME']:
				# print("ERROR: Incorrect shortened name. It is {0} characters long, but it must be shorter than {1}".format(len(short_name), config['MAX_LENGTH_SHORT_NAME']), end='\n\n')
			print("Enter \"Shortened name\" value for \"{0}\"".format(sku_name), end='\n\n')
			entered_text = input("Shortened name [Default: \"{0}\"]: ".format(sku_name)).strip()
			short_name = sku_name if entered_text == "" else entered_text
		save_sku_shortname(sku, sku_name, short_name)
		return short_name
#############################################################################################################################################################################################


# Load Connect items ########################################################################################################################################################################
logging.info("Loading Connect items")
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
#############################################################################################################################################################################################

# Start DataFrames ##########################################################################################################################################################################
om_current = pandas.read_excel(config['OM_THIS_MONTH'], sheet_name='Office_Dynamics_Windows_Intune', index_col=1)
om_last = pandas.read_excel(config['OM_LAST_MONTH'], sheet_name='OM', index_col=0)
om_two_months = pandas.read_excel(config['OM_TWO_MONTHS'], sheet_name='OM', index_col=0)
om_new = pandas.DataFrame(columns=config['OM_HEADERS'])
ghost_file = Path(config['OM_OUTPUT'])
if ghost_file.is_file():
	om_ghost = pandas.read_excel(config['OM_OUTPUT'], sheet_name='OM', index_col=0)
else:
	om_ghost = pandas.DataFrame()

pl_current = pandas.read_excel(config['PL_THIS_MONTH'], sheet_name='USD', index_col=4)
pl_next_month_preview = pandas.read_excel(config['PL_NEXT_MONTH_PREVIEW'], sheet_name='USD', index_col=4)
pl_last = pandas.read_excel(config['PL_LAST_MONTH'], sheet_name='USD', index_col=4)
pl_two_months = pandas.read_excel(config['PL_TWO_MONTHS'], sheet_name='USD', index_col=4)

relations_last = pandas.read_excel(config['OM_LAST_MONTH'], sheet_name='RM', index_col=0)
relations_two_months = pandas.read_excel(config['OM_TWO_MONTHS'], sheet_name='RM', index_col=0)
relations_current = pandas.DataFrame(columns=config['APS_RELATIONS_HEADERS'])
relations_current_connect = pandas.DataFrame(columns=config['CONNECT_RELATIONS_HEADERS'])
relations_last_connect = pandas.read_excel(config['OM_LAST_MONTH'], sheet_name='RM Connect', index_col=0)
relations_two_months_connect = pandas.read_excel(config['OM_TWO_MONTHS'], sheet_name='RM Connect', index_col=0)
relations_current_addons2addons = pandas.DataFrame()

upgrades_current = pandas.DataFrame(columns=config['UPGRADES_HEADERS'])

countries_last = pandas.read_excel(config['OM_LAST_MONTH'], sheet_name='CM', index_col=0)
countries_current = pandas.DataFrame(columns=config['COUNTRIES_HEADERS'])
country_availability = pandas.DataFrame(columns=config['COUNTRY_AVAIABILITY_HEADERS'] + config['T27'] + config['SERVICE_PROVIDER_COUNTRIES'])

om_add_this_month = pandas.DataFrame(columns=config['OM_ADD_THIS_MONTH_HEADERS'])
om_del_this_month = pandas.DataFrame(columns=config['OM_DEL_THIS_MONTH_HEADERS'])
om_name_changes_this_month = pandas.DataFrame(columns=config['OM_NAME_CHANGES_THIS_MONTH_HEADERS'])
om_microsoft_errors = pandas.DataFrame(columns=config['OM_MICROSOFT_ERRORS_HEADERS'])

new_skus_not_in_pl = pandas.DataFrame()
#############################################################################################################################################################################################

# Update last month OM to update this month's deletes #######################################################################################################################################
for sku, sku_data in om_last.iterrows():
	om_last.loc[sku, 'In Next Month OM'] = config['DEFAULT_VALUES']['YES'] if sku in om_current.index else config['DEFAULT_VALUES']['NO']
#############################################################################################################################################################################################

# Generate this month's OM ##################################################################################################################################################################
logging.info("Generating this month's OM")
for sku, sku_data in om_current.iterrows():
	om_new.loc[sku, 'In Two Months Ago OM'] = config['DEFAULT_VALUES']['YES'] if sku in om_two_months.index else config['DEFAULT_VALUES']['NO']
	om_new.loc[sku, 'Manually Added'] = config['DEFAULT_VALUES']['NO']
	om_new.loc[sku, 'Offer Display Name'] = sku_data['Offer Display Name']
	om_new.loc[sku, 'Provisioning ID'] = sku_data['Provisioning ID']
	om_new.loc[sku, 'Parent/Child'] = "Parent" if pandas.isna(sku_data['Depends On']) else "Child"
	om_new.loc[sku, 'Offer Type'] = sku_data['Offer Type']
	om_new.loc[sku, 'Duration'] = sku_data['Duration']
	om_new.loc[sku, 'Billing Frequency'] = sku_data['Billing Frequency']
	om_new.loc[sku, 'Min Seat Count'] = sku_data['Min Seat Count']
	om_new.loc[sku, 'Max Seat Count'] = sku_data['Max Seat Count']
	om_new.loc[sku, 'Offer Limit'] = sku_data['Offer Limit']
	om_new.loc[sku, 'Offer Limit Scope'] = sku_data['Offer Limit Scope']
	om_new.loc[sku, 'Depends On'] = sku_data['Depends On']
	om_new.loc[sku, 'Can Convert To'] = sku_data['Can Convert To']
	om_new.loc[sku, 'Offer URI'] = sku_data['Offer URI']
	om_new.loc[sku, 'LearnMoreLink'] = sku_data['LearnMoreLink']
	om_new.loc[sku, 'Offer Display Description'] = sku_data['Offer Display Description']
	om_new.loc[sku, 'Allowed Countries'] = sku_data['Allowed Countries']
	om_new.loc[sku, 'GUID + Offer Name'] = sku + sku_data['Offer Display Name']
	om_new.loc[sku, 'In current PL'] = pl_current.loc[sku, 'A/C/D/U'] if sku in pl_current.index else config['DEFAULT_VALUES']['NO']
	om_new.loc[sku, 'In Next Month PL'] = pl_next_month_preview.loc[sku, 'A/C/D/U'] if sku in pl_next_month_preview.index else config['DEFAULT_VALUES']['NO']
	om_new.loc[sku, 'In Last Month PL'] = pl_last.loc[sku, 'A/C/D/U'] if sku in pl_last.index else config['DEFAULT_VALUES']['NO']
	om_new.loc[sku, 'In Two Months Ago PL'] = pl_two_months.loc[sku, 'A/C/D/U'] if sku in pl_two_months.index else config['DEFAULT_VALUES']['NO']
	if sku in pl_current.index:
		connect_sku_with_license_type = sku.lower() + '_' + pl_current.loc[sku, 'License Agreement Type']
	elif sku in om_last.index:
		connect_sku_with_license_type = sku.lower() + '_' + om_last.loc[sku, 'License Type']
	else:
		new_skus_not_in_pl.loc[sku, 'Offer Display Name'] = sku_data['Offer Display Name']
	if connect_sku_with_license_type in connect_items.index:
		om_new.loc[sku, 'Connect Product Id'] = connect_items.loc[connect_sku_with_license_type, 'Connect product']
		om_new.loc[sku, 'Connect product name'] = connect_items.loc[connect_sku_with_license_type, 'Product name']
		om_new.loc[sku, 'Connect product category'] = connect_items.loc[connect_sku_with_license_type, 'Product category']
		if pandas.notna(connect_items.loc[connect_sku_with_license_type, 'Annual item']):
			om_new.loc[sku, 'Connect annual item Id'] = connect_items.loc[connect_sku_with_license_type, 'Annual item']
		if pandas.notna(connect_items.loc[connect_sku_with_license_type, 'Annual name']):
			om_new.loc[sku, 'Connect annual item name'] = connect_items.loc[connect_sku_with_license_type, 'Annual name']
		if pandas.notna(connect_items.loc[connect_sku_with_license_type, 'Monthly item']):
			om_new.loc[sku, 'Connect monthly item Id'] = connect_items.loc[connect_sku_with_license_type, 'Monthly item']
		if pandas.notna(connect_items.loc[connect_sku_with_license_type, 'Monthly name']):
			om_new.loc[sku, 'Connect monthly item name'] = connect_items.loc[connect_sku_with_license_type, 'Monthly name']

	if sku in om_last.index and om_last.loc[sku, 'Group'] != config['DEFAULT_VALUES']['MICROSOFT_ERROR']:
		om_new.loc[sku, 'In Last Month OM'] = config['DEFAULT_VALUES']['YES']
		om_new.loc[sku, 'Group'] = om_last.loc[sku, 'Group']
		om_new.loc[sku, 'CMP Category'] = om_last.loc[sku, 'CMP Category']
		om_new.loc[sku, 'Parent Category'] = om_last.loc[sku, 'Parent Category']
		om_new.loc[sku, 'Sales Category'] = om_last.loc[sku, 'Sales Category']
		om_new.loc[sku, 'Tax Category'] = om_last.loc[sku, 'Tax Category']
		om_new.loc[sku, 'License Type'] = pl_current.loc[sku, 'License Agreement Type'] if sku in pl_current.index else om_last.loc[sku, 'License Type']
		om_new.loc[sku, 'GUID + Offer Name in last month OM'] = config['DEFAULT_VALUES']['YES'] if om_last.loc[sku, 'GUID + Offer Name'] == om_new.loc[sku, 'GUID + Offer Name'] else config['DEFAULT_VALUES']['NO']
		om_new.loc[sku, 'GUID in last month OM'] = config['DEFAULT_VALUES']['YES']
		om_new.loc[sku, 'Shortened Names'] = om_last.loc[sku, 'Shortened Names'] if pandas.notna(om_last.loc[sku, 'Shortened Names']) else ""
		om_new.loc[sku, 'Length'] = len(om_new.loc[sku, 'Shortened Names'])
		om_new.loc[sku, 'Microsoft Change'] = config['DEFAULT_VALUES']['NO']
		for microsoft_field in config['MICROSOFT_LIMITS_TO_VALIDATE'] + config['MICROSOFT_FIELDS_TO_VALIDATE']:
			if om_new.loc[sku, microsoft_field] != om_last.loc[sku, microsoft_field]:
				if om_new.loc[sku, 'Microsoft Change'] == config['DEFAULT_VALUES']['NO']:
					if microsoft_field in config['MICROSOFT_FIELDS_TO_VALIDATE']:
						om_new.loc[sku, 'Microsoft Change'] = '* {}'.format(microsoft_field)
					else:
						om_new.loc[sku, 'Microsoft Change'] = '* {}: Old value: {} - New value: {}'.format(microsoft_field, om_last.loc[sku, microsoft_field], sku_data[microsoft_field])
				else:
					if microsoft_field in config['MICROSOFT_FIELDS_TO_VALIDATE']:
						om_new.loc[sku, 'Microsoft Change'] = om_new.loc[sku, 'Microsoft Change'] + '\n* {0}'.format(microsoft_field)
					else:
						om_new.loc[sku, 'Microsoft Change'] = om_new.loc[sku, 'Microsoft Change'] + '\n* {}: Old value: {} - New value: {}'.format(microsoft_field, om_last.loc[sku, microsoft_field], sku_data[microsoft_field])
	else:
		om_new.loc[sku, 'In Last Month OM'] = config['DEFAULT_VALUES']['NO']
		om_new.loc[sku, 'Microsoft Change'] = config['DEFAULT_VALUES']['NO']
		if sku not in om_ghost.index:
			om_new.loc[sku, 'Group'] = request_info('Group', list(config['SKU_GROUPS'].keys()) + [config['DEFAULT_VALUES']['MICROSOFT_ERROR']], sku, om_new.loc[sku, 'Offer Display Name'], om_new.loc[sku, 'In Two Months Ago PL'], om_new.loc[sku, 'In Last Month PL'], om_new.loc[sku, 'In current PL'], om_new.loc[sku, 'In Next Month PL'], pl_current.loc[sku, 'License Agreement Type'] if sku in pl_current.index else config['DEFAULT_VALUES']['NO'])
			if om_new.loc[sku, 'Group'] in config['SKU_GROUPS']:
				if om_new.loc[sku, 'Group'] == "Trials":
					om_new.loc[sku, 'CMP Category'] = request_info('CMP Category', list(dict.fromkeys(om_last['CMP Category'])), sku, om_new.loc[sku, 'Offer Display Name'], om_new.loc[sku, 'In Two Months Ago PL'], om_new.loc[sku, 'In Last Month PL'], om_new.loc[sku, 'In current PL'], om_new.loc[sku, 'In Next Month PL'], pl_current.loc[sku, 'License Agreement Type'] if sku in pl_current.index else config['DEFAULT_VALUES']['NO'])
					om_new.loc[sku, 'Parent Category'] = request_info('Parent Category', list(dict.fromkeys(om_last['Parent Category'])), sku, om_new.loc[sku, 'Offer Display Name'], om_new.loc[sku, 'In Two Months Ago PL'], om_new.loc[sku, 'In Last Month PL'], om_new.loc[sku, 'In current PL'], om_new.loc[sku, 'In Next Month PL'], pl_current.loc[sku, 'License Agreement Type'] if sku in pl_current.index else config['DEFAULT_VALUES']['NO'])
					om_new.loc[sku, 'Sales Category'] = request_info('Sales Category', list(dict.fromkeys(om_last['Sales Category'])), sku, om_new.loc[sku, 'Offer Display Name'], om_new.loc[sku, 'In Two Months Ago PL'], om_new.loc[sku, 'In Last Month PL'], om_new.loc[sku, 'In current PL'], om_new.loc[sku, 'In Next Month PL'], pl_current.loc[sku, 'License Agreement Type'] if sku in pl_current.index else config['DEFAULT_VALUES']['NO'])
				else:
					om_new.loc[sku, 'CMP Category'] = config['SKU_GROUPS'][om_new.loc[sku, 'Group']]['CMP Category'] if str(om_new.loc[sku, 'Parent/Child']) == "Parent" else "ADDON"
					om_new.loc[sku, 'Parent Category'] = config['SKU_GROUPS'][om_new.loc[sku, 'Group']]['Parent Category']
					om_new.loc[sku, 'Sales Category'] = config['SKU_GROUPS'][om_new.loc[sku, 'Group']]['Sales Category'] if str(om_new.loc[sku, 'Parent/Child']) == "Parent" else "ADDON"
				om_new.loc[sku, 'License Type'] = pl_current.loc[sku, 'License Agreement Type'] if sku in pl_current.index else request_info('License Type', list(dict.fromkeys(om_last['License Type'])), sku, om_new.loc[sku, 'Offer Display Name'], om_new.loc[sku, 'In Two Months Ago PL'], om_new.loc[sku, 'In Last Month PL'], om_new.loc[sku, 'In current PL'], om_new.loc[sku, 'In Next Month PL'])
				om_new.loc[sku, 'Tax Category'] = config['CSP_TAX_CATEGORY']
				om_new.loc[sku, 'GUID + Offer Name in last month OM'] = config['DEFAULT_VALUES']['NO']
				om_new.loc[sku, 'GUID in last month OM'] = config['DEFAULT_VALUES']['NO']
				om_new.loc[sku, 'Shortened Names'] = request_info('Shortened Names', sku=sku, sku_name=om_new.loc[sku, 'Offer Display Name'])
				om_new.loc[sku, 'Length'] = len(om_new.loc[sku, 'Shortened Names'])
			elif om_new.loc[sku, 'Group'] == config['DEFAULT_VALUES']['MICROSOFT_ERROR']:
				om_new.loc[sku, 'Parent Category'] = config['DEFAULT_VALUES']['MICROSOFT_ERROR']
				om_new.loc[sku, 'License Type'] = config['DEFAULT_VALUES']['MICROSOFT_ERROR']
				om_new.loc[sku, 'CMP Category'] = config['DEFAULT_VALUES']['MICROSOFT_ERROR']
				om_new.loc[sku, 'Sales Category'] = config['DEFAULT_VALUES']['MICROSOFT_ERROR']
		else:
			om_new.loc[sku, 'Group'] = om_ghost.loc[sku, 'Group']
			om_new.loc[sku, 'CMP Category'] = om_ghost.loc[sku, 'CMP Category']
			om_new.loc[sku, 'Parent Category'] = om_ghost.loc[sku, 'Parent Category']
			om_new.loc[sku, 'Sales Category'] = om_ghost.loc[sku, 'Sales Category']
			om_new.loc[sku, 'License Type'] = om_ghost.loc[sku, 'License Type']
			om_new.loc[sku, 'Tax Category'] = om_ghost.loc[sku, 'Tax Category']
			om_new.loc[sku, 'GUID + Offer Name in last month OM'] = om_ghost.loc[sku, 'GUID + Offer Name in last month OM']
			om_new.loc[sku, 'GUID in last month OM'] = om_ghost.loc[sku, 'GUID in last month OM']
			om_new.loc[sku, 'Shortened Names'] = om_ghost.loc[sku, 'Shortened Names']
			om_new.loc[sku, 'Length'] = om_ghost.loc[sku, 'Length']

	if om_new.loc[sku, 'GUID + Offer Name in last month OM'] == config['DEFAULT_VALUES']['NO'] and om_new.loc[sku, 'GUID in last month OM'] == config['DEFAULT_VALUES']['YES']:
		om_new.loc[sku, 'Name Change'] = config['DEFAULT_VALUES']['YES']
		om_new.loc[sku, 'Old Name'] = om_last.loc[sku, 'Offer Display Name']
	else:
		om_new.loc[sku, 'Name Change'] = config['DEFAULT_VALUES']['NO']

for sku_in_last_not_in_current, sku_in_last_not_in_current_data in om_last.iterrows():
	if sku_in_last_not_in_current not in om_new.index and sku_in_last_not_in_current in pl_current.index and pl_current.loc[sku_in_last_not_in_current, 'A/C/D/U'] != 'DEL':
		om_last.loc[sku_in_last_not_in_current, 'In Next Month OM'] = config['DEFAULT_VALUES']['YES']
		om_new.loc[sku_in_last_not_in_current, 'In Two Months Ago OM'] = config['DEFAULT_VALUES']['YES'] if sku_in_last_not_in_current in om_two_months.index else config['DEFAULT_VALUES']['NO']
		om_new.loc[sku_in_last_not_in_current, 'In Last Month OM'] = config['DEFAULT_VALUES']['YES']
		om_new.loc[sku_in_last_not_in_current, 'Offer Display Name'] = om_last.loc[sku_in_last_not_in_current, 'Offer Display Name']
		om_new.loc[sku_in_last_not_in_current, 'Provisioning ID'] = om_last.loc[sku_in_last_not_in_current, 'Provisioning ID']
		om_new.loc[sku_in_last_not_in_current, 'Manually Added'] = config['DEFAULT_VALUES']['NO']
		om_new.loc[sku_in_last_not_in_current, 'Group'] = om_last.loc[sku_in_last_not_in_current, 'Group']
		om_new.loc[sku_in_last_not_in_current, 'Parent/Child'] = om_last.loc[sku_in_last_not_in_current, 'Parent/Child']
		om_new.loc[sku_in_last_not_in_current, 'CMP Category'] = om_last.loc[sku_in_last_not_in_current, 'CMP Category']
		om_new.loc[sku_in_last_not_in_current, 'Parent Category'] = om_last.loc[sku_in_last_not_in_current, 'Parent Category']
		om_new.loc[sku_in_last_not_in_current, 'Sales Category'] = om_last.loc[sku_in_last_not_in_current, 'Sales Category']
		om_new.loc[sku_in_last_not_in_current, 'Tax Category'] = om_last.loc[sku_in_last_not_in_current, 'Tax Category']
		om_new.loc[sku_in_last_not_in_current, 'Offer Type'] = om_last.loc[sku_in_last_not_in_current, 'Offer Type']
		om_new.loc[sku_in_last_not_in_current, 'Duration'] = om_last.loc[sku_in_last_not_in_current, 'Duration']
		om_new.loc[sku_in_last_not_in_current, 'Billing Frequency'] = om_last.loc[sku_in_last_not_in_current, 'Billing Frequency']
		om_new.loc[sku_in_last_not_in_current, 'Min Seat Count'] = om_last.loc[sku_in_last_not_in_current, 'Min Seat Count']
		om_new.loc[sku_in_last_not_in_current, 'Max Seat Count'] = om_last.loc[sku_in_last_not_in_current, 'Max Seat Count']
		om_new.loc[sku_in_last_not_in_current, 'Offer Limit'] = om_last.loc[sku_in_last_not_in_current, 'Offer Limit']
		om_new.loc[sku_in_last_not_in_current, 'Offer Limit Scope'] = om_last.loc[sku_in_last_not_in_current, 'Offer Limit Scope']
		om_new.loc[sku_in_last_not_in_current, 'Depends On'] = om_last.loc[sku_in_last_not_in_current, 'Depends On']
		om_new.loc[sku_in_last_not_in_current, 'Offer URI'] = om_last.loc[sku_in_last_not_in_current, 'Offer URI']
		om_new.loc[sku_in_last_not_in_current, 'LearnMoreLink'] = om_last.loc[sku_in_last_not_in_current, 'LearnMoreLink']
		om_new.loc[sku_in_last_not_in_current, 'Offer Display Description'] = om_last.loc[sku_in_last_not_in_current, 'Offer Display Description']
		om_new.loc[sku_in_last_not_in_current, 'Allowed Countries'] = om_last.loc[sku_in_last_not_in_current, 'Allowed Countries']
		om_new.loc[sku_in_last_not_in_current, 'GUID + Offer Name'] = sku_in_last_not_in_current + om_new.loc[sku_in_last_not_in_current, 'Offer Display Name']
		om_new.loc[sku_in_last_not_in_current, 'GUID + Offer Name in last month OM'] = config['DEFAULT_VALUES']['YES']
		om_new.loc[sku_in_last_not_in_current, 'GUID in last month OM'] = config['DEFAULT_VALUES']['YES']
		om_new.loc[sku_in_last_not_in_current, 'Name Change'] = config['DEFAULT_VALUES']['NO']
		om_new.loc[sku_in_last_not_in_current, 'License Type'] = pl_current.loc[sku_in_last_not_in_current, 'License Agreement Type']
		om_new.loc[sku_in_last_not_in_current, 'Shortened Names'] = om_last.loc[sku_in_last_not_in_current, 'Shortened Names']
		om_new.loc[sku_in_last_not_in_current, 'Length'] = len(om_new.loc[sku_in_last_not_in_current, 'Shortened Names'])
		om_new.loc[sku_in_last_not_in_current, 'In current PL'] = pl_current.loc[sku_in_last_not_in_current, 'A/C/D/U']
		om_new.loc[sku_in_last_not_in_current, 'In Next Month PL'] = pl_next_month_preview.loc[sku_in_last_not_in_current, 'A/C/D/U'] if sku_in_last_not_in_current in pl_next_month_preview.index else config['DEFAULT_VALUES']['NO']
		om_new.loc[sku_in_last_not_in_current, 'In Last Month PL'] = pl_last.loc[sku_in_last_not_in_current, 'A/C/D/U'] if sku_in_last_not_in_current in pl_last.index else config['DEFAULT_VALUES']['NO']
		om_new.loc[sku_in_last_not_in_current, 'In Two Months Ago PL'] = pl_two_months.loc[sku_in_last_not_in_current, 'A/C/D/U'] if sku_in_last_not_in_current in pl_two_months.index else config['DEFAULT_VALUES']['NO']
		om_new.loc[sku_in_last_not_in_current, 'Microsoft Change'] = config['DEFAULT_VALUES']['NO']

for manual_sku, manual_sku_data in om_last[om_last['Manually Added'] == config['DEFAULT_VALUES']['YES']].iterrows():
	if manual_sku not in om_new.index:
		om_last.loc[manual_sku, 'In Next Month OM'] = config['DEFAULT_VALUES']['YES']
		om_new.loc[manual_sku, 'In Two Months Ago OM'] = config['DEFAULT_VALUES']['YES'] if manual_sku in om_two_months.index else config['DEFAULT_VALUES']['NO']
		om_new.loc[manual_sku, 'In Last Month OM'] = config['DEFAULT_VALUES']['YES']
		om_new.loc[manual_sku, 'Offer Display Name'] = manual_sku_data['Offer Display Name']
		om_new.loc[manual_sku, 'Provisioning ID'] = manual_sku_data['Provisioning ID']
		om_new.loc[manual_sku, 'Manually Added'] = config['DEFAULT_VALUES']['YES']
		om_new.loc[manual_sku, 'Group'] = manual_sku_data['Group']
		om_new.loc[manual_sku, 'Parent/Child'] = manual_sku_data['Parent/Child']
		om_new.loc[manual_sku, 'CMP Category'] = manual_sku_data['CMP Category']
		om_new.loc[manual_sku, 'Parent Category'] = manual_sku_data['Parent Category']
		om_new.loc[manual_sku, 'Sales Category'] = manual_sku_data['Sales Category']
		om_new.loc[manual_sku, 'Tax Category'] = manual_sku_data['Tax Category']
		om_new.loc[manual_sku, 'Offer Type'] = manual_sku_data['Offer Type']
		om_new.loc[manual_sku, 'Duration'] = manual_sku_data['Duration']
		om_new.loc[manual_sku, 'Billing Frequency'] = manual_sku_data['Billing Frequency']
		om_new.loc[manual_sku, 'Min Seat Count'] = manual_sku_data['Min Seat Count']
		om_new.loc[manual_sku, 'Max Seat Count'] = manual_sku_data['Max Seat Count']
		om_new.loc[manual_sku, 'Offer Limit'] = manual_sku_data['Offer Limit']
		om_new.loc[manual_sku, 'Offer Limit Scope'] = manual_sku_data['Offer Limit Scope']
		om_new.loc[manual_sku, 'Depends On'] = manual_sku_data['Depends On']
		om_new.loc[manual_sku, 'Offer URI'] = manual_sku_data['Offer URI']
		om_new.loc[manual_sku, 'LearnMoreLink'] = manual_sku_data['LearnMoreLink']
		om_new.loc[manual_sku, 'Offer Display Description'] = manual_sku_data['Offer Display Description']
		om_new.loc[manual_sku, 'Allowed Countries'] = manual_sku_data['Allowed Countries']
		om_new.loc[manual_sku, 'GUID + Offer Name'] = manual_sku + om_new.loc[manual_sku, 'Offer Display Name']
		om_new.loc[manual_sku, 'GUID + Offer Name in last month OM'] = config['DEFAULT_VALUES']['YES'] if manual_sku_data['GUID + Offer Name'] == om_new.loc[manual_sku, 'GUID + Offer Name'] else config['DEFAULT_VALUES']['NO']
		om_new.loc[manual_sku, 'GUID in last month OM'] = config['DEFAULT_VALUES']['YES']
		om_new.loc[manual_sku, 'Name Change'] = config['DEFAULT_VALUES']['NO']
		om_new.loc[manual_sku, 'License Type'] = manual_sku_data['License Type']
		om_new.loc[manual_sku, 'Shortened Names'] = manual_sku_data['Shortened Names']
		om_new.loc[manual_sku, 'Length'] = len(om_new.loc[manual_sku, 'Shortened Names'])
		om_new.loc[manual_sku, 'In current PL'] = pl_current.loc[manual_sku, 'A/C/D/U'] if manual_sku in pl_current.index else config['DEFAULT_VALUES']['NO']
		om_new.loc[manual_sku, 'In Next Month PL'] = pl_next_month_preview.loc[manual_sku, 'A/C/D/U'] if manual_sku in pl_next_month_preview.index else config['DEFAULT_VALUES']['NO']
		om_new.loc[manual_sku, 'In Last Month PL'] = pl_last.loc[manual_sku, 'A/C/D/U'] if manual_sku in pl_last.index else config['DEFAULT_VALUES']['NO']
		om_new.loc[manual_sku, 'In Two Months Ago PL'] = pl_two_months.loc[manual_sku, 'A/C/D/U'] if manual_sku in pl_two_months.index else config['DEFAULT_VALUES']['NO']
		om_new.loc[manual_sku, 'Microsoft Change'] = config['DEFAULT_VALUES']['NO']
		connect_sku_with_license_type = manual_sku.lower() + '_' + om_last.loc[manual_sku, 'License Type']
		if connect_sku_with_license_type in connect_items.index and pandas.notna(connect_items.loc[connect_sku_with_license_type, 'Annual item']):
			om_new.loc[manual_sku, 'Connect annual item Id'] = connect_items.loc[connect_sku_with_license_type, 'Annual item']
		if connect_sku_with_license_type in connect_items.index and pandas.notna(connect_items.loc[connect_sku_with_license_type, 'Annual name']):
			om_new.loc[manual_sku, 'Connect annual item name'] = connect_items.loc[connect_sku_with_license_type, 'Annual name']
		if connect_sku_with_license_type in connect_items.index and pandas.notna(connect_items.loc[connect_sku_with_license_type, 'Monthly item']):
			om_new.loc[manual_sku, 'Connect monthly item Id'] = connect_items.loc[connect_sku_with_license_type, 'Monthly item']
		if connect_sku_with_license_type in connect_items.index and pandas.notna(connect_items.loc[connect_sku_with_license_type, 'Monthly name']):
			om_new.loc[manual_sku, 'Connect monthly item name'] = connect_items.loc[connect_sku_with_license_type, 'Monthly name']
#############################################################################################################################################################################################

# Generate this month's APS RM  #############################################################################################################################################################
logging.info("Generating this month's APS RM")
for children_sku, children_data in om_new.iterrows():
	if pandas.notna(children_data['Depends On']):
		parents = str(children_data['Depends On']).split(";")
		for parent_sku in parents:
			if parent_sku in om_new.index:
				if om_new.loc[parent_sku, 'Duration'] in config['ALLOWED_DURATIONS'] or config['INCLUDE_LONG_TERM_SKUS']:
					try:
						if om_new.loc[parent_sku, 'Duration'] == children_data['Duration'] and om_new.loc[parent_sku, 'Group'] in config['SKU_GROUPS'] and children_data['License Type'] in config['ALLOWED_RELATIONS'][om_new.loc[parent_sku, 'License Type']]:
							relations_current.loc[parent_sku + children_sku, 'ParentId'] = parent_sku
							relations_current.loc[parent_sku + children_sku, 'Two Months Ago'] = config['DEFAULT_VALUES']['YES'] if (parent_sku + children_sku) in relations_two_months.index else config['DEFAULT_VALUES']['NO']
							relations_current.loc[parent_sku + children_sku, 'ChildId'] = children_sku
							relations_current.loc[parent_sku + children_sku, 'ChildName'] = om_new.loc[children_sku, 'Offer Display Name']
							relations_current.loc[parent_sku + children_sku, 'ChildProvisioningId'] = om_new.loc[children_sku, 'Provisioning ID']
							if (parent_sku + children_sku) in relations_last.index:
								relations_current.loc[parent_sku + children_sku, 'Last Month'] = config['DEFAULT_VALUES']['YES']
								relations_current.loc[parent_sku + children_sku, 'Reasons'] = relations_last.loc[parent_sku + children_sku, 'Reasons']
							else:
								relations_current.loc[parent_sku + children_sku, 'Last Month'] = config['DEFAULT_VALUES']['NO']
							relations_current.loc[parent_sku + children_sku, 'Parent In OM'] = config['DEFAULT_VALUES']['YES']
							relations_current.loc[parent_sku + children_sku, 'Parent Group'] = om_new.loc[parent_sku, 'Group']
							relations_current.loc[parent_sku + children_sku, 'Parent License'] = om_new.loc[parent_sku, 'License Type']
							relations_current.loc[parent_sku + children_sku, 'Parent/Child'] = om_new.loc[parent_sku, 'Parent/Child']
							relations_current.loc[parent_sku + children_sku, 'Parent CMP Category'] = om_new.loc[parent_sku, 'CMP Category']
							relations_current.loc[parent_sku + children_sku, 'Parent Sales Category'] = om_new.loc[parent_sku, 'Sales Category']
							relations_current.loc[parent_sku + children_sku, 'ParentName'] = om_new.loc[parent_sku, 'Offer Display Name']
							relations_current.loc[parent_sku + children_sku, 'ParentProvisioningId'] = om_new.loc[parent_sku, 'Provisioning ID']
							relations_current.loc[parent_sku + children_sku, 'Child License'] = children_data['License Type']
							if relations_current.loc[parent_sku + children_sku, 'Last Month'] == config['DEFAULT_VALUES']['NO']:
								relations_current.loc[parent_sku + children_sku, 'Child Change'] = config['DEFAULT_VALUES']['YES'] if parent_sku in relations_last['ParentId'] else config['DEFAULT_VALUES']['NO']
								relations_current.loc[parent_sku + children_sku, 'Parent Change'] = config['DEFAULT_VALUES']['YES'] if children_sku in relations_last['ChildId'] else config['DEFAULT_VALUES']['NO']
					except:
						logging.error("Error processing relations for parent_sku {0} and child_sku {1}".format(parent_sku, children_sku))

			elif config['EXTENDED_RELATIONS_MATRIX']:
				relations_current.loc[parent_sku + children_sku, 'ParentId'] = parent_sku
				relations_current.loc[parent_sku + children_sku, 'Two Months Ago'] = config['DEFAULT_VALUES']['YES'] if (parent_sku + children_sku) in relations_two_months.index else config['DEFAULT_VALUES']['NO']
				relations_current.loc[parent_sku + children_sku, 'ChildId'] = children_sku
				relations_current.loc[parent_sku + children_sku, 'ChildName'] = children_data['Offer Display Name']
				relations_current.loc[parent_sku + children_sku, 'ChildProvisioningId'] = children_data['Provisioning ID']
				if (parent_sku + children_sku) in relations_last.index:
					relations_current.loc[parent_sku + children_sku, 'Last Month'] = config['DEFAULT_VALUES']['YES']
					relations_current.loc[parent_sku + children_sku, 'Reasons'] = relations_last.loc[parent_sku + children_sku, 'Reasons']
				else:
					relations_current.loc[parent_sku + children_sku, 'Last Month'] = config['DEFAULT_VALUES']['NO']
				relations_current.loc[parent_sku + children_sku, 'Parent In OM'] = config['DEFAULT_VALUES']['NO']
				relations_current.loc[parent_sku + children_sku, 'Parent Group'] = config['DEFAULT_VALUES']['PARENT_NOT_IN_OM']
				relations_current.loc[parent_sku + children_sku, 'Parent License'] = config['DEFAULT_VALUES']['PARENT_NOT_IN_OM']
				relations_current.loc[parent_sku + children_sku, 'Parent/Child'] = config['DEFAULT_VALUES']['PARENT_NOT_IN_OM']
				relations_current.loc[parent_sku + children_sku, 'Parent CMP Category'] = config['DEFAULT_VALUES']['PARENT_NOT_IN_OM']
				relations_current.loc[parent_sku + children_sku, 'Parent Sales Category'] = config['DEFAULT_VALUES']['PARENT_NOT_IN_OM']
				relations_current.loc[parent_sku + children_sku, 'ParentName'] = config['DEFAULT_VALUES']['PARENT_NOT_IN_OM']
				relations_current.loc[parent_sku + children_sku, 'ParentProvisioningId'] = config['DEFAULT_VALUES']['PARENT_NOT_IN_OM']
				relations_current.loc[parent_sku + children_sku, 'Child License'] = config['DEFAULT_VALUES']['PARENT_NOT_IN_OM']
				relations_current.loc[parent_sku + children_sku, 'Parent Change'] = config['DEFAULT_VALUES']['PARENT_NOT_IN_OM']
				relations_current.loc[parent_sku + children_sku, 'Child Change'] = config['DEFAULT_VALUES']['PARENT_NOT_IN_OM']
				if relations_current.loc[parent_sku + children_sku, 'Last Month'] == config['DEFAULT_VALUES']['NO']:
					relations_current.loc[parent_sku + children_sku, 'Child Change'] = config['DEFAULT_VALUES']['YES'] if parent_sku in relations_last['ParentId'] else config['DEFAULT_VALUES']['NO']
					relations_current.loc[parent_sku + children_sku, 'Parent Change'] = config['DEFAULT_VALUES']['YES'] if children_sku in relations_last['ChildId'] else config['DEFAULT_VALUES']['NO']
#############################################################################################################################################################################################

# Remove corporate addons if charity exists #################################################################################################################################################
logging.info("Remove corporate addons if charity exists")
if config['REMOVE_CORPORATE_ADDONS_WHEN_CHARITY_EXISTS']:
	for relation_sku, relation_sku_data in relations_current[(relations_current['Parent License'] == 'Charity') & (relations_current['Child License'] == 'Corporate')].iterrows():
		charity_skus = om_new[om_new['License Type'] == 'Charity']['Offer Display Name'].str.startswith(om_new.loc[relation_sku_data['ChildId'], 'Offer Display Name'])
		for charity_sku, charity_sku_data in charity_skus[charity_skus].iteritems():
			if (relation_sku_data['ParentId'] + charity_sku) in relations_current.index and relation_sku in relations_current.index:
				relations_current = relations_current.drop(index=relation_sku)
#############################################################################################################################################################################################

# Build Connect RM based on the one from MS #################################################################################################################################################
logging.info("Build Connect RM based on the one from MS")
for children_sku, children_data in om_new.iterrows():
	if pandas.notna(children_data['Depends On']):
		parents = str(children_data['Depends On']).split(";")
		for parent_sku in parents:
			if parent_sku in om_new.index:
				if om_new.loc[parent_sku, 'Duration'] in config['ALLOWED_DURATIONS'] or config['INCLUDE_LONG_TERM_SKUS']:
					try:
						if om_new.loc[parent_sku, 'Duration'] == children_data['Duration'] and om_new.loc[parent_sku, 'Group'] in config['SKU_GROUPS'] and children_data['License Type'] in config['ALLOWED_RELATIONS'][om_new.loc[parent_sku, 'License Type']]:
							if om_new.loc[parent_sku, 'Parent/Child'] == 'Parent':
								relations_current_connect.loc[parent_sku + children_sku, 'ParentId'] = parent_sku
								relations_current_connect.loc[parent_sku + children_sku, 'Two Months Ago'] = config['DEFAULT_VALUES']['YES'] if (parent_sku + children_sku) in relations_two_months.index else config['DEFAULT_VALUES']['NO']
								relations_current_connect.loc[parent_sku + children_sku, 'ChildId'] = children_sku
								relations_current_connect.loc[parent_sku + children_sku, 'ChildName'] = om_new.loc[children_sku, 'Offer Display Name']
								relations_current_connect.loc[parent_sku + children_sku, 'Last Month'] = config['DEFAULT_VALUES']['YES'] if (parent_sku + children_sku) in relations_last.index else config['DEFAULT_VALUES']['NO']
								relations_current_connect.loc[parent_sku + children_sku, 'Parent License'] = om_new.loc[parent_sku, 'License Type']
								relations_current_connect.loc[parent_sku + children_sku, 'ParentName'] = om_new.loc[parent_sku, 'Offer Display Name']
								relations_current_connect.loc[parent_sku + children_sku, 'Child License'] = children_data['License Type']
								if relations_current_connect.loc[parent_sku + children_sku, 'Last Month'] == config['DEFAULT_VALUES']['NO']:
									relations_current_connect.loc[parent_sku + children_sku, 'Child Change'] = config['DEFAULT_VALUES']['YES'] if parent_sku in relations_last['ParentId'] else config['DEFAULT_VALUES']['NO']
									relations_current_connect.loc[parent_sku + children_sku, 'Parent Change'] = config['DEFAULT_VALUES']['YES'] if children_sku in relations_last['ChildId'] else config['DEFAULT_VALUES']['NO']
							else:
								relations_current_addons2addons.loc[parent_sku + children_sku, 'ParentId'] = parent_sku
								relations_current_addons2addons.loc[parent_sku + children_sku, 'ChildId'] = children_sku
					except:
						logging.error("Error processing relations for parent_sku {0} and child_sku {1}".format(parent_sku, children_sku))

for addon2addon_sku, addon2addon_data in relations_current_addons2addons.iterrows():
	for relation_sku, relation_sku_data in relations_current_connect[relations_current_connect['ChildId'] == addon2addon_data['ParentId']].iterrows():
		relations_current_connect.loc[relation_sku_data['ParentId'] + addon2addon_data['ChildId'], 'ParentId'] = relation_sku_data['ParentId']
		relations_current_connect.loc[relation_sku_data['ParentId'] + addon2addon_data['ChildId'], 'ChildId'] = addon2addon_data['ChildId']
		relations_current_connect.loc[relation_sku_data['ParentId'] + addon2addon_data['ChildId'], 'ChildName'] = om_new.loc[addon2addon_data['ChildId'], 'Offer Display Name']
		relations_current_connect.loc[relation_sku_data['ParentId'] + addon2addon_data['ChildId'], 'Parent License'] = om_new.loc[relation_sku_data['ParentId'], 'License Type']
		relations_current_connect.loc[relation_sku_data['ParentId'] + addon2addon_data['ChildId'], 'ParentName'] = om_new.loc[relation_sku_data['ParentId'], 'Offer Display Name']
		relations_current_connect.loc[relation_sku_data['ParentId'] + addon2addon_data['ChildId'], 'Child License'] = om_new.loc[addon2addon_data['ChildId'], 'License Type']
		relations_current_connect.loc[relation_sku_data['ParentId'] + addon2addon_data['ChildId'], 'Last Month'] = config['DEFAULT_VALUES']['YES'] if (relation_sku_data['ParentId'] + addon2addon_data['ChildId']) in relations_last.index else config['DEFAULT_VALUES']['NO']
		relations_current_connect.loc[relation_sku_data['ParentId'] + addon2addon_data['ChildId'], 'Two Months Ago'] = config['DEFAULT_VALUES']['YES'] if (relation_sku_data['ParentId'] + addon2addon_data['ChildId']) in relations_two_months.index else config['DEFAULT_VALUES']['NO']
#############################################################################################################################################################################################

# Update last month Relations matrix to update this month's deletes #########################################################################################################################
for relation_sku, relation_sku_data in relations_last.iterrows():
	relations_last.loc[relation_sku, 'In next month'] = config['DEFAULT_VALUES']['YES'] if relation_sku in relations_current.index else config['DEFAULT_VALUES']['NO']
#############################################################################################################################################################################################

# Build UM ##################################################################################################################################################################################
logging.info("Build UM")
for sku, sku_data in om_new[om_new['Parent/Child'] == 'Parent'].sort_values(by='Offer Display Name').iterrows():
	destinations = str(sku_data['Can Convert To']).split(";")
	for destination in destinations:
		if destination in om_new.index:
			upgrades_current.loc[sku + destination, "Offer Id origin"] = sku
			upgrades_current.loc[sku + destination, "Offer Display Name"] = sku_data["Offer Display Name"]
			upgrades_current.loc[sku + destination, "Offer Id destination"] = destination
			upgrades_current.loc[sku + destination, "Can Convert To"] = om_new.loc[destination, "Offer Display Name"]
#############################################################################################################################################################################################

# Generate this month's Country Matrix ######################################################################################################################################################
logging.info("Generating this month's country matrix")
for sku, sku_data in om_new.iterrows():
	countries = str(om_new.loc[sku, 'Allowed Countries']).split(";")
	for country in countries:
		if country in config['T27'] or country in config['SERVICE_PROVIDER_COUNTRIES'] or config['EXTENDED_COUNTRY_MATRIX']:
			countries_current.loc[sku + country, 'Last Month'] = config['DEFAULT_VALUES']['YES'] if (sku + country) in countries_last.index else config['DEFAULT_VALUES']['NO']
			countries_current.loc[sku + country, 'New Offer ID'] = config['DEFAULT_VALUES']['NO'] if sku in list(dict.fromkeys(countries_last['OfferID'])) else config['DEFAULT_VALUES']['YES']
			countries_current.loc[sku + country, 'In OM'] = config['DEFAULT_VALUES']['YES'] if sku in om_new.index else config['DEFAULT_VALUES']['NO']
			countries_current.loc[sku + country, 'New Country'] = config['DEFAULT_VALUES']['YES'] if (countries_current.loc[sku + country, 'New Offer ID'] == config['DEFAULT_VALUES']['NO'] and countries_current.loc[sku + country, 'Last Month'] == config['DEFAULT_VALUES']['NO']) else config['DEFAULT_VALUES']['NO']
			countries_current.loc[sku + country, 'CMP'] = config['DEFAULT_VALUES']['T27'] if country in config['T27'] else config['DEFAULT_VALUES']['OTHER']
			countries_current.loc[sku + country, 'OfferID'] = sku
			countries_current.loc[sku + country, 'OfferName'] = sku_data['Offer Display Name']
			countries_current.loc[sku + country, 'Country'] = country
			countries_current.loc[sku + country, 'Group'] = sku_data['Group']
			countries_current.loc[sku + country, 'Parent/Child'] = sku_data['Parent/Child']
			countries_current.loc[sku + country, 'CMP Category'] = sku_data['CMP Category']
			countries_current.loc[sku + country, 'Parent Category'] = sku_data['Parent Category']
			countries_current.loc[sku + country, 'Sales Category'] = sku_data['Sales Category']
#############################################################################################################################################################################################

# Update last month Country matrix to update this month's deletes ###########################################################################################################################
for country_sku, country_sku_data in countries_last.iterrows():
	if country_sku in countries_current.index:
		countries_last.loc[country_sku, "In Next Month"] = config['DEFAULT_VALUES']['YES']
		countries_last.loc[country_sku, 'Offer Next Month'] = config['DEFAULT_VALUES']['YES']
		countries_last.loc[country_sku, 'Country Change Next Month'] = config['DEFAULT_VALUES']['NO']
	else:
		countries_last.loc[country_sku, "In Next Month"] = config['DEFAULT_VALUES']['NO']
		countries_last.loc[country_sku, 'Offer Next Month'] = config['DEFAULT_VALUES']['YES'] if country_sku_data['OfferID'] in list(dict.fromkeys(countries_current['OfferID'])) else config['DEFAULT_VALUES']['NO']
		countries_last.loc[country_sku, 'Country Change Next Month'] = config['DEFAULT_VALUES']['YES'] if (country_sku_data["In Next Month"] == config['DEFAULT_VALUES']['NO'] and country_sku_data['Offer Next Month'] == config['DEFAULT_VALUES']['YES']) else config['DEFAULT_VALUES']['NO']
#############################################################################################################################################################################################

# Create DataFrame for this month's Adds ####################################################################################################################################################
om_new_filtered = om_new[om_new['In Last Month OM'] == config['DEFAULT_VALUES']['NO']]
for group in config['SKU_GROUPS']:
	for sku, sku_data in om_new_filtered[om_new_filtered['Group'] == group].sort_values(by='Offer Display Name').iterrows():
		om_add_this_month = om_add_this_month.append({"OfferID": sku, "Offer Display Name": sku_data['Offer Display Name'], "Group": group}, ignore_index=True)
#############################################################################################################################################################################################

# Create DataFrame for this month's Deletes #################################################################################################################################################
om_last_filtered = om_last[om_last['In Next Month OM'] == config['DEFAULT_VALUES']['NO']]
for group in config['SKU_GROUPS']:
	for sku, sku_data in om_last_filtered[om_last_filtered['Group'] == group].sort_values(by='Offer Display Name').iterrows():
		om_del_this_month = om_del_this_month.append({"OfferID": sku, "Offer Display Name": sku_data['Offer Display Name'], "Group": group}, ignore_index=True)
#############################################################################################################################################################################################

# Create DataFrame for this month's Name Changes ############################################################################################################################################
om_new_filtered = om_new[om_new['Name Change'] == config['DEFAULT_VALUES']['YES']]
for group in config['SKU_GROUPS']:
	for sku, sku_data in om_new_filtered[om_new_filtered['Group'] == group].sort_values(by='Offer Display Name').iterrows():
		om_name_changes_this_month = om_name_changes_this_month.append({"OfferID": sku, "Offer Display Name": sku_data['Offer Display Name'], "Group": group, "Old Name": sku_data['Old Name']}, ignore_index=True)
#############################################################################################################################################################################################

# Create DataFrame for Microsoft errors #####################################################################################################################################################
om_new_filtered = om_new[om_new['Group'] == config['DEFAULT_VALUES']['MICROSOFT_ERROR']]
for sku, sku_data in om_new_filtered.sort_values(by='Offer Display Name').iterrows():
	om_microsoft_errors = om_microsoft_errors.append({"OfferID": sku, "Offer Display Name": sku_data['Offer Display Name'], "In current PL": sku_data['In current PL'], "In Next Month PL": sku_data['In Next Month PL'], "In Last Month PL": sku_data['In Last Month PL'], "In Two Months Ago PL": sku_data['In Two Months Ago PL']}, ignore_index=True)
#############################################################################################################################################################################################

# Create Microsoft country availability matrix ##############################################################################################################################################
for sku, sku_data in om_new.iterrows():
	country_availability.loc[sku, 'Offer Display Name'] = sku_data['Offer Display Name']
	for country in (config['T27'] + config['SERVICE_PROVIDER_COUNTRIES']):
		country_availability.loc[sku, country] = config['DEFAULT_VALUES']['YES'] if (sku + country) in countries_current.index else config['DEFAULT_VALUES']['NO']
#############################################################################################################################################################################################

# Create Structure DataFrame and save everything into destination ###########################################################################################################################
logging.info("Saving files")
with pandas.ExcelWriter(config['OM_OUTPUT']) as writer:
	om_new.to_excel(writer, sheet_name='OM', index_label='OfferId')
	om_last.to_excel(writer, sheet_name='OM Last', index_label='OfferId')
	relations_current.to_excel(writer, sheet_name='RM', index_label='RelationId')
	relations_last.to_excel(writer, sheet_name='RM Last', index_label='RelationId')
	relations_current_connect.to_excel(writer, sheet_name='RM Connect', index_label='RelationId')
	upgrades_current.to_excel(writer, sheet_name='UM', index_label='UpgradeId')
	countries_current.to_excel(writer, sheet_name='CM', index_label='RelationId')
	countries_last.to_excel(writer, sheet_name='CM Last', index_label='RelationId')
	new_skus_not_in_pl.to_excel(writer, sheet_name='New SKU not in PL', index_label='Sku')
	connect_items.to_excel(writer, sheet_name='Connect items', index_label='Sku')
	om_add_this_month.to_excel(writer, sheet_name='ADD this month')
	om_del_this_month.to_excel(writer, sheet_name='DEL this month')
	om_name_changes_this_month.to_excel(writer, sheet_name='Name Changes')
	om_microsoft_errors.to_excel(writer, sheet_name='Microsoft Errors')
	country_availability.to_excel(writer, sheet_name='Microsoft Country availability', index_label='OfferId')
	for group in config['SKU_GROUPS']:
		structure_current = pandas.DataFrame(columns=config['APS_STRUCTURE_HEADERS'])
		om_new_filtered = om_new[om_new['Group'] == group].sort_values(by='Offer Display Name')
		for sku, sku_data in om_new_filtered[om_new_filtered['Parent/Child'] == 'Parent'].iterrows():
			if sku_data['Duration'] in config['ALLOWED_DURATIONS'] or config['INCLUDE_LONG_TERM_SKUS']:
				structure_current = structure_current.append({'OfferID': sku, 'Parent SKU + RelationID': sku, 'Parent SKU': sku, 'Parent Name': sku_data['Offer Display Name'], 'Parent Name Short': sku_data['Shortened Names'], 'Offer Display Name': sku_data['Offer Display Name'], 'CMP Category': sku_data['CMP Category'], 'SKU In Last Month OM': sku_data['In Last Month OM'], 'Name Change': sku_data['Name Change']}, ignore_index=True)
				if sku in list(dict.fromkeys(relations_current['ParentId'])):
					addons_sku_list = list(dict.fromkeys(relations_current[relations_current['ParentId'] == sku]['ChildId']))
					for relation_sku, relation_sku_data in relations_current[relations_current['ParentId'] == sku].sort_values(by=['ChildName', 'ChildId']).iterrows():
						structure_current = structure_current.append({'OfferID': relation_sku_data['ChildId'], 'RelationID': relation_sku, 'Parent SKU + RelationID': sku + relation_sku, 'Parent SKU': sku, 'Parent Name': sku_data['Offer Display Name'], 'Parent Name Short': sku_data['Shortened Names'], 'Offer Display Name': relation_sku_data['ChildName'], 'CMP Category': om_new.loc[relation_sku_data['ChildId'], 'CMP Category'], 'SKU In Last Month OM': om_new.loc[relation_sku_data['ChildId'], 'In Last Month OM'], 'Relation In Last Month': relation_sku_data['Last Month'], 'Name Change': om_new.loc[relation_sku_data['ChildId'], 'Name Change']}, ignore_index=True)
						if relation_sku_data['ChildId'] in list(dict.fromkeys(relations_current['ParentId'])):
							for addon_to_addon_sku, addon_to_addon_data in relations_current[relations_current['ParentId'] == relation_sku_data['ChildId']].sort_values(by=['ChildName', 'ChildId']).iterrows():
								if addon_to_addon_data['ChildId'] not in addons_sku_list and config['REMOVE_ADDONS2ADDONS_IF_ADDON_EXISTS']:
									structure_current = structure_current.append({'OfferID': addon_to_addon_data['ChildId'], 'RelationID': addon_to_addon_sku, 'Parent SKU + RelationID': sku + addon_to_addon_sku, 'Parent SKU': sku, 'Parent Name': sku_data['Offer Display Name'], 'Parent Name Short': sku_data['Shortened Names'], 'Offer Display Name': addon_to_addon_data['ChildName'], 'CMP Category': 'ADDON to ADDON', 'SKU In Last Month OM': om_new.loc[addon_to_addon_data['ChildId'], 'In Last Month OM'], 'Relation In Last Month': addon_to_addon_data['Last Month'], 'Name Change': om_new.loc[addon_to_addon_data['ChildId'], 'Name Change']}, ignore_index=True)

		structure_current.to_excel(writer, sheet_name=config['SKU_GROUPS'][group]['shortname'] + ' APS')

	for group in config['SKU_GROUPS']:
		structure_current = pandas.DataFrame(columns=config['CONNECT_STRUCTURE_HEADERS'])
		om_new_filtered = om_new[om_new['Group'] == group].sort_values(by='Offer Display Name')
		for parent_sku, parent_sku_data in om_new_filtered[om_new_filtered['Parent/Child'] == 'Parent'].iterrows():
			if parent_sku_data['Duration'] in config['ALLOWED_DURATIONS'] or config['INCLUDE_LONG_TERM_SKUS']:
				structure_current = structure_current.append({'OfferID': parent_sku, 'RelationID': parent_sku, 'Parent SKU + RelationID': parent_sku, 'Parent SKU': parent_sku, 'Parent Name': parent_sku_data['Offer Display Name'], 'Parent Name Short': parent_sku_data['Shortened Names'], 'Connect product category': parent_sku_data['Connect product category'], 'Connect Product Id': parent_sku_data['Connect Product Id'], 'Connect annual item Id': parent_sku_data['Connect annual item Id'], 'Connect annual item name': parent_sku_data['Connect annual item name'], 'Connect monthly item Id': parent_sku_data['Connect monthly item Id'], 'Connect monthly item name': parent_sku_data['Connect monthly item name'], 'Offer Display Name': parent_sku_data['Offer Display Name'], 'SKU In Last Month OM': parent_sku_data['In Last Month OM'], 'Name Change': parent_sku_data['Name Change']}, ignore_index=True)
				if parent_sku in list(dict.fromkeys(relations_current_connect['ParentId'])):
					for relation_sku, relation_sku_data in relations_current_connect[relations_current_connect['ParentId'] == parent_sku].sort_values(by=['ChildName', 'ChildId']).iterrows():
						relation_sku_based_on_license_type = relation_sku_data['ChildId'] + '_' + parent_sku_data['License Type']
						if relation_sku_based_on_license_type in connect_items.index:
							structure_current = structure_current.append({'OfferID': relation_sku_data['ChildId'], 'RelationID': relation_sku, 'Parent SKU + RelationID': parent_sku + relation_sku, 'Parent SKU': parent_sku, 'Parent Name': parent_sku_data['Offer Display Name'], 'Parent Name Short': parent_sku_data['Shortened Names'], 'Connect product category': 'ADD-ON', 'Connect Product Id': connect_items.loc[relation_sku_based_on_license_type, 'Connect product'], 'Connect annual item Id': connect_items.loc[relation_sku_based_on_license_type, 'Annual item'], 'Connect annual item name': connect_items.loc[relation_sku_based_on_license_type, 'Annual name'], 'Connect monthly item Id': connect_items.loc[relation_sku_based_on_license_type, 'Monthly item'], 'Connect monthly item name': connect_items.loc[relation_sku_based_on_license_type, 'Monthly name'], 'Offer Display Name': relation_sku_data['ChildName'], 'SKU In Last Month OM': om_new.loc[relation_sku_data['ChildId'], 'In Last Month OM'], 'Name Change': om_new.loc[relation_sku_data['ChildId'], 'Name Change']}, ignore_index=True)
						else:
							structure_current = structure_current.append({'OfferID': relation_sku_data['ChildId'], 'RelationID': relation_sku, 'Parent SKU + RelationID': parent_sku + relation_sku, 'Parent SKU': parent_sku, 'Parent Name': parent_sku_data['Offer Display Name'], 'Parent Name Short': parent_sku_data['Shortened Names'], 'Offer Display Name': relation_sku_data['ChildName'], 'Connect product category': 'ADD-ON', 'SKU In Last Month OM': om_new.loc[relation_sku_data['ChildId'], 'In Last Month OM'], 'Name Change': om_new.loc[relation_sku_data['ChildId'], 'Name Change']}, ignore_index=True)

		structure_current.to_excel(writer, sheet_name=config['SKU_GROUPS'][group]['shortname'] + ' Connect')
#############################################################################################################################################################################################

# Load Software Subscriptions DataFrames ####################################################################################################################################################
sw_current = pandas.read_excel(config['SW_THIS_MONTH'])
sw_last = pandas.read_excel(config['SW_LAST_MONTH'], sheet_name='SW', index_col=0)
sw_two_months = pandas.read_excel(config['SW_TWO_MONTHS'], sheet_name='SW', index_col=0)
sw_new = pandas.DataFrame(columns=config['SW_HEADERS'])
#############################################################################################################################################################################################

# Load Software Subscriptions ###############################################################################################################################################################
for sw_sku, sw_sku_data in sw_current.iterrows():
	if '{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']) in sw_new.index:
		sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Allowed Countries'] = '{0};{1}'.format(sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Allowed Countries'], sw_sku_data['Regions'])
		sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'In Two Months Ago OM'] = config['DEFAULT_VALUES']['YES'] if '{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']) in sw_two_months.index else config['DEFAULT_VALUES']['NO']
	else:
		sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Offer Display Name'] = sw_sku_data['SkuTitle']
		sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Group'] = 'Software Subscriptions Corporate'
		sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'CMP Category'] = config['SW_GROUPS'][sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Group']]['CMP Category']
		sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Parent Category'] = config['SW_GROUPS'][sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Group']]['Parent Category']
		sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Sales Category'] = config['SW_GROUPS'][sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Group']]['Sales Category']
		sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Parent/Child'] = 'Parent'
		sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Tax Category'] = config['SW_SUBSCRIPTIONS_TAX_CATEGORY']
		sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Duration'] = '3 Years' if sw_sku_data['SkuTitle'].endswith('3 year') else '1 Year'
		sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Billing Frequency'] = 'One Time'
		sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Min Seat Count'] = '1'
		sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Max Seat Count'] = '5000'
		sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Allowed Countries'] = sw_sku_data['Regions']
		if '{}:{:>4}_Corporate'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']) in connect_items.index:
			sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Connect Product Id'] = connect_items.loc['{}:{:>4}_Corporate'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Connect product']
			sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Connect product name'] = connect_items.loc['{}:{:>4}_Corporate'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Product name']
			sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Connect product category'] = connect_items.loc['{}:{:>4}_Corporate'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Product category']
			if pandas.notna(connect_items.loc['{}:{:>4}_Corporate'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Onetime item']):
				sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Connect onetime item Id'] = connect_items.loc['{}:{:>4}_Corporate'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Onetime item']
			if pandas.notna(connect_items.loc['{}:{:>4}_Corporate'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Onetime name']):
				sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Connect onetime item name'] = connect_items.loc['{}:{:>4}_Corporate'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Onetime name']
#############################################################################################################################################################################################

# Update last month OM to update this month's deletes #######################################################################################################################################
for sw_last_sku, sw_last_sku_data in sw_last.iterrows():
	if sw_last_sku in sw_new.index:
		sw_new.loc[sw_last_sku, 'In Last Month OM'] = config['DEFAULT_VALUES']['YES']
		sw_last.loc[sw_last_sku, 'In Next Month OM'] = config['DEFAULT_VALUES']['YES']
		sw_new.loc[sw_last_sku, 'Shortened Names'] = sw_last_sku_data['Shortened Names']
		sw_new.loc[sw_last_sku, 'Length'] = len(str(sw_new.loc[sw_last_sku, 'Shortened Names']))
	else:
		sw_new.loc[sw_last_sku, 'In Last Month OM'] = config['DEFAULT_VALUES']['NO']
		sw_last.loc[sw_last_sku, 'In Next Month OM'] = config['DEFAULT_VALUES']['NO']
#############################################################################################################################################################################################

# Create Structure DataFrame and save everything into destination ###########################################################################################################################
with pandas.ExcelWriter(config['SW_OUTPUT']) as writer:
	sw_new.sort_values(by='Offer Display Name').to_excel(writer, sheet_name='SW', index_label='OfferID')
	sw_last.sort_values(by='Offer Display Name').to_excel(writer, sheet_name='SW LAST', index_label='OfferID')
	connect_items.to_excel(writer, sheet_name='Connect items', index_label='Sku')
#############################################################################################################################################################################################
