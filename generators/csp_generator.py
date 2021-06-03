import os
import pandas
import numpy as np
import logging
from pandas.api.types import CategoricalDtype
from tinydb import Query, TinyDB
from datetime import date
import tabulate


# Generates CSP License Based output file
class CSPGenerator:

    def __init__(self, config, loader, connect_items):
        self.config = config
        self.loader = loader
        self.connect_items = connect_items
        self.om_output_path = loader.OM_OUTPUT_PATH
        self.db = TinyDB(f'./cache/db_{date.today().strftime("%Y%m%d")}.json')
        self.om_new = pandas.DataFrame(columns=config['OM_HEADERS'])
        self.relations_current = pandas.DataFrame(columns=config['APS_RELATIONS_HEADERS'])
        self.relations_current_connect = pandas.DataFrame(columns=config['CONNECT_RELATIONS_HEADERS'])
        self.upgrades_current = pandas.DataFrame(columns=config['UPGRADES_HEADERS'])
        self.countries_current = pandas.DataFrame(columns=config['COUNTRIES_HEADERS'])
        self.new_skus_not_in_pl = pandas.DataFrame()
        self.country_availability = pandas.DataFrame(columns=config['COUNTRY_AVAIABILITY_HEADERS'] + config['T27'] + config['SERVICE_PROVIDER_COUNTRIES'])
        self.om_add_this_month = pandas.DataFrame(columns=config['OM_ADD_THIS_MONTH_HEADERS'])
        self.om_del_this_month = pandas.DataFrame(columns=config['OM_DEL_THIS_MONTH_HEADERS'])
        self.om_name_changes_this_month = pandas.DataFrame(columns=config['OM_NAME_CHANGES_THIS_MONTH_HEADERS'])
        self.om_microsoft_errors = pandas.DataFrame(columns=config['OM_MICROSOFT_ERRORS_HEADERS'])
        self.aps_skus = pandas.DataFrame(columns=config['APS_STRUCTURE_HEADERS'])
        self.connect_skus = pandas.DataFrame(columns=config['CONNECT_STRUCTURE_HEADERS'])

    def generate(self):
        self.build_om()
        self.build_relations()
        self.build_connect_relations()
        self.build_upgrades()
        self.build_countries()
        self.build_updates()
        self.build_ms_errors()
        self.build_ms_countries()
        self.build_aps_skus()
        self.build_connect_skus()
        self.save_files()

    def build_om(self):
        # Generate this month's OM
        logging.info("Generating this month's OM")
        for sku, sku_data in self.loader.om_current.iterrows():
            self.om_new.loc[sku, 'In Two Months Ago OM'] = self.config['DEFAULT_VALUES']['YES'] if sku in self.loader.om_two_months.index else self.config['DEFAULT_VALUES']['NO']
            self.om_new.loc[sku, 'Manually Added'] = self.config['DEFAULT_VALUES']['NO']
            self.om_new.loc[sku, 'Offer Display Name'] = sku_data['Offer Display Name']
            self.om_new.loc[sku, 'Provisioning ID'] = sku_data['Provisioning ID']
            self.om_new.loc[sku, 'Parent/Child'] = "Parent" if pandas.isna(sku_data['Depends On']) else "Child"
            self.om_new.loc[sku, 'Offer Type'] = sku_data['Offer Type']
            self.om_new.loc[sku, 'Duration'] = sku_data['Duration']
            self.om_new.loc[sku, 'Billing Frequency'] = sku_data['Billing Frequency']
            self.om_new.loc[sku, 'Min Seat Count'] = sku_data['Min Seat Count']
            self.om_new.loc[sku, 'Max Seat Count'] = sku_data['Max Seat Count']
            self.om_new.loc[sku, 'Offer Limit'] = sku_data['Offer Limit']
            self.om_new.loc[sku, 'Offer Limit Scope'] = sku_data['Offer Limit Scope']
            self.om_new.loc[sku, 'Depends On'] = sku_data['Depends On']
            self.om_new.loc[sku, 'Can Convert To'] = sku_data['Can Convert To']
            self.om_new.loc[sku, 'Offer URI'] = sku_data['Offer URI']
            self.om_new.loc[sku, 'LearnMoreLink'] = sku_data['LearnMoreLink']
            self.om_new.loc[sku, 'Offer Display Description'] = sku_data['Offer Display Description']
            self.om_new.loc[sku, 'Allowed Countries'] = sku_data['Allowed Countries']
            self.om_new.loc[sku, 'GUID + Offer Name'] = sku + sku_data['Offer Display Name']
            self.om_new.loc[sku, 'In current PL'] = self.loader.pl_current.loc[sku, 'A/C/D/U'] if sku in self.loader.pl_current.index else self.config['DEFAULT_VALUES']['NO']
            self.om_new.loc[sku, 'In Next Month PL'] = self.loader.pl_next_month_preview.loc[sku, 'A/C/D/U'] if sku in self.loader.pl_next_month_preview.index else self.config['DEFAULT_VALUES']['NO']
            self.om_new.loc[sku, 'In Last Month PL'] = self.loader.pl_last.loc[sku, 'A/C/D/U'] if sku in self.loader.pl_last.index else self.config['DEFAULT_VALUES']['NO']
            self.om_new.loc[sku, 'In Two Months Ago PL'] = self.loader.pl_two_months.loc[sku, 'A/C/D/U'] if sku in self.loader.pl_two_months.index else self.config['DEFAULT_VALUES']['NO']
            if sku in self.loader.pl_current.index:
                connect_sku_with_license_type = sku.lower() + '_' + self.loader.pl_current.loc[sku, 'License Agreement Type']
            elif sku in self.loader.om_last.index:
                connect_sku_with_license_type = sku.lower() + '_' + self.loader.om_last.loc[sku, 'License Type']
            else:
                self.new_skus_not_in_pl.loc[sku, 'Offer Display Name'] = sku_data['Offer Display Name']
            if connect_sku_with_license_type in self.connect_items.index:
                self.om_new.loc[sku, 'Connect Product Id'] = self.connect_items.loc[connect_sku_with_license_type, 'Connect product']
                self.om_new.loc[sku, 'Connect product name'] = self.connect_items.loc[connect_sku_with_license_type, 'Product name']
                self.om_new.loc[sku, 'Connect product category'] = self.connect_items.loc[connect_sku_with_license_type, 'Product category']
                if pandas.notna(self.connect_items.loc[connect_sku_with_license_type, 'Annual item']):
                    self.om_new.loc[sku, 'Connect annual item Id'] = self.connect_items.loc[connect_sku_with_license_type, 'Annual item']
                if pandas.notna(self.connect_items.loc[connect_sku_with_license_type, 'Annual name']):
                    self.om_new.loc[sku, 'Connect annual item name'] = self.connect_items.loc[connect_sku_with_license_type, 'Annual name']
                if pandas.notna(self.connect_items.loc[connect_sku_with_license_type, 'Monthly item']):
                    self.om_new.loc[sku, 'Connect monthly item Id'] = self.connect_items.loc[connect_sku_with_license_type, 'Monthly item']
                if pandas.notna(self.connect_items.loc[connect_sku_with_license_type, 'Monthly name']):
                    self.om_new.loc[sku, 'Connect monthly item name'] = self.connect_items.loc[connect_sku_with_license_type, 'Monthly name']

            if sku in self.loader.om_last.index and self.loader.om_last.loc[sku, 'Group'] != self.config['DEFAULT_VALUES']['MICROSOFT_ERROR']:
                self.om_new.loc[sku, 'In Last Month OM'] = self.config['DEFAULT_VALUES']['YES']
                self.om_new.loc[sku, 'Group'] = self.loader.om_last.loc[sku, 'Group']
                self.om_new.loc[sku, 'CMP Category'] = self.loader.om_last.loc[sku, 'CMP Category']
                self.om_new.loc[sku, 'Parent Category'] = self.loader.om_last.loc[sku, 'Parent Category']
                self.om_new.loc[sku, 'Sales Category'] = self.loader.om_last.loc[sku, 'Sales Category']
                self.om_new.loc[sku, 'Tax Category'] = self.loader.om_last.loc[sku, 'Tax Category']
                self.om_new.loc[sku, 'License Type'] = self.loader.pl_current.loc[sku, 'License Agreement Type'] if sku in self.loader.pl_current.index else self.loader.om_last.loc[sku, 'License Type']
                self.om_new.loc[sku, 'GUID + Offer Name in last month OM'] = self.config['DEFAULT_VALUES']['YES'] if self.loader.om_last.loc[sku, 'GUID + Offer Name'] == self.om_new.loc[sku, 'GUID + Offer Name'] else self.config['DEFAULT_VALUES']['NO']
                self.om_new.loc[sku, 'GUID in last month OM'] = self.config['DEFAULT_VALUES']['YES']
                self.om_new.loc[sku, 'Shortened Names'] = self.loader.om_last.loc[sku, 'Shortened Names'] if pandas.notna(self.loader.om_last.loc[sku, 'Shortened Names']) else ""
                self.om_new.loc[sku, 'Length'] = len(self.om_new.loc[sku, 'Shortened Names'])
                self.om_new.loc[sku, 'Microsoft Change'] = self.config['DEFAULT_VALUES']['NO']
                for microsoft_field in self.config['MICROSOFT_LIMITS_TO_VALIDATE'] + self.config['MICROSOFT_FIELDS_TO_VALIDATE']:
                    if self.om_new.loc[sku, microsoft_field] != self.loader.om_last.loc[sku, microsoft_field]:
                        if self.om_new.loc[sku, 'Microsoft Change'] == self.config['DEFAULT_VALUES']['NO']:
                            if microsoft_field in self.config['MICROSOFT_FIELDS_TO_VALIDATE']:
                                self.om_new.loc[sku, 'Microsoft Change'] = '* {}'.format(microsoft_field)
                            else:
                                self.om_new.loc[sku, 'Microsoft Change'] = '* {}: Old value: {} - New value: {}'.format(microsoft_field, self.loader.om_last.loc[sku, microsoft_field], sku_data[microsoft_field])
                        else:
                            if microsoft_field in self.config['MICROSOFT_FIELDS_TO_VALIDATE']:
                                self.om_new.loc[sku, 'Microsoft Change'] = self.om_new.loc[sku, 'Microsoft Change'] + '\n* {0}'.format(microsoft_field)
                            else:
                                self.om_new.loc[sku, 'Microsoft Change'] = self.om_new.loc[sku, 'Microsoft Change'] + '\n* {}: Old value: {} - New value: {}'.format(microsoft_field, self.loader.om_last.loc[sku, microsoft_field], sku_data[microsoft_field])
            else: # SKU not in last OM
                self.om_new.loc[sku, 'In Last Month OM'] = self.config['DEFAULT_VALUES']['NO']
                self.om_new.loc[sku, 'Microsoft Change'] = self.config['DEFAULT_VALUES']['NO']
                if sku not in self.loader.om_ghost.index:
                    self.om_new.loc[sku, 'Group'] = self.request_info('Group', list(self.config['SKU_GROUPS'].keys()) + [self.config['DEFAULT_VALUES']['MICROSOFT_ERROR']], sku, self.om_new.loc[sku, 'Offer Display Name'], self.om_new.loc[sku, 'In Two Months Ago PL'], self.om_new.loc[sku, 'In Last Month PL'], self.om_new.loc[sku, 'In current PL'], self.om_new.loc[sku, 'In Next Month PL'], self.loader.pl_current.loc[sku, 'License Agreement Type'] if sku in self.loader.pl_current.index else self.config['DEFAULT_VALUES']['NO'])
                    if self.om_new.loc[sku, 'Group'] in self.config['SKU_GROUPS']:
                        if self.om_new.loc[sku, 'Group'] == "Trials":
                            self.om_new.loc[sku, 'CMP Category'] = self.request_info('CMP Category', list(dict.fromkeys(self.loader.om_last['CMP Category'])), sku, self.om_new.loc[sku, 'Offer Display Name'], self.om_new.loc[sku, 'In Two Months Ago PL'], self.om_new.loc[sku, 'In Last Month PL'], self.om_new.loc[sku, 'In current PL'], self.om_new.loc[sku, 'In Next Month PL'], self.loader.pl_current.loc[sku, 'License Agreement Type'] if sku in self.loader.pl_current.index else self.config['DEFAULT_VALUES']['NO'])
                            self.om_new.loc[sku, 'Parent Category'] = self.request_info('Parent Category', list(dict.fromkeys(self.loader.om_last['Parent Category'])), sku, self.om_new.loc[sku, 'Offer Display Name'], self.om_new.loc[sku, 'In Two Months Ago PL'], self.om_new.loc[sku, 'In Last Month PL'], self.om_new.loc[sku, 'In current PL'], self.om_new.loc[sku, 'In Next Month PL'], self.loader.pl_current.loc[sku, 'License Agreement Type'] if sku in self.loader.pl_current.index else self.config['DEFAULT_VALUES']['NO'])
                            self.om_new.loc[sku, 'Sales Category'] = self.request_info('Sales Category', list(dict.fromkeys(self.loader.om_last['Sales Category'])), sku, self.om_new.loc[sku, 'Offer Display Name'], self.om_new.loc[sku, 'In Two Months Ago PL'], self.om_new.loc[sku, 'In Last Month PL'], self.om_new.loc[sku, 'In current PL'], self.om_new.loc[sku, 'In Next Month PL'], self.loader.pl_current.loc[sku, 'License Agreement Type'] if sku in self.loader.pl_current.index else self.config['DEFAULT_VALUES']['NO'])
                        else:
                            self.om_new.loc[sku, 'CMP Category'] = self.config['SKU_GROUPS'][self.om_new.loc[sku, 'Group']]['CMP Category'] if str(self.om_new.loc[sku, 'Parent/Child']) == "Parent" else "ADDON"
                            self.om_new.loc[sku, 'Parent Category'] = self.config['SKU_GROUPS'][self.om_new.loc[sku, 'Group']]['Parent Category']
                            self.om_new.loc[sku, 'Sales Category'] = self.config['SKU_GROUPS'][self.om_new.loc[sku, 'Group']]['Sales Category'] if str(self.om_new.loc[sku, 'Parent/Child']) == "Parent" else "ADDON"
                        self.om_new.loc[sku, 'License Type'] = self.loader.pl_current.loc[sku, 'License Agreement Type'] if sku in self.loader.pl_current.index else self.request_info('License Type', list(dict.fromkeys(self.loader.om_last['License Type'])), sku, self.om_new.loc[sku, 'Offer Display Name'], self.om_new.loc[sku, 'In Two Months Ago PL'], self.om_new.loc[sku, 'In Last Month PL'], self.om_new.loc[sku, 'In current PL'], self.om_new.loc[sku, 'In Next Month PL'])
                        self.om_new.loc[sku, 'Tax Category'] = self.config['CSP_TAX_CATEGORY']
                        self.om_new.loc[sku, 'GUID + Offer Name in last month OM'] = self.config['DEFAULT_VALUES']['NO']
                        self.om_new.loc[sku, 'GUID in last month OM'] = self.config['DEFAULT_VALUES']['NO']
                        self.om_new.loc[sku, 'Shortened Names'] = self.request_info('Shortened Names', sku=sku, sku_name=self.om_new.loc[sku, 'Offer Display Name'])
                        self.om_new.loc[sku, 'Length'] = len(self.om_new.loc[sku, 'Shortened Names'])
                    elif self.om_new.loc[sku, 'Group'] == self.config['DEFAULT_VALUES']['MICROSOFT_ERROR']:
                        self.om_new.loc[sku, 'Parent Category'] = self.config['DEFAULT_VALUES']['MICROSOFT_ERROR']
                        self.om_new.loc[sku, 'License Type'] = self.config['DEFAULT_VALUES']['MICROSOFT_ERROR']
                        self.om_new.loc[sku, 'CMP Category'] = self.config['DEFAULT_VALUES']['MICROSOFT_ERROR']
                        self.om_new.loc[sku, 'Sales Category'] = self.config['DEFAULT_VALUES']['MICROSOFT_ERROR']
                else:
                    self.om_new.loc[sku, 'Group'] = self.loader.om_ghost.loc[sku, 'Group']
                    self.om_new.loc[sku, 'CMP Category'] = self.loader.om_ghost.loc[sku, 'CMP Category']
                    self.om_new.loc[sku, 'Parent Category'] = self.loader.om_ghost.loc[sku, 'Parent Category']
                    self.om_new.loc[sku, 'Sales Category'] = self.loader.om_ghost.loc[sku, 'Sales Category']
                    self.om_new.loc[sku, 'License Type'] = self.loader.om_ghost.loc[sku, 'License Type']
                    self.om_new.loc[sku, 'Tax Category'] = self.loader.om_ghost.loc[sku, 'Tax Category']
                    self.om_new.loc[sku, 'GUID + Offer Name in last month OM'] = self.loader.om_ghost.loc[sku, 'GUID + Offer Name in last month OM']
                    self.om_new.loc[sku, 'GUID in last month OM'] = self.loader.om_ghost.loc[sku, 'GUID in last month OM']
                    self.om_new.loc[sku, 'Shortened Names'] = self.loader.om_ghost.loc[sku, 'Shortened Names']
                    self.om_new.loc[sku, 'Length'] = self.loader.om_ghost.loc[sku, 'Length']

            if self.om_new.loc[sku, 'GUID + Offer Name in last month OM'] == self.config['DEFAULT_VALUES']['NO'] and self.om_new.loc[sku, 'GUID in last month OM'] == self.config['DEFAULT_VALUES']['YES']:
                self.om_new.loc[sku, 'Name Change'] = self.config['DEFAULT_VALUES']['YES']
                self.om_new.loc[sku, 'Old Name'] = self.loader.om_last.loc[sku, 'Offer Display Name']
            else:
                self.om_new.loc[sku, 'Name Change'] = self.config['DEFAULT_VALUES']['NO']
            
            sku_group = self.om_new.loc[sku, 'Group']
            sku_group_config = self.config['SKU_GROUPS']
            if sku_group == self.config['DEFAULT_VALUES']['MICROSOFT_ERROR']:
                self.om_new.loc[sku, 'Global Menu Group'] = sku_group
                self.om_new.loc[sku, 'Family'] = sku_group
                self.om_new.loc[sku, 'Plan Category Monthly (major group, billing, tax)'] = sku_group
                self.om_new.loc[sku, 'Plan Category Annual (major group, billing, tax)'] = sku_group
                self.om_new.loc[sku, 'Resource Category (major group, industry, tax)'] = sku_group
                self.om_new.loc[sku, 'Vendor ID'] = sku_group
            else:
                self.om_new.loc[sku, 'Global Menu Group'] = sku_group_config[sku_group]['Global Menu Group']
                self.om_new.loc[sku, 'Family'] = sku_group_config[sku_group]['OM Family']
                self.om_new.loc[sku, 'Plan Category Monthly (major group, billing, tax)'] = sku_group_config[sku_group]['Monthly Plan Cat']
                self.om_new.loc[sku, 'Plan Category Annual (major group, billing, tax)'] = sku_group_config[sku_group]['Annual Plan Cat']
                self.om_new.loc[sku, 'Resource Category (major group, industry, tax)'] = sku_group_config[sku_group]['Resource Cat']
                self.om_new.loc[sku, 'Vendor ID'] = sku_group_config[sku_group]['Vendor ID']
            self.om_new.loc[sku, 'Tax Category Name US'] = self.config['CSP_TAX_CAT_NAME_US']
            self.om_new.loc[sku, 'Tax Category Name Rest of World'] = self.config['CSP_TAX_CAT_NAME_WORLD']
            self.om_new.loc[sku, 'Previous Months Shortened Names'] = sku_data['Offer Display Name']
            self.om_new.loc[sku, 'BSS Monthly Name (Parents)'] = sku_data['Offer Display Name'] + " (Monthly Pre-Paid)"
            self.om_new.loc[sku, 'BSS Annual Name (Parents)'] = sku_data['Offer Display Name'] + " (Annual Pre-Paid)"
             

        for sku_in_last_not_in_current, sku_in_last_not_in_current_data in self.loader.om_last.iterrows():
            if sku_in_last_not_in_current not in self.om_new.index and sku_in_last_not_in_current in self.loader.pl_current.index and self.loader.pl_current.loc[sku_in_last_not_in_current, 'A/C/D/U'] != 'DEL':
                self.loader.om_last.loc[sku_in_last_not_in_current, 'In Next Month OM'] = self.config['DEFAULT_VALUES']['YES']
                self.om_new.loc[sku_in_last_not_in_current, 'In Two Months Ago OM'] = self.config['DEFAULT_VALUES']['YES'] if sku_in_last_not_in_current in self.loader.om_two_months.index else self.config['DEFAULT_VALUES']['NO']
                self.om_new.loc[sku_in_last_not_in_current, 'In Last Month OM'] = self.config['DEFAULT_VALUES']['YES']
                self.om_new.loc[sku_in_last_not_in_current, 'Offer Display Name'] = self.loader.om_last.loc[sku_in_last_not_in_current, 'Offer Display Name']
                self.om_new.loc[sku_in_last_not_in_current, 'Provisioning ID'] = self.loader.om_last.loc[sku_in_last_not_in_current, 'Provisioning ID']
                self.om_new.loc[sku_in_last_not_in_current, 'Manually Added'] = self.config['DEFAULT_VALUES']['NO']
                self.om_new.loc[sku_in_last_not_in_current, 'Group'] = self.loader.om_last.loc[sku_in_last_not_in_current, 'Group']
                self.om_new.loc[sku_in_last_not_in_current, 'Parent/Child'] = self.loader.om_last.loc[sku_in_last_not_in_current, 'Parent/Child']
                self.om_new.loc[sku_in_last_not_in_current, 'CMP Category'] = self.loader.om_last.loc[sku_in_last_not_in_current, 'CMP Category']
                self.om_new.loc[sku_in_last_not_in_current, 'Parent Category'] = self.loader.om_last.loc[sku_in_last_not_in_current, 'Parent Category']
                self.om_new.loc[sku_in_last_not_in_current, 'Sales Category'] = self.loader.om_last.loc[sku_in_last_not_in_current, 'Sales Category']
                self.om_new.loc[sku_in_last_not_in_current, 'Tax Category'] = self.loader.om_last.loc[sku_in_last_not_in_current, 'Tax Category']
                self.om_new.loc[sku_in_last_not_in_current, 'Offer Type'] = self.loader.om_last.loc[sku_in_last_not_in_current, 'Offer Type']
                self.om_new.loc[sku_in_last_not_in_current, 'Duration'] = self.loader.om_last.loc[sku_in_last_not_in_current, 'Duration']
                self.om_new.loc[sku_in_last_not_in_current, 'Billing Frequency'] = self.loader.om_last.loc[sku_in_last_not_in_current, 'Billing Frequency']
                self.om_new.loc[sku_in_last_not_in_current, 'Min Seat Count'] = self.loader.om_last.loc[sku_in_last_not_in_current, 'Min Seat Count']
                self.om_new.loc[sku_in_last_not_in_current, 'Max Seat Count'] = self.loader.om_last.loc[sku_in_last_not_in_current, 'Max Seat Count']
                self.om_new.loc[sku_in_last_not_in_current, 'Offer Limit'] = self.loader.om_last.loc[sku_in_last_not_in_current, 'Offer Limit']
                self.om_new.loc[sku_in_last_not_in_current, 'Offer Limit Scope'] = self.loader.om_last.loc[sku_in_last_not_in_current, 'Offer Limit Scope']
                self.om_new.loc[sku_in_last_not_in_current, 'Depends On'] = self.loader.om_last.loc[sku_in_last_not_in_current, 'Depends On']
                self.om_new.loc[sku_in_last_not_in_current, 'Offer URI'] = self.loader.om_last.loc[sku_in_last_not_in_current, 'Offer URI']
                self.om_new.loc[sku_in_last_not_in_current, 'LearnMoreLink'] = self.loader.om_last.loc[sku_in_last_not_in_current, 'LearnMoreLink']
                self.om_new.loc[sku_in_last_not_in_current, 'Offer Display Description'] = self.loader.om_last.loc[sku_in_last_not_in_current, 'Offer Display Description']
                self.om_new.loc[sku_in_last_not_in_current, 'Allowed Countries'] = self.loader.om_last.loc[sku_in_last_not_in_current, 'Allowed Countries']
                self.om_new.loc[sku_in_last_not_in_current, 'GUID + Offer Name'] = sku_in_last_not_in_current + self.om_new.loc[sku_in_last_not_in_current, 'Offer Display Name']
                self.om_new.loc[sku_in_last_not_in_current, 'GUID + Offer Name in last month OM'] = self.config['DEFAULT_VALUES']['YES']
                self.om_new.loc[sku_in_last_not_in_current, 'GUID in last month OM'] = self.config['DEFAULT_VALUES']['YES']
                self.om_new.loc[sku_in_last_not_in_current, 'Name Change'] = self.config['DEFAULT_VALUES']['NO']
                self.om_new.loc[sku_in_last_not_in_current, 'License Type'] = self.loader.pl_current.loc[sku_in_last_not_in_current, 'License Agreement Type']
                self.om_new.loc[sku_in_last_not_in_current, 'Shortened Names'] = self.loader.om_last.loc[sku_in_last_not_in_current, 'Shortened Names']
                self.om_new.loc[sku_in_last_not_in_current, 'Length'] = len(self.om_new.loc[sku_in_last_not_in_current, 'Shortened Names'])
                self.om_new.loc[sku_in_last_not_in_current, 'In current PL'] = self.loader.pl_current.loc[sku_in_last_not_in_current, 'A/C/D/U']
                self.om_new.loc[sku_in_last_not_in_current, 'In Next Month PL'] = self.loader.pl_next_month_preview.loc[sku_in_last_not_in_current, 'A/C/D/U'] if sku_in_last_not_in_current in self.loader.pl_next_month_preview.index else self.config['DEFAULT_VALUES']['NO']
                self.om_new.loc[sku_in_last_not_in_current, 'In Last Month PL'] = self.loader.pl_last.loc[sku_in_last_not_in_current, 'A/C/D/U'] if sku_in_last_not_in_current in self.loader.pl_last.index else self.config['DEFAULT_VALUES']['NO']
                self.om_new.loc[sku_in_last_not_in_current, 'In Two Months Ago PL'] = self.loader.pl_two_months.loc[sku_in_last_not_in_current, 'A/C/D/U'] if sku_in_last_not_in_current in self.loader.pl_two_months.index else self.config['DEFAULT_VALUES']['NO']
                self.om_new.loc[sku_in_last_not_in_current, 'Microsoft Change'] = self.config['DEFAULT_VALUES']['NO']

        for manual_sku, manual_sku_data in self.loader.om_last[self.loader.om_last['Manually Added'] == self.config['DEFAULT_VALUES']['YES']].iterrows():
            if manual_sku not in self.om_new.index:
                self.loader.om_last.loc[manual_sku, 'In Next Month OM'] = self.config['DEFAULT_VALUES']['YES']
                self.om_new.loc[manual_sku, 'In Two Months Ago OM'] = self.config['DEFAULT_VALUES']['YES'] if manual_sku in self.loader.om_two_months.index else self.config['DEFAULT_VALUES']['NO']
                self.om_new.loc[manual_sku, 'In Last Month OM'] = self.config['DEFAULT_VALUES']['YES']
                self.om_new.loc[manual_sku, 'Offer Display Name'] = manual_sku_data['Offer Display Name']
                self.om_new.loc[manual_sku, 'Provisioning ID'] = manual_sku_data['Provisioning ID']
                self.om_new.loc[manual_sku, 'Manually Added'] = self.config['DEFAULT_VALUES']['YES']
                self.om_new.loc[manual_sku, 'Group'] = manual_sku_data['Group']
                self.om_new.loc[manual_sku, 'Parent/Child'] = manual_sku_data['Parent/Child']
                self.om_new.loc[manual_sku, 'CMP Category'] = manual_sku_data['CMP Category']
                self.om_new.loc[manual_sku, 'Parent Category'] = manual_sku_data['Parent Category']
                self.om_new.loc[manual_sku, 'Sales Category'] = manual_sku_data['Sales Category']
                self.om_new.loc[manual_sku, 'Tax Category'] = manual_sku_data['Tax Category']
                self.om_new.loc[manual_sku, 'Offer Type'] = manual_sku_data['Offer Type']
                self.om_new.loc[manual_sku, 'Duration'] = manual_sku_data['Duration']
                self.om_new.loc[manual_sku, 'Billing Frequency'] = manual_sku_data['Billing Frequency']
                self.om_new.loc[manual_sku, 'Min Seat Count'] = manual_sku_data['Min Seat Count']
                self.om_new.loc[manual_sku, 'Max Seat Count'] = manual_sku_data['Max Seat Count']
                self.om_new.loc[manual_sku, 'Offer Limit'] = manual_sku_data['Offer Limit']
                self.om_new.loc[manual_sku, 'Offer Limit Scope'] = manual_sku_data['Offer Limit Scope']
                self.om_new.loc[manual_sku, 'Depends On'] = manual_sku_data['Depends On']
                self.om_new.loc[manual_sku, 'Offer URI'] = manual_sku_data['Offer URI']
                self.om_new.loc[manual_sku, 'LearnMoreLink'] = manual_sku_data['LearnMoreLink']
                self.om_new.loc[manual_sku, 'Offer Display Description'] = manual_sku_data['Offer Display Description']
                self.om_new.loc[manual_sku, 'Allowed Countries'] = manual_sku_data['Allowed Countries']
                self.om_new.loc[manual_sku, 'GUID + Offer Name'] = manual_sku + self.om_new.loc[manual_sku, 'Offer Display Name']
                self.om_new.loc[manual_sku, 'GUID + Offer Name in last month OM'] = self.config['DEFAULT_VALUES']['YES'] if manual_sku_data['GUID + Offer Name'] == self.om_new.loc[manual_sku, 'GUID + Offer Name'] else self.config['DEFAULT_VALUES']['NO']
                self.om_new.loc[manual_sku, 'GUID in last month OM'] = self.config['DEFAULT_VALUES']['YES']
                self.om_new.loc[manual_sku, 'Name Change'] = self.config['DEFAULT_VALUES']['NO']
                self.om_new.loc[manual_sku, 'License Type'] = manual_sku_data['License Type']
                self.om_new.loc[manual_sku, 'Shortened Names'] = manual_sku_data['Shortened Names']
                self.om_new.loc[manual_sku, 'Length'] = len(self.om_new.loc[manual_sku, 'Shortened Names'])
                self.om_new.loc[manual_sku, 'In current PL'] = self.loader.pl_current.loc[manual_sku, 'A/C/D/U'] if manual_sku in self.loader.pl_current.index else self.config['DEFAULT_VALUES']['NO']
                self.om_new.loc[manual_sku, 'In Next Month PL'] = self.loader.pl_next_month_preview.loc[manual_sku, 'A/C/D/U'] if manual_sku in self.loader.pl_next_month_preview.index else self.config['DEFAULT_VALUES']['NO']
                self.om_new.loc[manual_sku, 'In Last Month PL'] = self.loader.pl_last.loc[manual_sku, 'A/C/D/U'] if manual_sku in self.loader.pl_last.index else self.config['DEFAULT_VALUES']['NO']
                self.om_new.loc[manual_sku, 'In Two Months Ago PL'] = self.loader.pl_two_months.loc[manual_sku, 'A/C/D/U'] if manual_sku in self.loader.pl_two_months.index else self.config['DEFAULT_VALUES']['NO']
                self.om_new.loc[manual_sku, 'Microsoft Change'] = self.config['DEFAULT_VALUES']['NO']
                connect_sku_with_license_type = manual_sku.lower() + '_' + self.loader.om_last.loc[manual_sku, 'License Type']
                if connect_sku_with_license_type in self.connect_items.index and pandas.notna(self.connect_items.loc[connect_sku_with_license_type, 'Annual item']):
                    self.om_new.loc[manual_sku, 'Connect annual item Id'] = self.connect_items.loc[connect_sku_with_license_type, 'Annual item']
                if connect_sku_with_license_type in self.connect_items.index and pandas.notna(self.connect_items.loc[connect_sku_with_license_type, 'Annual name']):
                    self.om_new.loc[manual_sku, 'Connect annual item name'] = self.connect_items.loc[connect_sku_with_license_type, 'Annual name']
                if connect_sku_with_license_type in self.connect_items.index and pandas.notna(self.connect_items.loc[connect_sku_with_license_type, 'Monthly item']):
                    self.om_new.loc[manual_sku, 'Connect monthly item Id'] = self.connect_items.loc[connect_sku_with_license_type, 'Monthly item']
                if connect_sku_with_license_type in self.connect_items.index and pandas.notna(self.connect_items.loc[connect_sku_with_license_type, 'Monthly name']):
                    self.om_new.loc[manual_sku, 'Connect monthly item name'] = self.connect_items.loc[connect_sku_with_license_type, 'Monthly name']

    def request_info(self, name, choices=None, sku=None, sku_name=None, sku_pl_two_months_ago=None, sku_pl_last_month=None, sku_pl_current=None, sku_pl_next_month=None, sku_license_type=None):
        if name in ['Group', 'CMP Category', 'Parent Category', 'Sales Category']:
            info = self.lookup_sku_group(sku)
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
            self.save_sku_group(sku, sku_name, info)
            return info
        elif name in ['Shortened Names']:
            short_name = self.lookup_sku_shortname(sku)
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
            self.save_sku_shortname(sku, sku_name, short_name)
            return short_name

    def lookup_sku_group(self, sku):
        info_query = Query()
        info_db_result = self.db.search((info_query.sku == sku) & (info_query.group.exists()))
        if info_db_result:
            return info_db_result[0]['group']

    def lookup_sku_shortname(self, sku):
        info_query = Query()
        info_db_result = self.db.search((info_query.sku == sku) & (info_query.sku_short_name.exists()))
        if info_db_result:
            return info_db_result[0]['sku_short_name']

    def save_sku_group(self, sku, sku_name, group):
        upsert_query = Query()
        self.db.upsert({'sku': sku, 'sku_name': sku_name, 'group': group}, upsert_query.sku == sku)

    def save_sku_shortname(self, sku, sku_name, sku_short_name):
        upsert_query = Query()
        self.db.upsert({'sku': sku, 'sku_name': sku_name, 'sku_short_name': sku_short_name}, upsert_query.sku == sku)

    def build_relations(self):
        # Generate this month's APS RM
        logging.info("Generating this month's APS RM")
        for children_sku, children_data in self.om_new.iterrows():
            if pandas.notna(children_data['Depends On']):
                parents = str(children_data['Depends On']).split(";")
                for parent_sku in parents:
                    if parent_sku in self.om_new.index:
                        if self.om_new.loc[parent_sku, 'Duration'] in self.config['ALLOWED_DURATIONS'] or self.config['INCLUDE_LONG_TERM_SKUS']:
                            try:
                                if self.om_new.loc[parent_sku, 'Duration'] == children_data['Duration'] and self.om_new.loc[parent_sku, 'Group'] in self.config['SKU_GROUPS'] and children_data['License Type'] in self.config['ALLOWED_RELATIONS'][self.om_new.loc[parent_sku, 'License Type']]:
                                    self.relations_current.loc[parent_sku + children_sku, 'ParentId'] = parent_sku
                                    self.relations_current.loc[parent_sku + children_sku, 'Two Months Ago'] = self.config['DEFAULT_VALUES']['YES'] if (parent_sku + children_sku) in self.loader.relations_two_months.index else self.config['DEFAULT_VALUES']['NO']
                                    self.relations_current.loc[parent_sku + children_sku, 'ChildId'] = children_sku
                                    self.relations_current.loc[parent_sku + children_sku, 'ChildName'] = self.om_new.loc[children_sku, 'Offer Display Name']
                                    self.relations_current.loc[parent_sku + children_sku, 'ChildProvisioningId'] = self.om_new.loc[children_sku, 'Provisioning ID']
                                    if (parent_sku + children_sku) in self.loader.relations_last.index:
                                        self.relations_current.loc[parent_sku + children_sku, 'Last Month'] = self.config['DEFAULT_VALUES']['YES']
                                        self.relations_current.loc[parent_sku + children_sku, 'Reasons'] = self.loader.relations_last.loc[parent_sku + children_sku, 'Reasons']
                                    else:
                                        self.relations_current.loc[parent_sku + children_sku, 'Last Month'] = self.config['DEFAULT_VALUES']['NO']
                                    self.relations_current.loc[parent_sku + children_sku, 'Parent In OM'] = self.config['DEFAULT_VALUES']['YES']
                                    self.relations_current.loc[parent_sku + children_sku, 'Parent Group'] = self.om_new.loc[parent_sku, 'Group']
                                    self.relations_current.loc[parent_sku + children_sku, 'Parent License'] = self.om_new.loc[parent_sku, 'License Type']
                                    self.relations_current.loc[parent_sku + children_sku, 'Parent/Child'] = self.om_new.loc[parent_sku, 'Parent/Child']
                                    self.relations_current.loc[parent_sku + children_sku, 'Parent CMP Category'] = self.om_new.loc[parent_sku, 'CMP Category']
                                    self.relations_current.loc[parent_sku + children_sku, 'Parent Sales Category'] = self.om_new.loc[parent_sku, 'Sales Category']
                                    self.relations_current.loc[parent_sku + children_sku, 'ParentName'] = self.om_new.loc[parent_sku, 'Offer Display Name']
                                    self.relations_current.loc[parent_sku + children_sku, 'ParentProvisioningId'] = self.om_new.loc[parent_sku, 'Provisioning ID']
                                    self.relations_current.loc[parent_sku + children_sku, 'Child License'] = children_data['License Type']
                                    if self.relations_current.loc[parent_sku + children_sku, 'Last Month'] == self.config['DEFAULT_VALUES']['NO']:
                                        self.relations_current.loc[parent_sku + children_sku, 'Child Change'] = self.config['DEFAULT_VALUES']['YES'] if parent_sku in self.loader.relations_last['ParentId'] else self.config['DEFAULT_VALUES']['NO']
                                        self.relations_current.loc[parent_sku + children_sku, 'Parent Change'] = self.config['DEFAULT_VALUES']['YES'] if children_sku in self.loader.relations_last['ChildId'] else self.config['DEFAULT_VALUES']['NO']
                            except:
                                logging.error("Error processing relations for parent_sku {0} and child_sku {1}".format(parent_sku, children_sku))

                    elif self.config['EXTENDED_RELATIONS_MATRIX']:
                        self.relations_current.loc[parent_sku + children_sku, 'ParentId'] = parent_sku
                        self.relations_current.loc[parent_sku + children_sku, 'Two Months Ago'] = self.config['DEFAULT_VALUES']['YES'] if (parent_sku + children_sku) in self.loader.relations_two_months.index else self.config['DEFAULT_VALUES']['NO']
                        self.relations_current.loc[parent_sku + children_sku, 'ChildId'] = children_sku
                        self.relations_current.loc[parent_sku + children_sku, 'ChildName'] = children_data['Offer Display Name']
                        self.relations_current.loc[parent_sku + children_sku, 'ChildProvisioningId'] = children_data['Provisioning ID']
                        if (parent_sku + children_sku) in self.loader.relations_last.index:
                            self.relations_current.loc[parent_sku + children_sku, 'Last Month'] = self.config['DEFAULT_VALUES']['YES']
                            self.relations_current.loc[parent_sku + children_sku, 'Reasons'] = self.loader.relations_last.loc[parent_sku + children_sku, 'Reasons']
                        else:
                            self.relations_current.loc[parent_sku + children_sku, 'Last Month'] = self.config['DEFAULT_VALUES']['NO']
                        self.relations_current.loc[parent_sku + children_sku, 'Parent In OM'] = self.config['DEFAULT_VALUES']['NO']
                        self.relations_current.loc[parent_sku + children_sku, 'Parent Group'] = self.config['DEFAULT_VALUES']['PARENT_NOT_IN_OM']
                        self.relations_current.loc[parent_sku + children_sku, 'Parent License'] = self.config['DEFAULT_VALUES']['PARENT_NOT_IN_OM']
                        self.relations_current.loc[parent_sku + children_sku, 'Parent/Child'] = self.config['DEFAULT_VALUES']['PARENT_NOT_IN_OM']
                        self.relations_current.loc[parent_sku + children_sku, 'Parent CMP Category'] = self.config['DEFAULT_VALUES']['PARENT_NOT_IN_OM']
                        self.relations_current.loc[parent_sku + children_sku, 'Parent Sales Category'] = self.config['DEFAULT_VALUES']['PARENT_NOT_IN_OM']
                        self.relations_current.loc[parent_sku + children_sku, 'ParentName'] = self.config['DEFAULT_VALUES']['PARENT_NOT_IN_OM']
                        self.relations_current.loc[parent_sku + children_sku, 'ParentProvisioningId'] = self.config['DEFAULT_VALUES']['PARENT_NOT_IN_OM']
                        self.relations_current.loc[parent_sku + children_sku, 'Child License'] = self.config['DEFAULT_VALUES']['PARENT_NOT_IN_OM']
                        self.relations_current.loc[parent_sku + children_sku, 'Parent Change'] = self.config['DEFAULT_VALUES']['PARENT_NOT_IN_OM']
                        self.relations_current.loc[parent_sku + children_sku, 'Child Change'] = self.config['DEFAULT_VALUES']['PARENT_NOT_IN_OM']
                        if self.relations_current.loc[parent_sku + children_sku, 'Last Month'] == self.config['DEFAULT_VALUES']['NO']:
                            self.relations_current.loc[parent_sku + children_sku, 'Child Change'] = self.config['DEFAULT_VALUES']['YES'] if parent_sku in self.loader.relations_last['ParentId'] else self.config['DEFAULT_VALUES']['NO']
                            self.relations_current.loc[parent_sku + children_sku, 'Parent Change'] = self.config['DEFAULT_VALUES']['YES'] if children_sku in self.loader.relations_last['ChildId'] else self.config['DEFAULT_VALUES']['NO']

        # Remove corporate addons if charity exists #################################################################################################################################################
        logging.info("Remove corporate addons if charity exists")
        if self.config['REMOVE_CORPORATE_ADDONS_WHEN_CHARITY_EXISTS']:
            for relation_sku, relation_sku_data in self.relations_current[(self.relations_current['Parent License'] == 'Charity') & (self.relations_current['Child License'] == 'Corporate')].iterrows():
                charity_skus = self.om_new[self.om_new['License Type'] == 'Charity']['Offer Display Name'].str.startswith(self.om_new.loc[relation_sku_data['ChildId'], 'Offer Display Name'])
                for charity_sku, charity_sku_data in charity_skus[charity_skus].iteritems():
                    if (relation_sku_data['ParentId'] + charity_sku) in self.relations_current.index and relation_sku in self.relations_current.index:
                        self.relations_current = self.relations_current.drop(index=relation_sku)

    def build_connect_relations(self):
        # Build Connect RM based on the one from MS
        logging.info("Build Connect RM based on the one from MS")
        relations_current_addons2addons = pandas.DataFrame()
        for children_sku, children_data in self.om_new.iterrows():
            if pandas.notna(children_data['Depends On']):
                parents = str(children_data['Depends On']).split(";")
                for parent_sku in parents:
                    if parent_sku in self.om_new.index:
                        if self.om_new.loc[parent_sku, 'Duration'] in self.config['ALLOWED_DURATIONS'] or self.config['INCLUDE_LONG_TERM_SKUS']:
                            try:
                                if self.om_new.loc[parent_sku, 'Duration'] == children_data['Duration'] and self.om_new.loc[parent_sku, 'Group'] in self.config['SKU_GROUPS'] and children_data['License Type'] in self.config['ALLOWED_RELATIONS'][self.om_new.loc[parent_sku, 'License Type']]:
                                    if self.om_new.loc[parent_sku, 'Parent/Child'] == 'Parent':
                                        self.relations_current_connect.loc[parent_sku + children_sku, 'ParentId'] = parent_sku
                                        self.relations_current_connect.loc[parent_sku + children_sku, 'Two Months Ago'] = self.config['DEFAULT_VALUES']['YES'] if (parent_sku + children_sku) in self.loader.relations_two_months.index else self.config['DEFAULT_VALUES']['NO']
                                        self.relations_current_connect.loc[parent_sku + children_sku, 'ChildId'] = children_sku
                                        self.relations_current_connect.loc[parent_sku + children_sku, 'ChildName'] = self.om_new.loc[children_sku, 'Offer Display Name']
                                        self.relations_current_connect.loc[parent_sku + children_sku, 'Last Month'] = self.config['DEFAULT_VALUES']['YES'] if (parent_sku + children_sku) in self.loader.relations_last.index else self.config['DEFAULT_VALUES']['NO']
                                        self.relations_current_connect.loc[parent_sku + children_sku, 'Parent License'] = self.om_new.loc[parent_sku, 'License Type']
                                        self.relations_current_connect.loc[parent_sku + children_sku, 'ParentName'] = self.om_new.loc[parent_sku, 'Offer Display Name']
                                        self.relations_current_connect.loc[parent_sku + children_sku, 'Child License'] = children_data['License Type']
                                        if self.relations_current_connect.loc[parent_sku + children_sku, 'Last Month'] == self.config['DEFAULT_VALUES']['NO']:
                                            self.relations_current_connect.loc[parent_sku + children_sku, 'Child Change'] = self.config['DEFAULT_VALUES']['YES'] if parent_sku in self.loader.relations_last['ParentId'] else self.config['DEFAULT_VALUES']['NO']
                                            self.relations_current_connect.loc[parent_sku + children_sku, 'Parent Change'] = self.config['DEFAULT_VALUES']['YES'] if children_sku in self.loader.relations_last['ChildId'] else self.config['DEFAULT_VALUES']['NO']
                                    else:
                                        relations_current_addons2addons.loc[parent_sku + children_sku, 'ParentId'] = parent_sku
                                        relations_current_addons2addons.loc[parent_sku + children_sku, 'ChildId'] = children_sku
                            except:
                                logging.error("Error processing relations for parent_sku {0} and child_sku {1}".format(parent_sku, children_sku))

        for addon2addon_sku, addon2addon_data in relations_current_addons2addons.iterrows():
            for relation_sku, relation_sku_data in self.relations_current_connect[self.relations_current_connect['ChildId'] == addon2addon_data['ParentId']].iterrows():
                self.relations_current_connect.loc[relation_sku_data['ParentId'] + addon2addon_data['ChildId'], 'ParentId'] = relation_sku_data['ParentId']
                self.relations_current_connect.loc[relation_sku_data['ParentId'] + addon2addon_data['ChildId'], 'ChildId'] = addon2addon_data['ChildId']
                self.relations_current_connect.loc[relation_sku_data['ParentId'] + addon2addon_data['ChildId'], 'ChildName'] = self.om_new.loc[addon2addon_data['ChildId'], 'Offer Display Name']
                self.relations_current_connect.loc[relation_sku_data['ParentId'] + addon2addon_data['ChildId'], 'Parent License'] = self.om_new.loc[relation_sku_data['ParentId'], 'License Type']
                self.relations_current_connect.loc[relation_sku_data['ParentId'] + addon2addon_data['ChildId'], 'ParentName'] = self.om_new.loc[relation_sku_data['ParentId'], 'Offer Display Name']
                self.relations_current_connect.loc[relation_sku_data['ParentId'] + addon2addon_data['ChildId'], 'Child License'] = self.om_new.loc[addon2addon_data['ChildId'], 'License Type']
                self.relations_current_connect.loc[relation_sku_data['ParentId'] + addon2addon_data['ChildId'], 'Last Month'] = self.config['DEFAULT_VALUES']['YES'] if (relation_sku_data['ParentId'] + addon2addon_data['ChildId']) in self.loader.relations_last.index else self.config['DEFAULT_VALUES']['NO']
                self.relations_current_connect.loc[relation_sku_data['ParentId'] + addon2addon_data['ChildId'], 'Two Months Ago'] = self.config['DEFAULT_VALUES']['YES'] if (relation_sku_data['ParentId'] + addon2addon_data['ChildId']) in self.loader.relations_two_months.index else self.config['DEFAULT_VALUES']['NO']
    
        # Update last month Relations matrix to update this month's deletes
        for relation_sku, relation_sku_data in self.loader.relations_last.iterrows():
            self.loader.relations_last.loc[relation_sku, 'In next month'] = self.config['DEFAULT_VALUES']['YES'] if relation_sku in self.relations_current.index else self.config['DEFAULT_VALUES']['NO']

    def build_upgrades(self):
        # Build UM
        logging.info("Build UM")
        for sku, sku_data in self.om_new[self.om_new['Parent/Child'] == 'Parent'].sort_values(by='Offer Display Name').iterrows():
            destinations = str(sku_data['Can Convert To']).split(";")
            for destination in destinations:
                if destination in self.om_new.index:
                    self.upgrades_current.loc[sku + destination, "Offer Id origin"] = sku
                    self.upgrades_current.loc[sku + destination, "Offer Display Name"] = sku_data["Offer Display Name"]
                    self.upgrades_current.loc[sku + destination, "Offer Id destination"] = destination
                    self.upgrades_current.loc[sku + destination, "Can Convert To"] = self.om_new.loc[destination, "Offer Display Name"]

    def build_countries(self):
        # Generate this month's Country Matrix
        logging.info("Generating this month's country matrix")
        for sku, sku_data in self.om_new.iterrows():
            countries = str(self.om_new.loc[sku, 'Allowed Countries']).split(";")
            for country in countries:
                if country in self.config['T27'] or country in self.config['SERVICE_PROVIDER_COUNTRIES'] or self.config['EXTENDED_COUNTRY_MATRIX']:
                    self.countries_current.loc[sku + country, 'Last Month'] = self.config['DEFAULT_VALUES']['YES'] if (sku + country) in self.loader.countries_last.index else self.config['DEFAULT_VALUES']['NO']
                    self.countries_current.loc[sku + country, 'New Offer ID'] = self.config['DEFAULT_VALUES']['NO'] if sku in list(dict.fromkeys(self.loader.countries_last['OfferID'])) else self.config['DEFAULT_VALUES']['YES']
                    self.countries_current.loc[sku + country, 'In OM'] = self.config['DEFAULT_VALUES']['YES'] if sku in self.om_new.index else self.config['DEFAULT_VALUES']['NO']
                    self.countries_current.loc[sku + country, 'New Country'] = self.config['DEFAULT_VALUES']['YES'] if (self.countries_current.loc[sku + country, 'New Offer ID'] == self.config['DEFAULT_VALUES']['NO'] and self.countries_current.loc[sku + country, 'Last Month'] == self.config['DEFAULT_VALUES']['NO']) else self.config['DEFAULT_VALUES']['NO']
                    self.countries_current.loc[sku + country, 'CMP'] = self.config['DEFAULT_VALUES']['T27'] if country in self.config['T27'] else self.config['DEFAULT_VALUES']['OTHER']
                    self.countries_current.loc[sku + country, 'OfferID'] = sku
                    self.countries_current.loc[sku + country, 'OfferName'] = sku_data['Offer Display Name']
                    self.countries_current.loc[sku + country, 'Country'] = country
                    self.countries_current.loc[sku + country, 'Group'] = sku_data['Group']
                    self.countries_current.loc[sku + country, 'Parent/Child'] = sku_data['Parent/Child']
                    self.countries_current.loc[sku + country, 'CMP Category'] = sku_data['CMP Category']
                    self.countries_current.loc[sku + country, 'Parent Category'] = sku_data['Parent Category']
                    self.countries_current.loc[sku + country, 'Sales Category'] = sku_data['Sales Category']

        # Update last month Country matrix to update this month's deletes
        for country_sku, country_sku_data in self.loader.countries_last.iterrows():
            if country_sku in self.countries_current.index:
                self.loader.countries_last.loc[country_sku, "In Next Month"] = self.config['DEFAULT_VALUES']['YES']
                self.loader.countries_last.loc[country_sku, 'Offer Next Month'] = self.config['DEFAULT_VALUES']['YES']
                self.loader.countries_last.loc[country_sku, 'Country Change Next Month'] = self.config['DEFAULT_VALUES']['NO']
            else:
                self.loader.countries_last.loc[country_sku, "In Next Month"] = self.config['DEFAULT_VALUES']['NO']
                self.loader.countries_last.loc[country_sku, 'Offer Next Month'] = self.config['DEFAULT_VALUES']['YES'] if country_sku_data['OfferID'] in list(dict.fromkeys(self.countries_current['OfferID'])) else self.config['DEFAULT_VALUES']['NO']
                self.loader.countries_last.loc[country_sku, 'Country Change Next Month'] = self.config['DEFAULT_VALUES']['YES'] if (country_sku_data["In Next Month"] == self.config['DEFAULT_VALUES']['NO'] and country_sku_data['Offer Next Month'] == self.config['DEFAULT_VALUES']['YES']) else self.config['DEFAULT_VALUES']['NO']

    def build_updates(self):
        # Create DataFrame for this month's Adds
        om_new_filtered = self.om_new[self.om_new['In Last Month OM'] == self.config['DEFAULT_VALUES']['NO']]
        for group in self.config['SKU_GROUPS']:
            for sku, sku_data in om_new_filtered[om_new_filtered['Group'] == group].sort_values(by='Offer Display Name').iterrows():
                self.om_add_this_month = self.om_add_this_month.append({"OfferID": sku, "Offer Display Name": sku_data['Offer Display Name'], "Group": group}, ignore_index=True)

        # Create DataFrame for this month's Deletes
        om_last_filtered = self.loader.om_last[self.loader.om_last['In Next Month OM'] == self.config['DEFAULT_VALUES']['NO']]
        for group in self.config['SKU_GROUPS']:
            for sku, sku_data in om_last_filtered[om_last_filtered['Group'] == group].sort_values(by='Offer Display Name').iterrows():
                self.om_del_this_month = self.om_del_this_month.append({"OfferID": sku, "Offer Display Name": sku_data['Offer Display Name'], "Group": group}, ignore_index=True)

        # Create DataFrame for this month's Name Changes
        om_new_filtered = self.om_new[self.om_new['Name Change'] == self.config['DEFAULT_VALUES']['YES']]
        for group in self.config['SKU_GROUPS']:
            for sku, sku_data in om_new_filtered[om_new_filtered['Group'] == group].sort_values(by='Offer Display Name').iterrows():
                self.om_name_changes_this_month = self.om_name_changes_this_month.append({"OfferID": sku, "Offer Display Name": sku_data['Offer Display Name'], "Group": group, "Old Name": sku_data['Old Name']}, ignore_index=True)
    
    def build_ms_errors(self):
        # Create DataFrame for Microsoft errors
        om_new_filtered = self.om_new[self.om_new['Group'] == self.config['DEFAULT_VALUES']['MICROSOFT_ERROR']]
        for sku, sku_data in om_new_filtered.sort_values(by='Offer Display Name').iterrows():
            self.om_microsoft_errors = self.om_microsoft_errors.append({"OfferID": sku, "Offer Display Name": sku_data['Offer Display Name'], "In current PL": sku_data['In current PL'], "In Next Month PL": sku_data['In Next Month PL'], "In Last Month PL": sku_data['In Last Month PL'], "In Two Months Ago PL": sku_data['In Two Months Ago PL']}, ignore_index=True)

    def build_ms_countries(self):
        # Create Microsoft country availability matrix
        for sku, sku_data in self.om_new.iterrows():
            self.country_availability.loc[sku, 'Offer Display Name'] = sku_data['Offer Display Name']
            for country in (self.config['T27'] + self.config['SERVICE_PROVIDER_COUNTRIES']):
                self.country_availability.loc[sku, country] = self.config['DEFAULT_VALUES']['YES'] if (sku + country) in self.countries_current.index else self.config['DEFAULT_VALUES']['NO']

    def build_aps_skus(self):
        # Consolidated APS tab
        for group in self.config['SKU_GROUPS']:
            om_new_filtered = self.om_new[self.om_new['Group'] == group].sort_values(by='Offer Display Name')
            common_columns = {'License Type': self.config['SKU_GROUPS'][group]['License Type'], 'Ingram Group': self.config['SKU_GROUPS'][group]['Ingram Group'], 'Family': self.config['SKU_GROUPS'][group]['Family']}
            for sku, sku_data in om_new_filtered[om_new_filtered['Parent/Child'] == 'Parent'].iterrows():
                if sku_data['Duration'] in self.config['ALLOWED_DURATIONS'] or self.config['INCLUDE_LONG_TERM_SKUS']:
                    self.aps_skus = self.aps_skus.append({**common_columns, 'OfferID': sku, 'Parent SKU + RelationID': sku, 'Parent SKU': sku, 'Parent Name': sku_data['Offer Display Name'], 'Parent Name Short': sku_data['Shortened Names'], 'Offer Display Name': sku_data['Offer Display Name'], 'CMP Category': sku_data['CMP Category'], 'SKU In Last Month OM': sku_data['In Last Month OM'], 'Name Change': sku_data['Name Change']}, ignore_index=True)
                    if sku in list(dict.fromkeys(self.relations_current['ParentId'])):
                        addons_sku_list = list(dict.fromkeys(self.relations_current[self.relations_current['ParentId'] == sku]['ChildId']))
                        for relation_sku, relation_sku_data in self.relations_current[self.relations_current['ParentId'] == sku].sort_values(by=['ChildName', 'ChildId']).iterrows():
                            self.aps_skus = self.aps_skus.append({**common_columns, 'OfferID': relation_sku_data['ChildId'], 'RelationID': relation_sku, 'Parent SKU + RelationID': sku + relation_sku, 'Parent SKU': sku, 'Parent Name': sku_data['Offer Display Name'], 'Parent Name Short': sku_data['Shortened Names'], 'Offer Display Name': relation_sku_data['ChildName'], 'CMP Category': self.om_new.loc[relation_sku_data['ChildId'], 'CMP Category'], 'SKU In Last Month OM': self.om_new.loc[relation_sku_data['ChildId'], 'In Last Month OM'], 'Relation In Last Month': relation_sku_data['Last Month'], 'Name Change': self.om_new.loc[relation_sku_data['ChildId'], 'Name Change']}, ignore_index=True)
                            if relation_sku_data['ChildId'] in list(dict.fromkeys(self.relations_current['ParentId'])):
                                for addon_to_addon_sku, addon_to_addon_data in self.relations_current[self.relations_current['ParentId'] == relation_sku_data['ChildId']].sort_values(by=['ChildName', 'ChildId']).iterrows():
                                    if addon_to_addon_data['ChildId'] not in addons_sku_list and self.config['REMOVE_ADDONS2ADDONS_IF_ADDON_EXISTS']:
                                        addon_to_addon_offer_name = addon_to_addon_data['ChildName'] + " for " + addon_to_addon_data['ParentName']
                                        self.aps_skus = self.aps_skus.append({**common_columns, 'OfferID': addon_to_addon_data['ChildId'], 'RelationID': addon_to_addon_sku, 'Parent SKU + RelationID': sku + addon_to_addon_sku, 'Parent SKU': sku, 'Parent Name': sku_data['Offer Display Name'], 'Parent Name Short': sku_data['Shortened Names'], 'Offer Display Name': addon_to_addon_offer_name, 'CMP Category': 'ADDON to ADDON', 'SKU In Last Month OM': self.om_new.loc[addon_to_addon_data['ChildId'], 'In Last Month OM'], 'Relation In Last Month': addon_to_addon_data['Last Month'], 'Name Change': self.om_new.loc[addon_to_addon_data['ChildId'], 'Name Change']}, ignore_index=True)
        self.cat_lic_type = CategoricalDtype(['Corporate','Education','Charity','Trial','Government'],ordered=True)
        self.cat_ingram_group = CategoricalDtype(['O365','EM+S','Windows','M365','D365','Trial'],ordered=True)
        self.aps_skus['License Type'] = self.aps_skus['License Type'].astype(self.cat_lic_type)
        self.aps_skus['Ingram Group'] = self.aps_skus['Ingram Group'].astype(self.cat_ingram_group)
        self.aps_skus.sort_values(by=['License Type','Ingram Group'], inplace=True, ignore_index=True)
        self.aps_skus['Index'] = self.aps_skus.groupby(['License Type', 'Ingram Group']).cumcount()+1
        self.aps_skus.index = np.arange(1, len(self.aps_skus)+1)
	
    def build_connect_skus(self):
        # Consolidated Connect Tab
        for group in self.config['SKU_GROUPS']:
            om_new_filtered = self.om_new[self.om_new['Group'] == group].sort_values(by='Offer Display Name')
            common_columns = {'License Type': self.config['SKU_GROUPS'][group]['License Type'], 'Ingram Group': self.config['SKU_GROUPS'][group]['Ingram Group'],'Family': self.config['SKU_GROUPS'][group]['Family']}
            for parent_sku, parent_sku_data in om_new_filtered[om_new_filtered['Parent/Child'] == 'Parent'].iterrows():
                if parent_sku_data['Duration'] in self.config['ALLOWED_DURATIONS'] or self.config['INCLUDE_LONG_TERM_SKUS']:
                    self.connect_skus = self.connect_skus.append({**common_columns, 'OfferID': parent_sku, 'RelationID': parent_sku, 'Parent SKU + RelationID': parent_sku, 'Parent SKU': parent_sku, 'Parent Name': parent_sku_data['Offer Display Name'], 'Parent Name Short': parent_sku_data['Shortened Names'], 'Connect product category': parent_sku_data['Connect product category'], 'Connect Product Id': parent_sku_data['Connect Product Id'], 'Connect annual item Id': parent_sku_data['Connect annual item Id'], 'Connect annual item name': parent_sku_data['Connect annual item name'], 'Connect monthly item Id': parent_sku_data['Connect monthly item Id'], 'Connect monthly item name': parent_sku_data['Connect monthly item name'], 'Offer Display Name': parent_sku_data['Offer Display Name'], 'SKU In Last Month OM': parent_sku_data['In Last Month OM'], 'Name Change': parent_sku_data['Name Change']}, ignore_index=True)
                    if parent_sku in list(dict.fromkeys(self.relations_current_connect['ParentId'])):
                        for relation_sku, relation_sku_data in self.relations_current_connect[self.relations_current_connect['ParentId'] == parent_sku].sort_values(by=['ChildName', 'ChildId']).iterrows():
                            relation_sku_based_on_license_type = relation_sku_data['ChildId'] + '_' + parent_sku_data['License Type']
                            if relation_sku_based_on_license_type in self.connect_items.index:
                                self.connect_skus = self.connect_skus.append({**common_columns, 'OfferID': relation_sku_data['ChildId'], 'RelationID': relation_sku, 'Parent SKU + RelationID': parent_sku + relation_sku, 'Parent SKU': parent_sku, 'Parent Name': parent_sku_data['Offer Display Name'], 'Parent Name Short': parent_sku_data['Shortened Names'], 'Connect product category': 'ADD-ON', 'Connect Product Id': self.connect_items.loc[relation_sku_based_on_license_type, 'Connect product'], 'Connect annual item Id': self.connect_items.loc[relation_sku_based_on_license_type, 'Annual item'], 'Connect annual item name': self.connect_items.loc[relation_sku_based_on_license_type, 'Annual name'], 'Connect monthly item Id': self.connect_items.loc[relation_sku_based_on_license_type, 'Monthly item'], 'Connect monthly item name': self.connect_items.loc[relation_sku_based_on_license_type, 'Monthly name'], 'Offer Display Name': relation_sku_data['ChildName'], 'SKU In Last Month OM': self.om_new.loc[relation_sku_data['ChildId'], 'In Last Month OM'], 'Name Change': self.om_new.loc[relation_sku_data['ChildId'], 'Name Change']}, ignore_index=True)
                            else:
                                self.connect_skus = self.connect_skus.append({**common_columns, 'OfferID': relation_sku_data['ChildId'], 'RelationID': relation_sku, 'Parent SKU + RelationID': parent_sku + relation_sku, 'Parent SKU': parent_sku, 'Parent Name': parent_sku_data['Offer Display Name'], 'Parent Name Short': parent_sku_data['Shortened Names'], 'Offer Display Name': relation_sku_data['ChildName'], 'Connect product category': 'ADD-ON', 'SKU In Last Month OM': self.om_new.loc[relation_sku_data['ChildId'], 'In Last Month OM'], 'Name Change': self.om_new.loc[relation_sku_data['ChildId'], 'Name Change']}, ignore_index=True)
        self.connect_skus['License Type'] = self.connect_skus['License Type'].astype(self.cat_lic_type)
        self.connect_skus['Ingram Group'] = self.connect_skus['Ingram Group'].astype(self.cat_ingram_group)
        self.connect_skus.sort_values(by=['License Type','Ingram Group'], inplace=True, ignore_index=True)
        self.connect_skus['Index'] = self.connect_skus.groupby(['License Type', 'Ingram Group']).cumcount()+1
        self.connect_skus.index = np.arange(1, len(self.connect_skus)+1)

    def save_files(self):
        logging.info("Saving files")
        with pandas.ExcelWriter(self.om_output_path) as writer:
            self.om_new.to_excel(writer, sheet_name='OM', index_label='OfferId')
            self.loader.om_last.to_excel(writer, sheet_name='OM Last', index_label='OfferId')
            self.relations_current.to_excel(writer, sheet_name='RM', index_label='RelationId')
            self.loader.relations_last.to_excel(writer, sheet_name='RM Last', index_label='RelationId')
            self.relations_current_connect.to_excel(writer, sheet_name='RM Connect', index_label='RelationId')
            self.upgrades_current.to_excel(writer, sheet_name='UM', index_label='UpgradeId')
            self.countries_current.to_excel(writer, sheet_name='CM', index_label='RelationId')
            self.loader.countries_last.to_excel(writer, sheet_name='CM Last', index_label='RelationId')
            self.new_skus_not_in_pl.to_excel(writer, sheet_name='New SKU not in PL', index_label='Sku')
            self.connect_items.to_excel(writer, sheet_name='Connect items', index_label='Sku')
            self.om_add_this_month.to_excel(writer, sheet_name='ADD this month')
            self.om_del_this_month.to_excel(writer, sheet_name='DEL this month')
            self.om_name_changes_this_month.to_excel(writer, sheet_name='Name Changes')
            self.om_microsoft_errors.to_excel(writer, sheet_name='Microsoft Errors')
            self.country_availability.to_excel(writer, sheet_name='Microsoft Country availability', index_label='OfferId')
            self.aps_skus.to_excel(writer, sheet_name='APS SKUs', index_label='Overall Index')
            self.connect_skus.to_excel(writer, sheet_name='Connect SKUs', index_label='Overall Index')