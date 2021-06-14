import os
import pandas
import logging
from tinydb import Query, TinyDB
from datetime import date
import tabulate
from utils.text_translator import DeepLTranslator

INGRAM_MAX_SEAT_COUNT = 5000

class OMTabBuilder:
    def __init__(self, config, loader, connect_items):
        self.config = config
        self.loader = loader
        self.connect_items = connect_items
        self.db = TinyDB(f'./cache/db_{date.today().strftime("%Y%m%d")}.json')
        self.om_new = pandas.DataFrame(columns=config['OM_HEADERS'])
        self.translator = DeepLTranslator(config)

    def add_common_columns(self, sku, sku_data):
        sku_group = self.om_new.loc[sku, 'Group']
        sku_group_config = self.config['SKU_GROUPS']
        if sku_group == self.config['DEFAULT_VALUES']['MICROSOFT_ERROR'] or sku_group == "Trials":
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
        self.om_new.loc[sku, 'Ingram Max Seat Count'] = INGRAM_MAX_SEAT_COUNT if sku_data['Max Seat Count'] >= INGRAM_MAX_SEAT_COUNT else sku_data['Max Seat Count']
        self.om_new.loc[sku, 'Monthly Billing Model'] = 'Change Before Billing Period'
        self.om_new.loc[sku, 'Monthly Billing Period'] = 'Monthly'
        self.om_new.loc[sku, 'Annual Billing Model'] = 'Change Before Billing Period'
        self.om_new.loc[sku, 'Annual Billing Period'] = 'Annual'
        self.om_new.loc[sku, 'Description for CCPv2 Tile'] = sku_group_config[sku_group]['Description for CCPv2 Tile']




    def add_sku_description_translations(self, sku, sku_data):
        # Copy previous translations from last om or most recent version of the om 
        if sku in self.loader.om_ghost.index or sku in self.loader.om_last.index:
            # TODO check if description has changed and then translate it
            copy_from = self.loader.om_ghost if sku in self.loader.om_ghost.index else self.loader.om_last
            self.om_new.loc[sku, 'Offer Display Description France (FR)'] = copy_from.loc[sku, 'Offer Display Description France (FR)']
            self.om_new.loc[sku, 'Offer Display Description Germany (DE)'] = copy_from.loc[sku, 'Offer Display Description Germany (DE)']
            self.om_new.loc[sku, 'Offer Display Description Italian (IT)'] = copy_from.loc[sku, 'Offer Display Description Italian (IT)']
            self.om_new.loc[sku, 'Offer Display Description Portuguese (PT)'] = copy_from.loc[sku, 'Offer Display Description Portuguese (PT)']
            self.om_new.loc[sku, 'Offer Display Description (Spanish) ES'] = copy_from.loc[sku, 'Offer Display Description (Spanish) ES']
            self.om_new.loc[sku, 'New Translations'] = copy_from.loc[sku, 'New Translations']
        else:
            en_desc = sku_data['Offer Display Description']
            self.om_new.loc[sku, 'Offer Display Description France (FR)'] = self.translator.translate(en_desc, DeepLTranslator.FRENCH)
            self.om_new.loc[sku, 'Offer Display Description Germany (DE)'] = self.translator.translate(en_desc, DeepLTranslator.GERMAN)
            self.om_new.loc[sku, 'Offer Display Description Italian (IT)'] = self.translator.translate(en_desc, DeepLTranslator.ITALIAN)
            self.om_new.loc[sku, 'Offer Display Description Portuguese (PT)'] = self.translator.translate(en_desc, DeepLTranslator.PORTUGUESE)
            self.om_new.loc[sku, 'Offer Display Description (Spanish) ES'] = self.translator.translate(en_desc, DeepLTranslator.SPANISH)
            self.om_new.loc[sku, 'New Translations'] = self.config['DEFAULT_VALUES']['YES']

    def build(self):
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
            self.add_common_columns(sku, sku_data)
            self.add_sku_description_translations(sku, sku_data)
             

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
                self.add_common_columns(sku_in_last_not_in_current, sku_in_last_not_in_current_data)
                self.add_sku_description_translations(sku_in_last_not_in_current, sku_in_last_not_in_current_data)

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
                self.add_common_columns(manual_sku, manual_sku_data)
                self.add_sku_description_translations(manual_sku, manual_sku_data)
        return self.om_new
    
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