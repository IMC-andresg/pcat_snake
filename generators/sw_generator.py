import pandas
import logging

# Generates Software Subscriptions Output file
class SWGenerator:

    def __init__(self, config, loader, connect_items):
        self.config = config
        self.loader = loader
        self.sw_output_path = loader.SW_OUTPUT_PATH
        self.sw_new = pandas.DataFrame(columns=config['SW_HEADERS'])
        self.connect_items = connect_items

    def generate(self):
        self.build_om()
        self.save_files()

    def build_om(self):
        # Load Software Subscriptions
        for sw_sku, sw_sku_data in self.loader.sw_current.iterrows():
            if '{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']) in self.sw_new.index:
                self.sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Allowed Countries'] = '{0};{1}'.format(self.sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Allowed Countries'], sw_sku_data['Regions'])
                self.sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'In Two Months Ago OM'] = self.config['DEFAULT_VALUES']['YES'] if '{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']) in self.loader.sw_two_months.index else self.config['DEFAULT_VALUES']['NO']
            else:
                self.sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Offer Display Name'] = sw_sku_data['SkuTitle']
                self.sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Group'] = 'Software Subscriptions Corporate'
                self.sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'CMP Category'] = self.config['SW_GROUPS'][self.sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Group']]['CMP Category']
                self.sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Parent Category'] = self.config['SW_GROUPS'][self.sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Group']]['Parent Category']
                self.sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Sales Category'] = self.config['SW_GROUPS'][self.sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Group']]['Sales Category']
                self.sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Parent/Child'] = 'Parent'
                self.sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Tax Category'] = self.config['SW_SUBSCRIPTIONS_TAX_CATEGORY']
                self.sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Duration'] = '3 Years' if sw_sku_data['SkuTitle'].endswith('3 year') else '1 Year'
                self.sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Billing Frequency'] = 'One Time'
                self.sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Min Seat Count'] = '1'
                self.sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Max Seat Count'] = '5000'
                self.sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Allowed Countries'] = sw_sku_data['Regions']
                if '{}:{:>4}_Corporate'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']) in self.connect_items.index:
                    self.sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Connect Product Id'] = self.connect_items.loc['{}:{:>4}_Corporate'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Connect product']
                    self.sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Connect product name'] = self.connect_items.loc['{}:{:>4}_Corporate'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Product name']
                    self.sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Connect product category'] = self.connect_items.loc['{}:{:>4}_Corporate'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Product category']
                    if pandas.notna(self.connect_items.loc['{}:{:>4}_Corporate'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Onetime item']):
                        self.sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Connect onetime item Id'] = self.connect_items.loc['{}:{:>4}_Corporate'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Onetime item']
                    if pandas.notna(self.connect_items.loc['{}:{:>4}_Corporate'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Onetime name']):
                        self.sw_new.loc['{}:{:>4}'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Connect onetime item name'] = self.connect_items.loc['{}:{:>4}_Corporate'.format(sw_sku_data['ProductId'], sw_sku_data['SkuId']), 'Onetime name']
        
        # Update last month OM to update this month's deletes 
        for sw_last_sku, sw_last_sku_data in self.loader.sw_last.iterrows():
            if sw_last_sku in self.sw_new.index:
                self.sw_new.loc[sw_last_sku, 'In Last Month OM'] = self.config['DEFAULT_VALUES']['YES']
                self.loader.sw_last.loc[sw_last_sku, 'In Next Month OM'] = self.config['DEFAULT_VALUES']['YES']
                self.sw_new.loc[sw_last_sku, 'Shortened Names'] = sw_last_sku_data['Shortened Names']
                self.sw_new.loc[sw_last_sku, 'Length'] = len(str(self.sw_new.loc[sw_last_sku, 'Shortened Names']))
            else:
                self.sw_new.loc[sw_last_sku, 'In Last Month OM'] = self.config['DEFAULT_VALUES']['NO']
                self.loader.sw_last.loc[sw_last_sku, 'In Next Month OM'] = self.config['DEFAULT_VALUES']['NO']

    def save_files(self):
        logging.info("Saving SW output")
        with pandas.ExcelWriter(self.sw_output_path) as writer:
            self.sw_new.sort_values(by='Offer Display Name').to_excel(writer, sheet_name='SW', index_label='OfferID')
            self.loader.sw_last.sort_values(by='Offer Display Name').to_excel(writer, sheet_name='SW LAST', index_label='OfferID')
            self.connect_items.to_excel(writer, sheet_name='Connect items', index_label='Sku')