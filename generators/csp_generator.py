import pandas
import numpy as np
import logging
import traceback
from pandas.api.types import CategoricalDtype
from generators.om_tab_builder import OMTabBuilder


# Generates CSP License Based output file
class CSPGenerator:

    def __init__(self, config, loader, connect_items):
        self.config = config
        self.loader = loader
        self.connect_items = connect_items
        self.om_output_path = loader.OM_OUTPUT_PATH
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
        self.om_new = OMTabBuilder(self.config, self.loader, self.connect_items).build()
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
        logging.info("Generating this month's APS SKU Tab")
        for group in self.config['SKU_GROUPS']:
            om_new_filtered = self.om_new[self.om_new['Group'] == group].sort_values(by='Offer Display Name')
            license_type = self.config['SKU_GROUPS'][group]['License Type']
            common_columns = {'License Type': license_type, 'Ingram Group': self.config['SKU_GROUPS'][group]['Ingram Group'], 'Family': self.config['SKU_GROUPS'][group]['Family']}
            country_specific_name =''            
            name_change = ''     
            sku_rel_id_parent = ''       
            for sku, sku_data in om_new_filtered[om_new_filtered['Parent/Child'] == 'Parent'].iterrows():                
                if sku_data['Duration'] in self.config['ALLOWED_DURATIONS'] or self.config['INCLUDE_LONG_TERM_SKUS']:
                    # Calculating MAX seat count for Ingram
                    max_im_seat_count = sku_data['Max Seat Count']
                    if max_im_seat_count > 5000:
                        max_im_seat_count = 5000
                    #Rows for Parent Plan
                    sku_rel_id = sku               
                    if sku_rel_id in self.loader.aps_skus_last['PPC Dup'].values:
                        aps_skus_last_name = self.loader.aps_skus_last[self.loader.aps_skus_last['PPC Dup'] ==  sku_rel_id].iloc[0]['Name for Ingram']                        
                    else:
                        aps_skus_last_name = 'Not In Last Month' 
                    if sku_data['Offer Display Name'] != aps_skus_last_name:
                        name_change = 'YES'
                    else:
                        name_change = 'NO'                                               
                    additional_column = {'PPC Dup': sku,**common_columns, 'OfferID': sku,'Offer Name (From Microsoft Offers Matrix)':sku_data['Offer Display Name'],'Name for Ingram':sku_data['Offer Display Name'],'Old Offer Name':aps_skus_last_name, 'Parent SKU + RelationID': sku, 'Parent SKU': sku, 'Parent Name': sku_data['Offer Display Name'],'CMP Category': sku_data['CMP Category'], 'SKU In Last Month OM': sku_data['In Last Month OM'],'Included Amount':1,'Lower Limit, Min Seat Count':sku_data['Min Seat Count'],'Max Amount Ingram':max_im_seat_count,'Unit':'User','Subscription Term':sku_data['Duration'],'Name Change': name_change}
                    self.build_country_boxes('parent',additional_column,sku_rel_id,sku,license_type,sku_rel_id_parent)
                    sku_rel_id_parent = sku_rel_id
                    if sku in list(dict.fromkeys(self.relations_current['ParentId'])):
                        addons_sku_list = list(dict.fromkeys(self.relations_current[self.relations_current['ParentId'] == sku]['ChildId']))
                        for relation_sku, relation_sku_data in self.relations_current[self.relations_current['ParentId'] == sku].sort_values(by=['ChildName', 'ChildId']).iterrows():
                            #Rows for Addon Plan
                            if relation_sku_data['ChildId'] in self.config['COUNTRY_SPECIFIC_SKU']:
                                country_specific_name = self.config['COUNTRY_SPECIFIC_SKU'][relation_sku_data['ChildId']]
                            else:
                                country_specific_name = relation_sku_data['ChildName']
                            #print(country_specific_name)    
                            sku_rel_id = sku + relation_sku
                            if sku_rel_id in self.loader.aps_skus_last['PPC Dup'].values:
                                aps_skus_last_name = self.loader.aps_skus_last[self.loader.aps_skus_last['PPC Dup'] ==  sku_rel_id].iloc[0]['Name for Ingram']
                            else:
                                aps_skus_last_name = 'Not In Last Month'                            
                            if country_specific_name != aps_skus_last_name:
                                name_change = 'YES'
                            else:
                                name_change = 'NO'                    
                            additional_column = {'PPC Dup': sku + relation_sku,**common_columns, 'OfferID': relation_sku_data['ChildId'],'Offer Name (From Microsoft Offers Matrix)':relation_sku_data['ChildName'],'Name for Ingram':country_specific_name,'Old Offer Name':aps_skus_last_name, 'RelationID': relation_sku, 'Parent SKU + RelationID': sku + relation_sku, 'Parent SKU': sku, 'Parent Name': sku_data['Offer Display Name'], 'CMP Category': self.om_new.loc[relation_sku_data['ChildId'], 'CMP Category'], 'SKU In Last Month OM': self.om_new.loc[relation_sku_data['ChildId'], 'In Last Month OM'], 'Relation In Last Month': relation_sku_data['Last Month'], 'Included Amount':0,'Lower Limit, Min Seat Count':sku_data['Min Seat Count'],'Max Amount Ingram':max_im_seat_count,'Unit':'User','Subscription Term':sku_data['Duration'],'Name Change': name_change}
                            self.build_country_boxes('addon',additional_column,sku_rel_id,sku,license_type,sku_rel_id_parent) 
                            sku_rel_id_parent = sku_rel_id                   
                            if relation_sku_data['ChildId'] in list(dict.fromkeys(self.relations_current['ParentId'])):
                                for addon_to_addon_sku, addon_to_addon_data in self.relations_current[self.relations_current['ParentId'] == relation_sku_data['ChildId']].sort_values(by=['ChildName', 'ChildId']).iterrows():
                                   #Rows for Addon to Addon Plan
                                   if addon_to_addon_data['ChildId'] not in addons_sku_list and self.config['REMOVE_ADDONS2ADDONS_IF_ADDON_EXISTS']:
                                        addon_to_addon_offer_name = addon_to_addon_data['ChildName'] + " for " + country_specific_name
                                        sku_rel_id = sku + addon_to_addon_sku
                                        if sku_rel_id in self.loader.aps_skus_last['PPC Dup'].values:
                                            aps_skus_last_name = self.loader.aps_skus_last[self.loader.aps_skus_last['PPC Dup'] ==  sku_rel_id].iloc[0]['Name for Ingram']
                                        else:
                                            aps_skus_last_name = 'Not In Last Month'   
                                        if addon_to_addon_offer_name!= aps_skus_last_name:
                                            name_change = 'YES'
                                        else:
                                            name_change = 'NO'                                          
                                        additional_column = {'PPC Dup': sku + addon_to_addon_sku,**common_columns, 'OfferID': addon_to_addon_data['ChildId'],'Offer Name (From Microsoft Offers Matrix)':addon_to_addon_data['ChildName'],'Name for Ingram':addon_to_addon_offer_name,'Old Offer Name':aps_skus_last_name, 'RelationID': addon_to_addon_sku, 'Parent SKU + RelationID': sku + addon_to_addon_sku, 'Parent SKU': sku, 'Parent Name': sku_data['Offer Display Name'], 'CMP Category': 'ADDON to ADDON', 'SKU In Last Month OM': self.om_new.loc[addon_to_addon_data['ChildId'], 'In Last Month OM'], 'Relation In Last Month': addon_to_addon_data['Last Month'],'Included Amount':0,'Lower Limit, Min Seat Count':sku_data['Min Seat Count'],'Max Amount Ingram':max_im_seat_count,'Unit':'User','Subscription Term':sku_data['Duration'], 'Name Change': name_change}
                                        self.build_country_boxes('addon2addon',additional_column,sku_rel_id,sku,license_type,sku_rel_id_parent)                                        
                    
        self.cat_lic_type = CategoricalDtype(['Corporate','Education','Charity','Trial','Government'],ordered=True)
        self.cat_ingram_group = CategoricalDtype(['O365','EM+S','Windows','M365','D365','Trial'],ordered=True)
        self.aps_skus['License Type'] = self.aps_skus['License Type'].astype(self.cat_lic_type)
        self.aps_skus['Ingram Group'] = self.aps_skus['Ingram Group'].astype(self.cat_ingram_group)
        self.aps_skus.sort_values(by=['License Type','Ingram Group'], inplace=True, ignore_index=True)
        self.aps_skus['Index'] = self.aps_skus.groupby(['License Type', 'Ingram Group']).cumcount()+1
        self.aps_skus.index = np.arange(1, len(self.aps_skus)+1)
	
    def build_country_boxes(self, plan_type, additional_column, sku_rel_id, sku, license_type, sku_rel_id_parent):
        # Fill data for all Country Boxes
        logging.debug("Generating this month's APS SKU Country Boxes, SKU Relation id=", sku_rel_id," and SKU id",sku)
        countries = self.config['COUNTRY_LIST']
        country_values = {}
        parent_data_check=''
        for country in countries:
            try:
                if sku_rel_id in self.loader.aps_skus_last['PPC Dup'].values:
                    new_relation = 'NO'  
                    country_value = self.country_data_check(self.loader.aps_skus_last[self.loader.aps_skus_last['PPC Dup'] ==  sku_rel_id].iloc[0][country])                    
                else:
                    new_relation = 'YES'                
                    country_value = ''                                    
                    if self.country_availability.loc[sku,country] == 'YES':
                        if license_type == 'Government':
                            country_value = 'NO'
                        else:
                            if plan_type == "parent":
                                country_value = 'Add If Available'                      
                            elif plan_type == "addon":
                                parent_data_check = self.aps_skus[self.aps_skus['PPC Dup'] == sku_rel_id_parent].iloc[0][country]               
                                if parent_data_check in ['Existing In Country', 'Add If Available']: 
                                    country_value = 'Add If Available'
                                else:
                                    country_value = 'NO'
                                        
                            elif plan_type == "addon2addon": 
                                parent_data_check = self.aps_skus[self.aps_skus['PPC Dup'] ==  sku_rel_id_parent].iloc[0][country]
                                if parent_data_check in ['Existing In Country', 'Add If Available']:
                                    country_value = 'Add If Available'
                                else:
                                    country_value = 'NO'
                    else:
                        country_value = 'NP'       
            except Exception:
                print('---------------------------------------------------------')
                traceback.print_exc()    
                print('---------------------------------------------------------')
            country_values[country] = country_value            
        self.aps_skus = self.aps_skus.append({**additional_column,'New Relation':new_relation,**country_values}, ignore_index=True)
        
    def country_data_check(self,country_data):
        country_data_return=''
        if country_data == 'Add If Available':
            country_data_return = 'Existing In Country'
        elif country_data == 'Delete':
            country_data_return = 'NP'
        elif country_data == 'Name Change':
            country_data_return = 'Existing In Country'        
        else:
            country_data_return = country_data
        return country_data_return    

    def build_aps_skus_last(self):
        # Fill data for Delete list in last month aps skus tab
        countries = self.config['COUNTRY_LIST']
        country_values = {}        
        deleted_list = self.loader.om_del_this_month['OfferID'].values        
        for deleted_sku in deleted_list:
            for sku, sku_data in self.aps_skus_last[self.aps_skus_last.loc['OfferId',deleted_sku]]: 
                for country in countries:
                    deleted = 'YES'
                    country_value = 'Delete'
                    country_values[country] = country_value

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
            self.loader.aps_skus_last.to_excel(writer, sheet_name='APS SKUs Last', index_label='Overall Index')
            self.connect_skus.to_excel(writer, sheet_name='Connect SKUs', index_label='Overall Index')
            