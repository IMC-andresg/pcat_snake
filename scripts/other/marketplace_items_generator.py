#!/usr/bin/env python
# coding: utf-8

"""
This script is used to generate NCE Commercial Marketplace SKUs suitable for Connect import.
It parses the offer matrix CSV file provided by Microsoft and generates an excel file with all SKUs.

USAGE:
1. Install required dependencies: pip install -r requirements.txt
2. Prepare / download offer matrix input file. You will need to pass the path as a command line argument
3. Run the tool: python marketplace_items_generator.py -i om_input.csv -o output.xlsx
"""

import pandas as pd
from pandas import Series, DataFrame
from argparse import ArgumentParser


def fix_trial_billing_freq(row):
    if row['Term'] == '1 Month Trial to 1 Year Subscription':
        return 'Annual'
    else:
        return row['Billing Frequency']

def update_MPN(row):
    if row['Billing Frequency'] == 'Annual':
        return row['ProductId']+":"+row['SkuId']+"_P1Y:"+row['Billing Frequency']
    if row['Billing Frequency'] == 'Monthly':
        return row['ProductId']+":"+row['SkuId']+"_P1M:"+row['Billing Frequency']
    return ""

def has_trial(x):
    if (len(x) > 1):
        return 'True'
    else:
        return 'False'

def build_sku_title(row):
    title = row['SkuTitle']
    billing_freq_labels = {'Annual': '1YR ANN', 'Monthly': '1MO MTH'}
    prefix = []
    postfix = []
    prefixes = ''
    postfixes = ''
    if row['Publisher'] not in title and row['Publisher'] not in row['ProductTitle']:
        prefix.append(row['Publisher'])
    if row['ProductTitle'] not in title and row['ProductTitle'] not in prefix:
        prefix.append(row['ProductTitle'])
    if row['Billing Frequency'] not in title:
        postfix.append(billing_freq_labels[row['Billing Frequency']])
    if row['HasTrial'] == 'True':
        postfix.append('+Trial')
    if len(prefix) > 0:
        prefixes = '-'.join(prefix)+' '
    if len(postfix) > 0:
        postfixes = ' ('+' '.join(postfix)+')'
    return prefixes + title + postfixes

parser = ArgumentParser()
parser.add_argument("-i", "--input", dest="inputfile", required=True,
                    help="commercial marketplace offer matrix file")
parser.add_argument("-o", "--output", dest="outputfile", required=True,
                    help="output file")
args = parser.parse_args()

inputfile_path = args.inputfile
outputfile_path = args.outputfile
om_data = pd.read_csv(inputfile_path)
om_data = om_data[(om_data['SubType'] == 'SaaS') & (om_data['Billing Frequency'] != 'OneTime')]
om_data.insert(1,'MPN','')
om_data.insert(4,'HasTrial','')
om_data = om_data.drop(columns=['Type', 'SubType', 'Purchase Unit','Regions','Segment','Currency','ListPrice'])

om_data['Billing Frequency'] = om_data.apply(lambda row: fix_trial_billing_freq(row), axis=1)
om_data['MPN'] = om_data.apply(lambda row: update_MPN(row), axis=1)
om_data = om_data.groupby(['MPN']).agg({
    'ProductTitle':'first',
    'SkuTitle': 'first',
    'Publisher':'first',
    'Description':'first',
    'Billing Frequency': 'first',
    'MSRP': 'max',
    'HasTrial': has_trial
})
om_data['SkuTitle'] = om_data.apply(lambda row: build_sku_title(row), axis=1)
om_data = om_data.sort_values(by=['Publisher', 'MPN'])
om_data.to_excel(outputfile_path, engine='xlsxwriter')  

# TODO handle duplicate named SKUs like DZH318Z08C2N 
print(f'Total SKUs Exported: {len(om_data)}')