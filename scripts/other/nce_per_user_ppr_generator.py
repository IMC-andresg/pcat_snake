#!/usr/bin/env python
# coding: utf-8

"""
This script is used to generate partial PPR file for Microsoft NCE Per-User SKUs.

USAGE:
1. Install required dependencies: pip install -r requirements.txt
2. Prepare / download offer matrix and price list input files. You will need to pass the paths as command line arguments
3. Run the tool: python nce_per_user_ppr_generator.py -om om_input.csv -pl pl_input.csv -o output.xlsx
"""

import pandas as pd
from pandas import Series, DataFrame
from argparse import ArgumentParser

def build_connect_name(row):
    name = row['SkuTitle']+" "
    if 'Trial'in row['Tags']:
        name += 'Trial '
    name += '(NCE COM '
    if row['TermDuration'] == 'P1Y':
        name += "1YR "
    elif row['TermDuration'] == 'P1M':
        name += "1MO "
    elif row['TermDuration'] == 'P3Y':
        name += "3YR "
    
    if row['BillingPlan'] == 'Annual':
        name += "ANN)"
    elif row['BillingPlan'] == 'Triennial':
        name += "TRI)"
    elif row['BillingPlan'] == 'Monthly':
        name += "MTH)"
    
    return name

def resource_rates_min(row):
    mpn = row['MPN'][:17]
    om_row = om_df[om_df['MPN']==mpn].MinLicenses
    return om_row.values[0]

def resource_rates_max(row):
    mpn = row['MPN'][:17]
    om_row = om_df[om_df['MPN']==mpn].MaxLicenses
    if om_row.values[0] >= 10000000:
        return -1
    return om_row.values[0]
parser = ArgumentParser()
parser.add_argument("-om", "--offermatrix", dest="omfile", required=True,
                    help="NCE per-user offer matrix file")
parser.add_argument("-pl", "--pricelist", dest="plfile", required=True,
                    help="NCE per-user price list file")
parser.add_argument("-o", "--output", dest="outputfile", required=True,
                    help="output file")
args = parser.parse_args()

om_file_path = args.omfile
pl_file_path = args.plfile
outputfile_path = args.outputfile

om_df = pd.read_csv(om_file_path)
pl_df = pd.read_csv(pl_file_path)

om_df['MPN'] = om_df['ProductId']+":"+om_df['SkuId']
om_df["ProductSkuConversion"] = om_df["ProductSkuConversion"].astype(str)
om_df["ProductSkuConversion"] = om_df["ProductSkuConversion"].apply(lambda x: x.replace("/", ":"))

# Subset and Filter out expired SKUs

pl_df = pl_df[['ProductId','SkuId','SkuTitle','SkuDescription','TermDuration', 'BillingPlan','EffectiveEndDate','Tags','ERP Price']]
pl_df = pl_df[pl_df['EffectiveEndDate']=='11/30/9999 11:59:59 PM'].drop(columns=['EffectiveEndDate'])

# Add MPN 
pl_df['MPN'] = pl_df['ProductId']+":"+pl_df['SkuId']+"_"+pl_df['TermDuration']+":"+pl_df['BillingPlan']
pl_df['Connect Name'] = pl_df.apply(lambda row: build_connect_name(row), axis=1)

# Resource Rates
pl_df['MinUnits'] = pl_df.apply(lambda row: resource_rates_min(row), axis=1)
pl_df['MaxAmount'] = pl_df.apply(lambda row: resource_rates_max(row), axis=1)
pl_df = pl_df.sort_values('Connect Name')

# Upgrade Paths
rows_list = []
om_upgrades = (om_df[om_df['ProductSkuConversion']!=om_df['MPN']])[['MPN','ProductSkuConversion']]
for om_up_index, om_up_row in om_upgrades.iterrows():
    if om_up_row['ProductSkuConversion'] == 'nan':
        continue
    from_skus = pl_df[pl_df['MPN'].str.startswith(om_up_row['MPN'])][['MPN','Connect Name','BillingPlan', 'TermDuration']]
    to_skus = DataFrame()
    om_up_list = om_up_row['ProductSkuConversion'].split(",")
    for i in om_up_list:
        to_skus = to_skus.append(pl_df[pl_df['MPN'].str.startswith(i)][['MPN','Connect Name','BillingPlan', 'TermDuration']])
    for from_sku_index, from_sku_row in from_skus.iterrows():
        for to_sku_index, to_sku_row in to_skus.iterrows():
            if from_sku_row['Connect Name'] != to_sku_row['Connect Name']:
                if from_sku_row['BillingPlan'] == 'Annual' and to_sku_row['BillingPlan'] != 'Annual':
                    continue
                up_row = {'From Plan': from_sku_row['Connect Name'],'To Plan': to_sku_row['Connect Name']}
                rows_list.append(up_row)

up_df = DataFrame(rows_list)
up_df.sort_values(['From Plan','To Plan'])

# Res dependncies
rows_list = []
om_limits = (om_df[om_df['AssetOwnershipLimit']>0])[['MPN','SkuTitle','AssetOwnershipLimit','AssetOwnershipLimitType']]
for om_limit_index, om_limit_row in om_limits.iterrows():
    skus = pl_df[pl_df['MPN'].str.startswith(om_limit_row['MPN'])][['MPN','Connect Name','BillingPlan', 'TermDuration']]
    if len(skus) == 0:
        print("No active SKU found in PL: "+ om_limit_row['SkuTitle'])
    for sku1_index, sku1_row in skus.iterrows():
        for sku2_index, sku2_row in skus.iterrows():
            limit_row = {'Resource #1': sku1_row['Connect Name'],'Resource #2': sku2_row['Connect Name'],'Dependence Kind': 'Conflicts on Account Level'}
            rows_list.append(limit_row)
limit_df = DataFrame(rows_list)
limit_df.sort_values(['Resource #1','Resource #2'])

with pd.ExcelWriter(outputfile_path) as writer:  
    pl_df.to_excel(writer, sheet_name='items')
    up_df.to_excel(writer, sheet_name='upgrades')
    limit_df.to_excel(writer, sheet_name='limits')
