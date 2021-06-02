import pandas
import logging
from datetime import date
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path

# Generates CSP License Based output file
class PastMonthsLoader:
    def __init__(self, config):
        self.config = config

    def init_paths(self):
        # Determine running month & files
        default_month = date.today().strftime("%Y%m")
        entered_text = input(f"Enter month to process [Default: {default_month}]: ").strip()
        try:
            if entered_text == "":
                selected_month_year_str = default_month
                selected_month_year = date.today()
            else:
                selected_month_year_str = entered_text
                selected_month_year = datetime.strptime(entered_text, '%Y%m')
        except ValueError:
            raise ValueError("Incorrect data format, should be YYYYMM")

        one_month_ago_str = (selected_month_year + relativedelta(months=-1)).strftime("%Y%m")
        two_months_ago_str = (selected_month_year + relativedelta(months=-2)).strftime("%Y%m")
        next_month_year_str = (selected_month_year + relativedelta(months=+1)).strftime("%Y%m")


        self.OM_THIS_MONTH_PATH = self.config['OM_PATH_TMPL'].replace('<MONTH_YEAR>', selected_month_year_str)
        self.OM_LAST_MONTH_PATH = self.config['OUT_PATH_TMPL'].replace('<MONTH_YEAR>', one_month_ago_str)
        self.OM_TWO_MONTHS_PATH = self.config['OUT_PATH_TMPL'].replace('<MONTH_YEAR>', two_months_ago_str)
        self.PL_THIS_MONTH_PATH = self.config['PL_PATH_TMPL'].replace('<MONTH_YEAR>', selected_month_year_str)
        self.PL_NEXT_MONTH_PREVIEW_PATH = self.config['PL_PREV_PATH_TMPL'].replace('<PREV_MONTH_YEAR>', next_month_year_str).replace('<MONTH_YEAR>', selected_month_year_str)
        self.PL_LAST_MONTH_PATH = self.config['PL_PATH_TMPL'].replace('<MONTH_YEAR>', one_month_ago_str)
        self.PL_TWO_MONTHS_PATH = self.config['PL_PATH_TMPL'].replace('<MONTH_YEAR>', two_months_ago_str)
        self.OM_OUTPUT_PATH = self.config['OUT_PATH_TMPL'].replace('<MONTH_YEAR>', selected_month_year_str)
        self.SW_THIS_MONTH_PATH = self.config['SW_PATH_TMPL'].replace('<MONTH_YEAR>', selected_month_year_str)
        self.SW_LAST_MONTH_PATH = self.config['SW_OUT_PATH_TMPL'].replace('<MONTH_YEAR>', one_month_ago_str)
        self.SW_TWO_MONTHS_PATH = self.config['SW_OUT_PATH_TMPL'].replace('<MONTH_YEAR>', two_months_ago_str)
        self.SW_OUTPUT_PATH = self.config['SW_OUT_PATH_TMPL'].replace('<MONTH_YEAR>', selected_month_year_str)

    def load_files(self):
        logging.info("Load OM & PL files")
        self.om_current = pandas.read_excel(self.OM_THIS_MONTH_PATH, sheet_name='Office_Dynamics_Windows_Intune', index_col=1)
        self.om_last = pandas.read_excel(self.OM_LAST_MONTH_PATH, sheet_name='OM', index_col=0)
        self.om_two_months = pandas.read_excel(self.OM_TWO_MONTHS_PATH, sheet_name='OM', index_col=0)

        self.ghost_file = Path(self.OM_OUTPUT_PATH)
        if self.ghost_file.is_file():
            self.om_ghost = pandas.read_excel(self.OM_OUTPUT_PATH, sheet_name='OM', index_col=0)
        else:
            self.om_ghost = pandas.DataFrame()

        self.pl_current = self.read_pl_file(self.PL_THIS_MONTH_PATH)
        self.pl_next_month_preview = self.read_pl_file(self.PL_NEXT_MONTH_PREVIEW_PATH)
        self.pl_last = self.read_pl_file(self.PL_LAST_MONTH_PATH)
        self.pl_two_months = self.read_pl_file(self.PL_TWO_MONTHS_PATH)

        self.relations_last = pandas.read_excel(self.OM_LAST_MONTH_PATH, sheet_name='RM', index_col=0)
        self.relations_two_months = pandas.read_excel(self.OM_TWO_MONTHS_PATH, sheet_name='RM', index_col=0)
        self.relations_last_connect = pandas.read_excel(self.OM_LAST_MONTH_PATH, sheet_name='RM Connect', index_col=0)
        self.relations_two_months_connect = pandas.read_excel(self.OM_TWO_MONTHS_PATH, sheet_name='RM Connect', index_col=0)
        self.countries_last = pandas.read_excel(self.OM_LAST_MONTH_PATH, sheet_name='CM', index_col=0)

        self.sw_current = pandas.read_excel(self.SW_THIS_MONTH_PATH)
        self.sw_last = pandas.read_excel(self.SW_LAST_MONTH_PATH, sheet_name='SW', index_col=0)
        self.sw_two_months = pandas.read_excel(self.SW_TWO_MONTHS_PATH, sheet_name='SW', index_col=0)

        # Update last month OM to update this month's deletes
        for sku, sku_data in self.om_last.iterrows():
	        self.om_last.loc[sku, 'In Next Month OM'] = self.config['DEFAULT_VALUES']['YES'] if sku in self.om_current.index else self.config['DEFAULT_VALUES']['NO']

    def read_pl_file(self, file_path):
        sheet_map = pandas.read_excel(file_path, sheet_name=None)
        mdf = pandas.concat(sheet_map, axis=0, ignore_index=True)
        return mdf.drop_duplicates(subset=['Offer ID'],ignore_index=True).set_index('Offer ID')