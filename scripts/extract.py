import httpx
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime
import pandas as pd


class Extract:
    def __init__(self):
        self._get_faostat_data()
        self._get_info_countries_data()
    def _get_faostat_data(self):
        response=httpx.get('https://fenixservices.fao.org/faostat/static/bulkdownloads/Production_Crops_Livestock_E_All_Data.zip')
        zip_file = ZipFile(BytesIO(response.content))

        self.dataset_flags = pd.read_csv(zip_file.open('Production_Crops_Livestock_E_Flags.csv'))
        self.dataset_item_codes = pd.read_csv(zip_file.open('Production_Crops_Livestock_E_ItemCodes.csv'),
                                         encoding_errors='ignore')
        self.dataset_area_codes=pd.read_csv(zip_file.open('Production_Crops_Livestock_E_AreaCodes.csv'), encoding_errors='ignore')
        self.all_data_faostat=pd.read_csv(zip_file.open('Production_Crops_Livestock_E_All_Data.csv'), encoding_errors='ignore')

    def _get_info_countries_data(self):
        response = httpx.get(
            'https://population.un.org/wpp/Download/Files/1_Indicators (Standard)/CSV_FILES/WPP2022_Demographic_Indicators_Medium.zip')
        zip_file = ZipFile(BytesIO(response.content))

        self.infos_countries = pd.read_csv(zip_file.open('WPP2022_Demographic_Indicators_Medium.csv'))

    def change_columns_year(self):
        """
        Change columns of faostat representing years for rows.
        """
        self.all_data_faostat = self.all_data_faostat.melt(
            id_vars=['Area Code', 'Area Code (M49)', 'Area', 'Item Code', 'Item Code (CPC)',
                     'Item', 'Element Code', 'Element', 'Unit'], var_name='Year', value_name="Value")

    def save_data_raw(self):
        now = datetime.now()
        date_time_str = now.strftime("%m-%d-%Y")

        self.all_data_faostat.to_csv(f'/opt/data/data_raw/all_data_faostat_{date_time_str}.csv')
        self.infos_countries.to_csv(f'/opt/data/data_raw/infos_countries_{date_time_str}.csv')