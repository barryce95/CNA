import os, uuid
import azure
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
import numpy as np
import openpyxl
from azure.storage.blob import BlobServiceClient
import time

os.chdir("C:\\Users\\cbarry\\PycharmProjects\\CNA\\ETL")
exec(open("config.py").read())
import helper_functions
from helper_functions import download_csv_from_cloud_storage, download_excel_from_cloud_storage, map_income, minority_tract_conditions


### Download Census Data from Azure Cloud Storage

#Read in Flat File
census_2022 = download_csv_from_cloud_storage('temp_df', census, None)
census_2022.columns = census_2022.columns+1

#Read in Data Dictionary
census_2022_dd = download_excel_from_cloud_storage('temp_df', census_dd, 0, "Data Dictionary")
census_2022_dd.Index = census_2022_dd.Index.fillna(0).astype(int)
census_2022_dd = census_2022_dd.set_index('Index')

#Update Column Names from Data Dictionary
new_col_list = []
for col in census_2022.columns:
    new_col_list.append(census_2022_dd.loc[col].Description)

census_2022.columns = new_col_list
census_2022.columns = census_2022.columns.str.lstrip('Key field. ')

#Create census_tract_merge column to be used in joining Census and HMDA data
census_2022['census_tract_merge'] = (census_2022['FIPS state code'].astype(str).str.pad(width=2, side='left', fillchar='0') + 
                                    census_2022['FIPS county code'].astype(str).str.pad(width=3, side='left', fillchar='0') + 
                                    census_2022['Census tract. Implied decimal point.'].astype(str).str.pad(width=6, side='left', fillchar='0'))



### Read in HMDA Data
hmda = download_csv_from_cloud_storage('temp_df', lar, 0)
hmda.census_tract = hmda.census_tract.fillna(0).astype(np.int64)
hmda = hmda[hmda['census_tract']!=0]
hmda['census_tract_merge'] = hmda.census_tract.astype(str).str.pad(width=3, side='left', fillchar='0')

### Merging Data Sources
sample_census = census_2022[census_2022['FIPS state code'] == state_fips_code]

sample_census = census_2022[cols_census]
sample_census = sample_census.rename(columns=cols_census_dict)

sample_hmda = hmda[cols_hmda]

sample_merge = sample_hmda.merge(sample_census, on = 'census_tract_merge', how='left')

### Build Out minority_census_tract column and income_bin
sample_merge_clean = sample_merge[sample_merge.minority_pct_of_tract.notnull()]
sample_merge_clean.income = sample_merge_clean.income.fillna(0)

sample_merge_clean['minority_census_tract'] = sample_merge_clean.apply(minority_tract_conditions, axis=1)
sample_merge_clean['income_bin'] = sample_merge_clean.apply(map_income, axis=1)



### Write Out Data to CSV file locally
os.chdir("C:\\Users\\cbarry\\PycharmProjects\\CNA\\Data")
sample_merge_clean.to_csv(outfile)