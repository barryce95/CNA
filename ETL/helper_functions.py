import os, uuid
import azure
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
import numpy as np
import openpyxl
from azure.storage.blob import BlobServiceClient
import time

def download_csv_from_cloud_storage(LOCALFILENAME, BLOBNAME, HEADER):
    """
    Function to load csv file from Azure Blob Storage into Pandas Dataframe.

        Inputs: {'LOCALFILENAME': path to local temporary destination (string), 'BLOBNAME': name of storage object we would like to call (string), 'HEADER': number of rows to be considered for header (int)},
        Outputs: {'sample_merge_lookup_all': Dataframe from input Blob Object}
    """
    # Download File from blob and save as Temp File
    t1=time.time()
    blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)
    blob_client_instance = blob_service_client_instance.get_blob_client(CONTAINERNAME, BLOBNAME, snapshot=None)
    with open(LOCALFILENAME, "wb") as my_blob:
        blob_data = blob_client_instance.download_blob()
        blob_data.readinto(my_blob)
    t2=time.time()
    print(("It takes %s seconds to download "+BLOBNAME) % (t2 - t1))


    # Read In Data Frame
    df = pd.read_csv(LOCALFILENAME, header=HEADER)
    # Remove Temporary File
    os.remove(LOCALFILENAME)
    return df

def download_pipe_from_cloud_storage(LOCALFILENAME, BLOBNAME, HEADER):
    """
    Function to load csv file from Azure Blob Storage into Pandas Dataframe.

        Inputs: {'LOCALFILENAME': path to local temporary destination (string), 'BLOBNAME': name of storage object we would like to call (string), 'HEADER': number of rows to be considered for header (int)},
        Outputs: {'df': Dataframe from input Blob Object}
    """
    
    # Download File from blob and save as Temp File
    t1=time.time()
    blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)
    blob_client_instance = blob_service_client_instance.get_blob_client(CONTAINERNAME, BLOBNAME, snapshot=None)
    with open(LOCALFILENAME, "wb") as my_blob:
        blob_data = blob_client_instance.download_blob()
        blob_data.readinto(my_blob)
    t2=time.time()
    print(("It takes %s seconds to download "+BLOBNAME) % (t2 - t1))


    # Read In Data Frame
    df = pd.read_csv(LOCALFILENAME, header=HEADER, sep = '|')
    # Remove Temporary File
    os.remove(LOCALFILENAME)
    return df

def download_excel_from_cloud_storage(LOCALFILENAME, BLOBNAME, HEADER, SHEETNAME):
    """
    Function to load csv file from Azure Blob Storage into Pandas Dataframe.

        Inputs: {'LOCALFILENAME': path to local temporary destination (string), 'BLOBNAME': name of storage object we would like to call (string), 'HEADER': number of rows to be considered for header (int), 'SHEETNAME': name of tab in Excel workbook which data resides},
        Outputs: {'df': Dataframe from input Blob Object}
    """
    # Download File from blob and save as Temp File
    t1=time.time()
    blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)
    blob_client_instance = blob_service_client_instance.get_blob_client(CONTAINERNAME, BLOBNAME, snapshot=None)
    with open(LOCALFILENAME, "wb") as my_blob:
        blob_data = blob_client_instance.download_blob()
        blob_data.readinto(my_blob)
    t2=time.time()
    print(("It takes %s seconds to download "+BLOBNAME) % (t2 - t1))

    # Read In Data Frame
    df = pd.read_excel(LOCALFILENAME, header=HEADER, sheet_name=SHEETNAME)
    # Remove Temporary File
    os.remove(LOCALFILENAME)
    return df






def map_income(df):
    if df['income'] >= 0 and df['income'] <= 25:
        return '$0-$25k'
    elif df['income'] > 25 and df['income'] <= 50:
        return '$25k-$50k'
    elif df['income'] > 50 and df['income'] <= 100:
        return '$50k-$100k'
    elif df['income'] > 100 and df['income'] <= 150:
        return '$100k-$150k'
    elif df['income'] > 150 and df ['income'] <= 250:
        return '$150k-$250k'
    else:
        return '>$250k'

def minority_tract_conditions(df):
    if (df['minority_pct_of_tract'] >= 50) and (df['pct_tract_msa_med_fam_income'] < 100):
        return 1
    else:
        return 0