# Import necessary libraries
import pandas as pd
import numpy as np
import re
from sqlalchemy import select
from sqlalchemy import create_engine
import pymssql

# Get the backend db engine access
cnx = create_engine('mssql+pymssql://maclising:d0hw3lc0m3@10.11.134.109:1433/DPCB_STAGING') 
table_name = "epi_2024_coverage"

# Path to your Excel file
EXCEL_FILE = "2024 EPI Coverage_Dashboard partial data from Jan-May.xlsx"

xls = pd.ExcelFile(EXCEL_FILE)

def clean_name(name: str) -> str:
        name = name.strip()
        name = name.lower()
        name = re.sub(r'[^a-z0-9]', '_', name)
        name = re.sub(r'_+', '_', name)
        return name

# Create an empty DataFrame to hold all sheets data
all_data = pd.DataFrame()

# Iterate over each sheet name
for sheet_name in xls.sheet_names:
    df = pd.read_excel(xls, sheet_name=sheet_name)
    df.columns = [clean_name(col) for col in df.columns]

    # Loop through each column and apply string methods
    for col in df.select_dtypes(include='object').columns:
        # Convert column to string
        df[col] = df[col].astype(str)
        
        # Apply the string operations
        df[col] = df[col].str.upper()
        df[col] = df[col].replace(r'^\s*$', np.nan, regex=True)
        df[col] = df[col].replace(r'^(NAN)$', np.nan, regex=True)
        df[col] = df[col].replace(r'^(NAT)$', np.nan, regex=True)
        
    all_data = pd.concat([all_data, df], ignore_index=True)

# Convert specified columns to Int
convert_dtypes_to_int = ['bcg','penta1','penta2','penta3','opv_1','opv_2','opv3','ipv_1','ipv_2',
                         'hepb_w_in_24_hrs_','hepb_after_24_hrs_','rota1','rota2','mcv1','mcv2',
                         'fic','cic','pcv_1','pcv_2','pcv_3']

# Convert specified columns to Int
for column in convert_dtypes_to_int:
    all_data[column] = pd.to_numeric(all_data[column], errors='coerce').fillna(0).astype('int64')


# Save to a new Excel file to local
all_data.to_excel('merged_data.xlsx', index=False)

# Push the data to MS SQL (on-premise) Server
print("Uploading data to Microsoft SLQ Server (On-Premise)\n")
all_data.to_sql(table_name, con=cnx, if_exists='replace')
print(f"Uploading the {table_name} data on MS SQL Server (On-Premise) has been completed\n")
