import pandas as pd
import numpy as np
import os
import pickle
import socket
from sqlalchemy import create_engine
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import credentials
# PostgreSQL connection string
PG_CONNECTION_STRING = credentials.PG_CONNECTION_STRING
engine = create_engine(PG_CONNECTION_STRING)

# Google Sheets API setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = 'credentials.json'
SPREADSHEET_ID = '1OcC8wNyZ9LG-96kU7HfOMZUoHiAAD4MTDX8arKsuKzY'

TABLE_SHEET_MAP = {
    'cpu_cstate_rates': 'CPU C-State Rates - Monthly Avg',
    'disk_iops': 'Disk IOPS - Monthly Avg',
    'disk_throughput': 'Disk Throughput - Monthly Avg',
    'ram_stats': 'RAM Stats - Monthly Avg',
    'disk_free_space': 'Disk Free Space - Monthly Avg',
    'bandwidth': 'Bandwidth - Monthly Avg'
}

def authenticate():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(SERVICE_ACCOUNT_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return creds

def build_sheets_service():
    creds = authenticate()
    return build('sheets', 'v4', credentials=creds)

def recreate_sheet(service, sheet_name):
    spreadsheet = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    existing_sheets = [s['properties'] for s in spreadsheet.get('sheets', [])]
    existing_titles = [s['title'] for s in existing_sheets]

    if sheet_name in existing_titles:
        sheet_id = next((s['sheetId'] for s in existing_sheets if s['title'] == sheet_name), None)
        if sheet_id is not None:
            service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={
                "requests": [{"deleteSheet": {"sheetId": sheet_id}}]
            }).execute()
    service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={
        "requests": [{"addSheet": {"properties": {"title": sheet_name}}}]
    }).execute()

def dataframe_to_sheets_values(df):
    values = [df.columns.tolist()]
    for _, row in df.iterrows():
        values.append([str(x) if isinstance(x, (pd.Timestamp, pd.Period)) else x for x in row])
    return values

def preprocess_month(df):
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df[df['timestamp'].notnull()]
    df['month'] = df['timestamp'].dt.to_period('M').astype(str)
    return df

def process_cpu_utilization(df):
    df = preprocess_month(df)
    if not {'timestamp', 'state', 'rate_per_sec'}.issubset(df.columns):
        return pd.DataFrame()

    total = df.groupby(['timestamp'])['rate_per_sec'].sum().rename('total')
    idle = df[df['state'].isin(['idle', 'iowait'])].groupby('timestamp')['rate_per_sec'].sum().rename('idle')
    merged = pd.concat([total, idle], axis=1).fillna(0)
    merged['cpu_util_percent'] = (100 * (1 - merged['idle'] / merged['total'])).clip(0, 100)
    merged.reset_index(inplace=True)
    merged['month'] = merged['timestamp'].dt.to_period('M').astype(str)
    return merged.groupby('month')['cpu_util_percent'].mean().reset_index()

def process_disk_iops(df):
    df = preprocess_month(df)
    if not {'read_iops', 'write_iops'}.issubset(df.columns):
        return pd.DataFrame()
    df['iops'] = df['read_iops'] + df['write_iops']
    return df.groupby('month')['iops'].mean().reset_index()

def process_disk_throughput(df):
    df = preprocess_month(df)
    if not {'read_bytes_per_sec', 'write_bytes_per_sec'}.issubset(df.columns):
        return pd.DataFrame()
    df['total_throughput'] = df['read_bytes_per_sec'] + df['write_bytes_per_sec']
    df['throughput_mbps'] = df['total_throughput'] * 8 / (1024 ** 2)
    return df.groupby('month')['throughput_mbps'].mean().reset_index()

def process_ram_stats(df):
    df = preprocess_month(df)
    if 'bytes' not in df.columns:
        return pd.DataFrame()
    df['bytes_gb'] = df['bytes'] / (1024 ** 3)
    return df.groupby('month')['bytes_gb'].mean().reset_index()

def process_disk_space(df):
    df = preprocess_month(df)
    if 'bytes' not in df.columns:
        return pd.DataFrame()
    df['bytes_gb'] = df['bytes'] / (1024 ** 3)
    return df.groupby('month')['bytes_gb'].mean().reset_index()

def process_bandwidth(df):
    df = preprocess_month(df)
    if 'value' not in df.columns:
        return pd.DataFrame()
    df['value_mbps'] = df['value'] * 8 / (1024 ** 2)
    return df.groupby('month')['value_mbps'].mean().reset_index()

def main():
    service = build_sheets_service()

    processors = {
        'cpu_cstate_rates': process_cpu_utilization,
        'disk_iops': process_disk_iops,
        'disk_throughput': process_disk_throughput,
        'ram_stats': process_ram_stats,
        'disk_free_space': process_disk_space,
        'bandwidth': process_bandwidth
    }

    for table, sheet_name in TABLE_SHEET_MAP.items():
        try:
            df = pd.read_sql(f"SELECT * FROM {table};", engine)
            if df.empty:
                print(f"⚠️ Empty DataFrame for {table}")
                continue
            processed = processors[table](df)
            if processed.empty:
                print(f"⚠️ No processed data for {table}")
                continue
            recreate_sheet(service, sheet_name)
            values = dataframe_to_sheets_values(processed)
            service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{sheet_name}'!A1",
                valueInputOption='USER_ENTERED',
                body={'values': values}
            ).execute()
            print(f"✅ Uploaded: {sheet_name}")
        except Exception as e:
            print(f"❌ Error processing {table}: {e}")

main()