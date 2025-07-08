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
SPREADSHEET_ID = credentials.SPREADSHEET_ID

TABLE_SHEET_MAP = {
    'cpu_cstate_rates': 'CPU C-State Rates',
    'disk_iops': 'Disk IOPS',
    'disk_throughput': 'Disk Throughput',
    'ram_stats': 'RAM Stats',
    'disk_free_space': ['Disk Free Space', 'Disk Free Space - Volumes'],
    'nic_status': 'NIC Status',
    'bandwidth': 'Bandwidth'
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
    print("✅ Authentication successful")
    return creds

def build_sheets_service():
    creds = authenticate()
    service = build('sheets', 'v4', credentials=creds)
    print("✅ Google Sheets service built")
    return service

def ensure_sheet_exists(service, sheet_name):
    spreadsheet = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    existing_sheets = [s['properties']['title'] for s in spreadsheet.get('sheets', [])]
    if isinstance(sheet_name, list):
        for sn in sheet_name:
            if sn not in existing_sheets:
                print(f"🆕 Sheet '{sn}' not found. Creating it...")
                requests = [{"addSheet": {"properties": {"title": sn}}}]
                service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": requests}).execute()
                print(f"✅ Sheet '{sn}' created")
            else:
                print(f"Sheet '{sn}' already exists")
    else:
        if sheet_name not in existing_sheets:
            print(f"🆕 Sheet '{sheet_name}' not found. Creating it...")
            requests = [{"addSheet": {"properties": {"title": sheet_name}}}]
            service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": requests}).execute()
            print(f"✅ Sheet '{sheet_name}' created")
        else:
            print(f"Sheet '{sheet_name}' already exists")

def fetch_table_data(table_name):
    print(f"Fetching data from table '{table_name}'")
    df = pd.read_sql(f"SELECT * FROM {table_name};", engine)
    print(f"Columns in {table_name}: {df.columns.tolist()}")
    print(f"Fetched {len(df)} rows from '{table_name}'")
    if not df.empty:
        print(f"Sample data:\n{df.head(3)}")
    return df

def truncate_timestamp_to_minute(df):
    if 'timestamp' in df.columns:
        df = df.copy()
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df['timestamp'] = df['timestamp'].apply(lambda x: x.floor('min') if pd.notnull(x) else x)
    return df

def dataframe_to_sheets_values(df):
    values = [df.columns.tolist()]
    for _, row in df.iterrows():
        row_values = []
        for x in row:
            if pd.isnull(x):
                row_values.append('')
            elif isinstance(x, (int, float, np.integer, np.floating)):
                row_values.append(x)
            else:
                row_values.append(str(x))
        values.append(row_values)
    return values

def convert_all_but_ip_ts_month_to_numeric(df):
    cols_to_skip = {'ip_address', 'timestamp', 'month'}
    for col in df.columns:
        if col not in cols_to_skip:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def convert_all_but_ip_ts_month_volume_to_numeric(df):
    cols_to_skip = {'ip_address', 'timestamp', 'month', 'volume'}
    for col in df.columns:
        if col not in cols_to_skip:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def update_google_sheet(service, sheet_name, values):
    print(f"Uploading {len(values) - 1} rows to sheet '{sheet_name}'")
    result = service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{sheet_name}'!A1",
        valueInputOption='USER_ENTERED',
        body={'values': values}
    ).execute()
    print(f"✅ Uploaded {result.get('updatedCells')} cells to '{sheet_name}'")

def clear_google_sheet(service, sheet_name):
    print(f"Clearing sheet '{sheet_name}'")
    service.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{sheet_name}'",
        body={}
    ).execute()
    print(f"✅ Cleared '{sheet_name}'")

def add_month_and_ip(df, ip):
    df = df.copy()
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df['month'] = df['timestamp'].apply(lambda x: x.strftime('%Y-%m') if pd.notnull(x) else '')
    df['ip_address'] = ip
    return df

def calculate_cpu_utilization_correct(df):
    df = truncate_timestamp_to_minute(df)
    if not {'timestamp', 'state', 'rate_per_sec'}.issubset(df.columns):
        print("⚠️ Missing required columns for CPU utilization calculation")
        return pd.DataFrame()

    total_rate = df.groupby('timestamp')['rate_per_sec'].sum()
    idle_rate = df[df['state'].isin(['idle', 'iowait'])].groupby('timestamp')['rate_per_sec'].sum()
    cpu_util = 100 * (1 - idle_rate / total_rate)
    cpu_util = cpu_util.clip(lower=0, upper=100).round(1).fillna(0)

    result = pd.DataFrame({
        'timestamp': cpu_util.index,
        'cpu_util_percent': cpu_util.astype(str) + '%',
        'numeric': cpu_util.values,
    })

    print(f"Calculated CPU utilization for {len(result)} timestamps")
    return result

def main():
    local_ip = socket.gethostbyname(socket.gethostname())
    service = build_sheets_service()

    for table_key, sheet_value in TABLE_SHEET_MAP.items():
        print(f"\n🔄 Processing table: {table_key}")
        try:
            df = fetch_table_data(table_key)
            if df.empty:
                print(f"⚠️ No data found in table '{table_key}'. Skipping.")
                continue

            df = truncate_timestamp_to_minute(df)

            if table_key == 'cpu_cstate_rates':
                df = calculate_cpu_utilization_correct(df)
                if df.empty:
                    print("⚠️ No CPU utilization data to upload. Skipping.")
                    continue
                df = add_month_and_ip(df, local_ip)
                df = convert_all_but_ip_ts_month_to_numeric(df)
                ensure_sheet_exists(service, sheet_value)
                clear_google_sheet(service, sheet_value)
                update_google_sheet(service, sheet_value, dataframe_to_sheets_values(df))

            elif table_key == 'disk_iops':
                if {'read_iops', 'write_iops'}.issubset(df.columns):
                    grouped = df.groupby('timestamp')[['read_iops', 'write_iops']].mean().reset_index()
                    grouped['iops_utilization'] = (grouped['read_iops'] + grouped['write_iops']).round().astype(int)
                    df = grouped[['timestamp', 'iops_utilization']]
                    df = add_month_and_ip(df, local_ip)
                    df = convert_all_but_ip_ts_month_to_numeric(df)
                    ensure_sheet_exists(service, sheet_value)
                    clear_google_sheet(service, sheet_value)
                    update_google_sheet(service, sheet_value, dataframe_to_sheets_values(df))
                else:
                    print(f"⚠️ Missing columns 'read_iops' or 'write_iops' in {table_key}")

            elif table_key == 'disk_throughput':
                if {'read_bytes_per_sec', 'write_bytes_per_sec'}.issubset(df.columns):
                    grouped = df.groupby('timestamp')[['read_bytes_per_sec', 'write_bytes_per_sec']].sum().reset_index()
                    grouped['disk_throughput_mb'] = (
                        (grouped['read_bytes_per_sec'] + grouped['write_bytes_per_sec']) / (1024 ** 2)).round(2)
                    df = grouped[['timestamp', 'disk_throughput_mb']]
                    df = add_month_and_ip(df, local_ip)
                    df = convert_all_but_ip_ts_month_to_numeric(df)
                    ensure_sheet_exists(service, sheet_value)
                    clear_google_sheet(service, sheet_value)
                    update_google_sheet(service, sheet_value, dataframe_to_sheets_values(df))
                else:
                    print(f"⚠️ Missing columns for disk throughput in {table_key}")

            elif table_key == 'ram_stats':
                if {'metric', 'bytes'}.issubset(df.columns):
                    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                    pivot_df = df.pivot_table(index='timestamp', columns='metric', values='bytes',
                                              aggfunc='mean').reset_index()
                    print(f"Pivoted ram_stats columns: {pivot_df.columns.tolist()}")
                    if 'MemAvailable' in pivot_df.columns:
                        pivot_df['available_mb'] = (pivot_df['MemAvailable'] / (1024 ** 2)).round(2)
                        pivot_df['available_gb'] = (pivot_df['available_mb'] / 1024).round(3)
                        df = pivot_df[['timestamp', 'available_mb', 'available_gb']].copy()
                        df = add_month_and_ip(df, local_ip)
                        df = convert_all_but_ip_ts_month_to_numeric(df)
                        ensure_sheet_exists(service, sheet_value)
                        clear_google_sheet(service, sheet_value)
                        update_google_sheet(service, sheet_value, dataframe_to_sheets_values(df))
                    else:
                        print(f"⚠️ 'MemAvailable' metric missing in ram_stats")
                else:
                    print(f"⚠️ Missing 'metric' or 'bytes' columns in ram_stats")

            elif table_key == 'disk_free_space':
                # Calculate additional columns
                df['free_space_mb'] = (df['bytes'] / (1024 ** 2)).round(2)
                df['bytes_gb'] = (df['bytes'] / (1024 ** 3)).round(3)
                df['free_space_gb'] = (df['free_space_mb'] / 1024).round(3)

                # Add month and ip
                df = add_month_and_ip(df, local_ip)

                # Upload summary tab
                grouped = df.groupby('timestamp')['bytes'].sum().reset_index()
                grouped['total_free_space_mb'] = (grouped['bytes'] / (1024 ** 2)).round(2)
                grouped['total_free_space_gb'] = (grouped['total_free_space_mb'] / 1024).round(3)
                df_sum = grouped[['timestamp', 'total_free_space_mb', 'total_free_space_gb']]
                df_sum = add_month_and_ip(df_sum, local_ip)
                df_sum = convert_all_but_ip_ts_month_to_numeric(df_sum)
                ensure_sheet_exists(service, 'Disk Free Space')
                clear_google_sheet(service, 'Disk Free Space')
                update_google_sheet(service, 'Disk Free Space', dataframe_to_sheets_values(df_sum))

                # Upload volumes tab with all needed columns in right order
                required_cols = ['volume', 'bytes', 'timestamp', 'free_space_mb', 'bytes_gb', 'free_space_gb', 'month', 'ip_address']
                missing_cols = [col for col in required_cols if col not in df.columns]
                if missing_cols:
                    print(f"⚠️ Missing columns for volumes upload: {missing_cols}")
                else:
                    df_vol = df[required_cols].copy()
                    print("Columns to upload:", df_vol.columns.tolist())
                    print("Sample data:\n", df_vol.head(5))

                    # Use the new function here to skip converting 'volume' column
                    df_vol = convert_all_but_ip_ts_month_volume_to_numeric(df_vol)
                    ensure_sheet_exists(service, 'Disk Free Space - Volumes')
                    clear_google_sheet(service, 'Disk Free Space - Volumes')
                    update_google_sheet(service, 'Disk Free Space - Volumes', dataframe_to_sheets_values(df_vol))

            elif table_key == 'nic_status':
                if 'up' in df.columns:
                    grouped = df.groupby('timestamp')['up'].mean().reset_index()
                    grouped['avg_nic_up_percent'] = (grouped['up'] * 100).round(1)
                    df = grouped[['timestamp', 'avg_nic_up_percent']]
                    df = add_month_and_ip(df, local_ip)
                    df = convert_all_but_ip_ts_month_to_numeric(df)
                    ensure_sheet_exists(service, sheet_value)
                    clear_google_sheet(service, sheet_value)
                    update_google_sheet(service, sheet_value, dataframe_to_sheets_values(df))
                else:
                    print(f"⚠️ Missing 'up' column in nic_status")

            elif table_key == 'bandwidth':
                if 'value' in df.columns:
                    grouped = df.groupby('timestamp')['value'].sum().reset_index()
                    grouped['total_bandwidth_bytes_per_sec'] = grouped['value'].round(2)
                    grouped['value_gb'] = (grouped['value'] / (1024 ** 2)).round(3)
                    grouped['value_mbps'] = (grouped['value'] * 8 / (1024 ** 2)).round(3)
                    df = grouped
                    df = add_month_and_ip(df, local_ip)
                    df = convert_all_but_ip_ts_month_to_numeric(df)
                    ensure_sheet_exists(service, sheet_value)
                    clear_google_sheet(service, sheet_value)
                    update_google_sheet(service, sheet_value, dataframe_to_sheets_values(df))
                else:
                    print(f"⚠️ Missing 'value' column in bandwidth")

        except Exception as e:
            print(f"❌ Error processing table '{table_key}': {e}")


main()