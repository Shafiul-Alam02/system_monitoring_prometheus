import requests
import pandas as pd
import time
from sqlalchemy import create_engine
from datetime import datetime, UTC
import re
import credentials



# PostgreSQL connection
engine = create_engine(credentials.PG_CONNECTION_STRING)
NODE_EXPORTER_METRICS_URL = credentials.NODE_EXPORTER_METRICS_URL


# Fetch raw text from Node Exporter
def fetch_raw_metrics():
    res = requests.get(NODE_EXPORTER_METRICS_URL)
    res.raise_for_status()
    return res.text


# Parse metric lines with regex
def parse_metric(pattern, text, value_name):
    regex = re.compile(pattern)
    records = []
    for line in text.splitlines():
        m = regex.match(line)
        if m:
            labels_str, value = m.groups()
            labels = dict(re.findall(r'(\w+)="(.*?)"', labels_str)) if labels_str else {}
            labels[value_name] = float(value)
            records.append(labels)
    return pd.DataFrame(records)


# Insert DataFrame into PostgreSQL table
def insert(df, table):
    if not df.empty:
        df = df.copy()
        df.loc[:, "timestamp"] = datetime.now(UTC)
        if "instance" in df.columns:
            df.loc[:, "ip"] = df["instance"].str.extract(r'(\d+\.\d+\.\d+\.\d+)').fillna("127.0.0.1")
        else:
            df.loc[:, "ip"] = "127.0.0.1"
        df.to_sql(table, engine, if_exists='append', index=False)
        print(f"✅ Inserted {len(df)} rows into '{table}'")
    else:
        print(f"⚠️ No data to insert into '{table}'.")


# Calculate rate between two snapshots
def calculate_rate(df1, df2, label_cols, value_col):
    df = pd.merge(df1, df2, on=label_cols, suffixes=("_old", "_new"))
    df[value_col + "_rate"] = (df[value_col + "_new"] - df[value_col + "_old"]) / 60
    df = df[df[value_col + "_rate"] >= 0]
    return df


# Store CPU (all modes)
def store_cpu(text1, text2):
    df1 = parse_metric(r'node_cpu_seconds_total\{([^}]*)\} ([0-9.e+-]+)', text1, "value")
    df2 = parse_metric(r'node_cpu_seconds_total\{([^}]*)\} ([0-9.e+-]+)', text2, "value")
    if not df1.empty and not df2.empty:
        if "instance" not in df1.columns: df1["instance"] = "localhost:9100"
        if "instance" not in df2.columns: df2["instance"] = "localhost:9100"
        df_rate = calculate_rate(df1, df2, ["instance", "cpu", "mode"], "value")
        df_rate.rename(columns={"cpu": "core", "mode": "state", "value_rate": "rate_per_sec"}, inplace=True)
        insert(df_rate[["core", "state", "rate_per_sec"]], "cpu_cstate_rates")
    else:
        print("⚠️ No CPU data available.")


# Store memory available
def store_memory(text):
    df = parse_metric(r'node_memory_MemAvailable_bytes(?:\{([^}]*)\})? ([0-9.e+-]+)', text, "bytes")
    if not df.empty:
        df["metric"] = "MemAvailable"
        insert(df[["metric", "bytes"]], "ram_stats")


# Store disk free space
def store_disk_free(text):
    df = parse_metric(r'node_filesystem_free_bytes\{([^}]*)\} ([0-9.e+-]+)', text, "bytes")
    if "mountpoint" in df.columns:
        df.rename(columns={"mountpoint": "volume"}, inplace=True)
    df["volume"] = df["volume"].map({
        "/": "C",
        "/run": "D",
        "/run/user/1000": "E",
        "/run/lock": "F",
        "/run/snapd/ns": "G"
    }).fillna(df["volume"])
    insert(df[["volume", "bytes"]], "disk_free_space")


# Store network bandwidth
def store_bandwidth(text1, text2):
    rx1 = parse_metric(r'node_network_receive_bytes_total\{([^}]*)\} ([0-9.e+-]+)', text1, "value")
    rx2 = parse_metric(r'node_network_receive_bytes_total\{([^}]*)\} ([0-9.e+-]+)', text2, "value")
    tx1 = parse_metric(r'node_network_transmit_bytes_total\{([^}]*)\} ([0-9.e+-]+)', text1, "value")
    tx2 = parse_metric(r'node_network_transmit_bytes_total\{([^}]*)\} ([0-9.e+-]+)', text2, "value")
    for df_old, df_new, mode in [(rx1, rx2, "receive"), (tx1, tx2, "transmit")]:
        if "instance" not in df_old.columns: df_old["instance"] = "localhost:9100"
        if "instance" not in df_new.columns: df_new["instance"] = "localhost:9100"
        df_rate = calculate_rate(df_old, df_new, ["instance", "device"], "value")
        df_rate["metric_name"] = f"node_network_{mode}_bytes_total"
        df_rate["mode"] = mode
        df_rate["value"] = df_rate["value_rate"]
        df_rate["value_mbps"] = df_rate["value"] * 8 / 1024 / 1024
        insert(df_rate[["metric_name", "mode", "value", "value_mbps"]], "bandwidth")


# Store disk IOPS and throughput
def store_disk_iops_throughput(text1, text2):
    r1 = parse_metric(r'node_disk_reads_completed_total\{([^}]*)\} ([0-9.e+-]+)', text1, "reads")
    r2 = parse_metric(r'node_disk_reads_completed_total\{([^}]*)\} ([0-9.e+-]+)', text2, "reads")
    w1 = parse_metric(r'node_disk_writes_completed_total\{([^}]*)\} ([0-9.e+-]+)', text1, "writes")
    w2 = parse_metric(r'node_disk_writes_completed_total\{([^}]*)\} ([0-9.e+-]+)', text2, "writes")
    rb1 = parse_metric(r'node_disk_read_bytes_total\{([^}]*)\} ([0-9.e+-]+)', text1, "rbytes")
    rb2 = parse_metric(r'node_disk_read_bytes_total\{([^}]*)\} ([0-9.e+-]+)', text2, "rbytes")
    wb1 = parse_metric(r'node_disk_written_bytes_total\{([^}]*)\} ([0-9.e+-]+)', text1, "wbytes")
    wb2 = parse_metric(r'node_disk_written_bytes_total\{([^}]*)\} ([0-9.e+-]+)', text2, "wbytes")

    if not r1.empty and not r2.empty and not w1.empty and not w2.empty:
        r = calculate_rate(r1, r2, ["device"], "reads")
        w = calculate_rate(w1, w2, ["device"], "writes")
        df = pd.merge(r, w, on="device")
        df.rename(columns={"reads_rate": "read_iops", "writes_rate": "write_iops", "device": "volume"}, inplace=True)
        df["volume"] = df["volume"].map({"sda": "C", "sdb": "D", "sdc": "E"}).fillna(df["volume"])
        insert(df[["volume", "read_iops", "write_iops"]], "disk_iops")

    if not rb1.empty and not rb2.empty and not wb1.empty and not wb2.empty:
        r = calculate_rate(rb1, rb2, ["device"], "rbytes")
        w = calculate_rate(wb1, wb2, ["device"], "wbytes")
        df = pd.merge(r, w, on="device")
        df.rename(
            columns={"rbytes_rate": "read_bytes_per_sec", "wbytes_rate": "write_bytes_per_sec", "device": "volume"},
            inplace=True)
        df["volume"] = df["volume"].map({"sda": "C", "sdb": "D", "sdc": "E"}).fillna(df["volume"])
        insert(df[["volume", "read_bytes_per_sec", "write_bytes_per_sec"]], "disk_throughput")


# Run the whole process
def main():
    print("📡 Fetching first snapshot...")
    text1 = fetch_raw_metrics()
    time.sleep(10)
    print("📡 Fetching second snapshot...")
    text2 = fetch_raw_metrics()
    store_bandwidth(text1, text2)
    store_memory(text2)
    store_disk_free(text2)
    store_cpu(text1, text2)
    store_disk_iops_throughput(text1, text2)
    print("✅ All metrics collected and stored.")


# Run now

main()