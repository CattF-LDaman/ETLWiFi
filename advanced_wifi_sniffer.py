import csv
import hashlib
import os
import subprocess
import threading
import time
from collections import defaultdict
from datetime import datetime
from scapy.all import sniff, Dot11

# ==================== CONFIGURATION ====================
MONITOR_INTERFACE = "wlx001cf09e26ce"  # Your interface name
DATA_FILE = "/mnt/TOPHDD/my_neighborhood.csv"
CHANNELS = [1, 6, 11]     # Core 2.4 GHz frequencies
HOP_INTERVAL = 2          # Seconds to spend on each channel
FLUSH_INTERVAL = 10       # Summary interval in seconds (squashes data into 10s chunks)
# =======================================================

# Thread safety lock and global in-memory database
data_lock = threading.Lock()
packet_buffer = defaultdict(lambda: {
    "pkt_count": 0,
    "total_bytes": 0,
    "rssi_values": [],
    "mgmt_count": 0,
    "ctrl_count": 0,
    "data_count": 0,
    "channels": set()
})

def initialize_csv():
    """Initializes the CSV with rich aggregate data columns."""
    file_exists = os.path.isfile(DATA_FILE)
    with open(DATA_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "window_timestamp", "mac", "packet_count", 
                "total_bytes", "min_rssi", "max_rssi", "avg_rssi", 
                "mgmt_pkts", "ctrl_pkts", "data_pkts", "observed_channels"
            ])
            print(f"📁 Initialized aggregated dataset: {DATA_FILE}")

def channel_hopper():
    """Background thread navigating wireless channels."""
    global current_channel
    while True:
        for channel in CHANNELS:
            try:
                subprocess.run(
                    ["sudo", "iw", "dev", MONITOR_INTERFACE, "set", "channel", str(channel)],
                    check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                )
                current_channel = channel
            except subprocess.CalledProcessError:
                pass
            time.sleep(HOP_INTERVAL)

def packet_handler(pkt):
    """Processes every raw radio packet inside RAM without hitting disk."""
    if pkt.haslayer(Dot11):
        sender = pkt.addr2
        if not sender:
            return
        
        # 1. Basic metrics extraction
        packet_size = len(pkt)
        frame_type = pkt.type  # 0: Mgmt, 1: Ctrl, 2: Data
        
        try:
            rssi = pkt.dBm_AntSignal
        except AttributeError:
            rssi = None

        # 2. Privacy masking
        # hashed_mac = hashlib.sha256(sender.encode()).hexdigest()[:16]

        # 3. Securely update the RAM buffer
        with data_lock:
            stats = packet_buffer[sender]
            stats["pkt_count"] += 1
            stats["total_bytes"] += packet_size
            stats["channels"].add(str(current_channel))
            
            if rssi is not None:
                stats["rssi_values"].append(rssi)
                
            if frame_type == 0:
                stats["mgmt_count"] += 1
            elif frame_type == 1:
                stats["ctrl_count"] += 1
            elif frame_type == 2:
                stats["data_count"] += 1

def csv_flusher_loop():
    """Periodically writes RAM snapshots out to disk and resets buffers."""
    print(f"⏱️ Aggregator loop running. Flushing to disk every {FLUSH_INTERVAL}s...")
    while True:
        time.sleep(FLUSH_INTERVAL)
        
        # Atomically isolate the current window's data
        with data_lock:
            global packet_buffer
            active_window_data = packet_buffer
            packet_buffer = defaultdict(lambda: {
                "pkt_count": 0, "total_bytes": 0, "rssi_values": [],
                "mgmt_count": 0, "ctrl_count": 0, "data_count": 0, "channels": set()
            })

        if not active_window_data:
            continue

        timestamp = datetime.utcnow().isoformat()
        rows_to_write = []

        # Process and compute features for the snapshot window
        for mac, stats in active_window_data.items():
            rssis = stats["rssi_values"]
            min_rssi = min(rssis) if rssis else -99
            max_rssi = max(rssis) if rssis else -99
            avg_rssi = round(sum(rssis) / len(rssis), 1) if rssis else -99
            channels_str = "|".join(stats["channels"])

            rows_to_write.append([
                timestamp, mac, stats["pkt_count"], stats["total_bytes"],
                min_rssi, max_rssi, avg_rssi,
                stats["mgmt_count"], stats["ctrl_count"], stats["data_count"],
                channels_str
            ])

        # Batch write window down to the storage media
        with open(DATA_FILE, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(rows_to_write)
            
        print(f"💾 [Flush] Saved activities for {len(rows_to_write)} unique devices.")

if __name__ == "__main__":
    initialize_csv()
    current_channel = CHANNELS[0]

    # Spin up execution threads
    threading.Thread(target=channel_hopper, daemon=True).start()
    threading.Thread(target=csv_flusher_loop, daemon=True).start()

    print(f"🚀 Capturing ALL traffic on {MONITOR_INTERFACE}. Aggregating on-the-fly...")
    try:
        sniff(iface=MONITOR_INTERFACE, prn=packet_handler, store=0)
    except KeyboardInterrupt:
        print("\n🛑 Shutting down. All pending logs preserved.")