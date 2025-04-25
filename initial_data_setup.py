import json
import os
import pymysql
import pandas as pd
from datetime import timedelta, datetime
import yaml

# Load configuration
config = yaml.safe_load(open('config.yml', 'r'))
db_config = config['database']
folder_path = config['paths']['logs']

# Data Handling
file_list = [i for i in os.listdir(folder_path) if '.txt.' in i]

data = []
for file_name in file_list:
    file_path = os.path.join(folder_path, file_name)
    with open(file_path, 'r') as f:
        lines = []
        for line in f.read().split('\n')[:-1]:
            try:
                c = json.loads(line)
                lines.append(c)
            except json.JSONDecodeError as e:
                print(f'File: {file_name}, Invalid JSON: {line[:100]}, Error: {e}')
        print(f'Size of file "{file_name}": {len(lines)}')
        data.extend(lines)

print(f'Size of whole data: {len(data)}')

# Filter rows with audio data
data = [i for i in data if 'res' in i['snd'].keys()]
print(f'Size after removing no audio rows: {len(data)}')
if data:
    print(f'Sample debug image path: traffic/{data[0]["debug_img"].split("/", 1)[-1]}')

# Prepare data for insertion
traffic_data_list = []
audio_data_list = []
summary_data = []
traffic_id_counter = 1

for entry in data:
    try:
        full_img = f'traffic/{entry["full_img"].split("/", 1)[-1]}'
        debug_img = f'traffic/{entry["debug_img"].split("/", 1)[-1]}'
        traffic_tuple = (
            traffic_id_counter, entry['cam'], entry['probs'], entry['cls'], entry['dto'],
            entry['save_dto'], entry['point_len'], entry['intersection'][0],
            entry['intersection'][1], entry['box'][0], entry['box'][1],
            entry['box'][2], entry['box'][3], entry['frame_dto'], entry['tid'],
            entry['seq_len'], full_img, debug_img
        )

        ks_time = timedelta(milliseconds=entry['snd']['res'].get('ks', 0)) % timedelta(days=1)
        ke_time = timedelta(milliseconds=entry['snd']['res'].get('ke', 0)) % timedelta(days=1)
        dbas = entry['snd']['res'].get('dba', []) + [None] * (30 - len(entry['snd']['res'].get('dba', [])))
        max_dba = max([val for val in dbas if val is not None], default=None)

        audio_tuple = (
            traffic_id_counter, os.path.basename(entry['snd']['snd']), entry['snd']['snd_lvl'],
            ks_time, ke_time, entry['snd']['res']['kd'], *dbas, max_dba
        )

        dto = pd.to_datetime(entry['dto'])
        summary_data.append({
            'traffic_id': traffic_id_counter,
            'dto': dto,
            'max_dba': max_dba,
            'month': dto.strftime('%Y-%m'),
            'day': dto.day,
            'date': dto.date(),
            'hour': dto.hour,
            'ten_min_interval': dto.minute // 10
        })

        traffic_data_list.append(traffic_tuple)
        audio_data_list.append(audio_tuple)
        traffic_id_counter += 1

    except Exception as e:
        print(f"Skipping row due to error: {e}")
        print(entry)
        continue

# Convert summary data to DataFrame
summary_df = pd.DataFrame(summary_data)

# Compute Monthly Summary
monthly_summary = summary_df.groupby(['month', 'day']).agg({
    'traffic_id': 'count',
    'max_dba': 'max'
}).reset_index()
monthly_summary.columns = ['month', 'day', 'vehicle_count', 'max_dba']
monthly_summary_list = [tuple(row) for row in monthly_summary.itertuples(index=False)]

# Compute Daily Summary
daily_summary = summary_df.groupby(['date', 'hour', 'ten_min_interval']).agg({
    'traffic_id': 'count',
    'max_dba': 'max'
}).reset_index()
daily_summary.columns = ['date', 'hour', 'ten_min_interval', 'vehicle_count', 'max_dba']
daily_summary_list = [tuple(row) for row in daily_summary.itertuples(index=False)]

# Database Setup
conn = pymysql.connect(**db_config, autocommit=True)
cur = conn.cursor(pymysql.cursors.DictCursor)

# Drop existing tables
cur.execute("DROP TABLE IF EXISTS TrafficData")
cur.execute("DROP TABLE IF EXISTS AudioData")
cur.execute("DROP TABLE IF EXISTS monthly_summary")
cur.execute("DROP TABLE IF EXISTS daily_summary")

# Create TrafficData table
cur.execute("""
CREATE TABLE TrafficData (
    traffic_id INT NOT NULL,
    cam VARCHAR(50),
    probs FLOAT,
    cls INT,
    dto DATETIME,
    save_dto DATETIME,
    point_len INT,
    intersection_x INT,
    intersection_y INT,
    box_x1 FLOAT,
    box_y1 FLOAT,
    box_x2 FLOAT,
    box_y2 FLOAT,
    frame_dto DATETIME,
    tid INT,
    seq_len INT,
    full_img VARCHAR(500),
    debug_img VARCHAR(500),
    PRIMARY KEY(traffic_id)
);
""")

# Create AudioData table (without foreign key)
cur.execute("""
CREATE TABLE AudioData (
    audio_id INT NOT NULL AUTO_INCREMENT,
    traffic_id INT,
    snd_file VARCHAR(255),
    snd_lvl FLOAT,
    ks TIME,
    ke TIME,
    kd INT,
    dba1 FLOAT, dba2 FLOAT, dba3 FLOAT, dba4 FLOAT, dba5 FLOAT, dba6 FLOAT,
    dba7 FLOAT, dba8 FLOAT, dba9 FLOAT, dba10 FLOAT, dba11 FLOAT, dba12 FLOAT,
    dba13 FLOAT, dba14 FLOAT, dba15 FLOAT, dba16 FLOAT, dba17 FLOAT, dba18 FLOAT,
    dba19 FLOAT, dba20 FLOAT, dba21 FLOAT, dba22 FLOAT, dba23 FLOAT, dba24 FLOAT,
    dba25 FLOAT, dba26 FLOAT, dba27 FLOAT, dba28 FLOAT, dba29 FLOAT, dba30 FLOAT,
    max_dba DECIMAL(10,2),
    PRIMARY KEY(audio_id),
    INDEX idx_traffic_id (traffic_id),
    INDEX idx_max_dba (max_dba)
);
""")

# Create monthly_summary table
cur.execute("""
CREATE TABLE monthly_summary (
    month VARCHAR(7) NOT NULL,
    day INT NOT NULL,
    vehicle_count INT,
    max_dba DECIMAL(10,2),
    PRIMARY KEY (month, day)
);
""")

# Create daily_summary table
cur.execute("""
CREATE TABLE daily_summary (
    date DATE NOT NULL,
    hour INT NOT NULL,
    ten_min_interval INT NOT NULL,
    vehicle_count INT,
    max_dba DECIMAL(10,2),
    PRIMARY KEY (date, hour, ten_min_interval)
);
""")

# Bulk Inserts
cur.executemany("""
INSERT INTO TrafficData (
    traffic_id, cam, probs, cls, dto, save_dto, point_len, intersection_x, intersection_y,
    box_x1, box_y1, box_x2, box_y2, frame_dto, tid, seq_len, full_img, debug_img
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""", traffic_data_list)
print('Inserted Traffic Data Successfully')

cur.executemany("""
INSERT INTO AudioData (
    traffic_id, snd_file, snd_lvl, ks, ke, kd,
    dba1, dba2, dba3, dba4, dba5, dba6, dba7, dba8, dba9, dba10,
    dba11, dba12, dba13, dba14, dba15, dba16, dba17, dba18, dba19, dba20,
    dba21, dba22, dba23, dba24, dba25, dba26, dba27, dba28, dba29, dba30, max_dba
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""", audio_data_list)
print('Inserted Audio Data Successfully')

cur.executemany("""
INSERT INTO monthly_summary (month, day, vehicle_count, max_dba)
VALUES (%s, %s, %s, %s)
""", monthly_summary_list)
print('Inserted Monthly Summary Successfully')

cur.executemany("""
INSERT INTO daily_summary (date, hour, ten_min_interval, vehicle_count, max_dba)
VALUES (%s, %s, %s, %s, %s)
""", daily_summary_list)
print('Inserted Daily Summary Successfully')

cur.execute("CREATE INDEX idx_dto ON TrafficData (dto)")
print('Created Indexes Successfully')

cur.close()
conn.close()
print('Database connection closed.')