import json
import os
import pymysql
from datetime import timedelta, datetime
import yaml

# Load configuration
config = yaml.safe_load(open('config.yml', 'r'))
db_config = config['database']
folder_path = config['paths']['newdata']
file_list = [i for i in os.listdir(folder_path) if '.txt.' in i]

data = []
for file_name in file_list:
    file_path = os.path.join(folder_path, file_name)
    with open(file_path, 'r') as f:
        try:
            lines = [json.loads(line) for line in f.read().split('\n')[:-1] if line]
            print(f'Size of the file "{file_name}": {len(lines)}')
            data.extend(lines)
        except Exception as e:
            print(f'Error with file {file_name}: {e}')

print(f'\nSize of the whole data: {len(data)}')

# Filter rows with audio data
data = [i for i in data if 'res' in i['snd'].keys()]
print(f'Size after removing no audio rows: {len(data)}')
if data:
    print(f'Sample debug image path: traffic/{data[0]["debug_img"].split("/", 1)[-1]}')

# Prepare batch insert lists
traffic_data_list = []
audio_data_list = []
monthly_summary_dict = {}  # Key: (month, day), Value: {'vehicle_count': int, 'max_dba': float}
daily_summary_dict = {}    # Key: (date, hour, ten_min_interval), Value: {'vehicle_count': int, 'max_dba': float}
traffic_id_counter = 0

# Connect to SQL database
conn = pymysql.connect(**db_config, autocommit=True)
cur = conn.cursor(pymysql.cursors.DictCursor)

# Get the maximum traffic_id
cur.execute("SELECT MAX(traffic_id) as max_id FROM TrafficData")
result = cur.fetchone()
traffic_id_counter = result['max_id'] + 1 if result['max_id'] is not None else 1
print(f'Starting traffic_id_counter at: {traffic_id_counter}')

for entry in data:
    try:
        # Process Traffic Data
        full_img = 'traffic/' + entry['full_img'].split('/', 1)[-1]
        debug_img = 'traffic/' + entry['debug_img'].split('/', 1)[-1]
        
        traffic_tuple = (
            traffic_id_counter, entry['cam'], entry['probs'], entry['cls'], entry['dto'], 
            entry['save_dto'], entry['point_len'], entry['intersection'][0], 
            entry['intersection'][1], entry['box'][0], entry['box'][1], 
            entry['box'][2], entry['box'][3], entry['frame_dto'], entry['tid'], 
            entry['seq_len'], full_img, debug_img
        )

        # Process Audio Data
        ks_time = timedelta(milliseconds=entry['snd']['res'].get('ks', 0)) % timedelta(days=1)
        ke_time = timedelta(milliseconds=entry['snd']['res'].get('ke', 0)) % timedelta(days=1)
        dbas = entry['snd']['res'].get('dba', []) + [None] * (30 - len(entry['snd']['res'].get('dba', [])))
        max_dba = max([val for val in dbas if val is not None], default=None)

        audio_tuple = (
            traffic_id_counter, os.path.basename(entry['snd']['snd']), entry['snd']['snd_lvl'],
            ks_time, ke_time, entry['snd']['res']['kd'], *dbas, max_dba
        )

        # Summary Data
        dto = datetime.strptime(entry['dto'], '%Y-%m-%d %H:%M:%S')
        month = dto.strftime('%Y-%m')
        day = dto.day
        date = dto.date()
        hour = dto.hour
        ten_min_interval = dto.minute // 10

        # Monthly Summary
        month_key = (month, day)
        if month_key not in monthly_summary_dict:
            monthly_summary_dict[month_key] = {'vehicle_count': 0, 'max_dba': None}
        monthly_summary_dict[month_key]['vehicle_count'] += 1
        if max_dba is not None:
            current_max = monthly_summary_dict[month_key]['max_dba']
            monthly_summary_dict[month_key]['max_dba'] = max(current_max, max_dba) if current_max is not None else max_dba

        # Daily Summary
        day_key = (date, hour, ten_min_interval)
        if day_key not in daily_summary_dict:
            daily_summary_dict[day_key] = {'vehicle_count': 0, 'max_dba': None}
        daily_summary_dict[day_key]['vehicle_count'] += 1
        if max_dba is not None:
            current_max = daily_summary_dict[day_key]['max_dba']
            daily_summary_dict[day_key]['max_dba'] = max(current_max, max_dba) if current_max is not None else max_dba

        traffic_data_list.append(traffic_tuple)
        audio_data_list.append(audio_tuple)
        traffic_id_counter += 1

    except Exception as e:
        print(f"Skipping row due to error: {e}")
        print(f"Problematic entry: {entry}")
        continue

# Convert summary dictionaries to insert lists
monthly_summary_list = [(k[0], k[1], v['vehicle_count'], v['max_dba']) for k, v in monthly_summary_dict.items()]
daily_summary_list = [(k[0], k[1], k[2], v['vehicle_count'], v['max_dba']) for k, v in daily_summary_dict.items()]

# Bulk insert TrafficData
traffic_sql = """
INSERT INTO TrafficData (
    traffic_id, cam, probs, cls, dto, save_dto, point_len, intersection_x, intersection_y, 
    box_x1, box_y1, box_x2, box_y2, frame_dto, tid, seq_len, full_img, debug_img
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
try:
    cur.executemany(traffic_sql, traffic_data_list)
    conn.commit()
    print(f'Inserted {len(traffic_data_list)} rows into TrafficData successfully')
except Exception as e:
    print(f'Error inserting TrafficData: {e}')
    conn.rollback()

# Bulk insert AudioData
audio_sql = """
INSERT INTO AudioData (
    traffic_id, snd_file, snd_lvl, ks, ke, kd, 
    dba1, dba2, dba3, dba4, dba5, dba6, dba7, dba8, dba9, dba10,
    dba11, dba12, dba13, dba14, dba15, dba16, dba17, dba18, dba19, dba20,
    dba21, dba22, dba23, dba24, dba25, dba26, dba27, dba28, dba29, dba30, max_dba
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
try:
    cur.executemany(audio_sql, audio_data_list)
    conn.commit()
    print(f'Inserted {len(audio_data_list)} rows into AudioData successfully')
except Exception as e:
    print(f'Error inserting AudioData: {e}')
    conn.rollback()

# Bulk insert monthly_summary
monthly_summary_sql = """
INSERT INTO monthly_summary (month, day, vehicle_count, max_dba)
VALUES (%s, %s, %s, %s)
"""
try:
    cur.executemany(monthly_summary_sql, monthly_summary_list)
    conn.commit()
    print(f'Inserted {len(monthly_summary_list)} rows into monthly_summary successfully')
except Exception as e:
    print(f'Error inserting monthly_summary: {e}')
    conn.rollback()

# Bulk insert daily_summary
daily_summary_sql = """
INSERT INTO daily_summary (date, hour, ten_min_interval, vehicle_count, max_dba)
VALUES (%s, %s, %s, %s, %s)
"""
try:
    cur.executemany(daily_summary_sql, daily_summary_list)
    conn.commit()
    print(f'Inserted {len(daily_summary_list)} rows into daily_summary successfully')
except Exception as e:
    print(f'Error inserting daily_summary: {e}')
    conn.rollback()

# Clean up
cur.close()
conn.close()
print('Database connection closed.')