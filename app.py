import os
import yaml
import pymysql
from cryptography.fernet import Fernet
from flask import Flask, request, jsonify, render_template, send_file
import requests
from io import BytesIO
import plotly
import plotly.graph_objs as go
import json
from decimal import Decimal
from datetime import datetime

app = Flask(__name__)

# Load configuration
config = yaml.safe_load(open('config.yml', 'r'))
KEY = config['key'].encode('utf-8')
db_config = config['database']

def encrypt_string(message, key):
    try:
        f = Fernet(key)
        encrypted_message = f.encrypt(message.encode())
        return encrypted_message.decode()
    except Exception as e:
        print(f"Encryption error: {e}")
        return None

def create_multigraph(time_labels, max_dba, vehicle_counts, interval='10min'):
    if not time_labels:
        if interval == '10min':
            time_labels = [f"{h}:{m:02d}" for h in range(7, 20) for m in range(0, 60, 10)]
            max_dba = [0] * len(time_labels)
            vehicle_counts = [0] * len(time_labels)
        else:
            time_labels = [str(i) for i in range(1, 32)]
            max_dba = [0] * 31
            vehicle_counts = [0] * 31
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=time_labels,
        y=max_dba,
        name='Max dBA',
        marker_color='#20c997',
        opacity=0.6,
        yaxis='y'
    ))
    
    fig.add_trace(go.Scatter(
        x=time_labels,
        y=vehicle_counts,
        name='Vehicle Count',
        mode='lines+markers',
        marker=dict(size=6, color='#fd7e14'),
        line=dict(width=2, color='#fd7e14'),
        yaxis='y2'
    ))
    
    if interval == '10min':
        tickvals = [f"{h}:00" for h in range(7, 20)]
        ticktext = [f"{h}:00" for h in range(7, 20)]
        title = "Traffic Noise and Vehicle Counts (07:00 - 19:00)"
    else:
        tickvals = [str(i) for i in range(1, 32, 5)]
        ticktext = [str(i) for i in range(1, 32, 5)]
        title = "Traffic Noise and Vehicle Counts by Day"
    
    fig.update_layout(
        title=title,
        xaxis=dict(
            title="Time of Day" if interval == '10min' else "Day of Month",
            tickmode='array',
            tickvals=tickvals,
            ticktext=ticktext,
            tickangle=45
        ),
        yaxis=dict(
            title=dict(text="Max dBA", font=dict(color='#20c997')),
            tickfont=dict(color='#20c997')
        ),
        yaxis2=dict(
            title=dict(text="Vehicle Count", font=dict(color='#fd7e14')),
            tickfont=dict(color='#fd7e14'),
            overlaying='y',
            side='right'
        ),
        template="ggplot2",
        font=dict(size=14),
        hovermode="x unified",
        height=600,
        showlegend=True,
        legend=dict(x=0.1, y=1.1, orientation='h')
    )
    
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

# Home page
@app.route('/')
def home():
    return render_template('home.html')

@app.route('/by_month')
def by_month():
    conn = pymysql.connect(**db_config, autocommit=True)
    cur = conn.cursor(pymysql.cursors.DictCursor)
    
    # Get all available months with display names
    sql = '''SELECT DISTINCT month FROM monthly_summary ORDER BY month'''
    cur.execute(sql)
    all_months = []
    for row in cur.fetchall():
        month_str = row['month']  # e.g., '2025-04'
        try:
            # Convert 'YYYY-MM' to 'Month YYYY' (e.g., 'April 2025')
            month_dt = datetime.strptime(month_str, '%Y-%m')
            display_name = month_dt.strftime('%B %Y')
            all_months.append({'value': month_str, 'display': display_name})
        except ValueError:
            continue
    
    # Get top 100 max dBA records with images
    selected_month = request.args.get('month', all_months[-1]['value'] if all_months else None)
    grid_data = []
    if selected_month:
        sql = '''
        SELECT a.traffic_id, a.max_dba, t.debug_img
        FROM AudioData a
        JOIN TrafficData t ON a.traffic_id = t.traffic_id
        WHERE DATE_FORMAT(t.dto, '%%Y-%%m') = %s
        ORDER BY a.max_dba DESC
        LIMIT 100'''
        cur.execute(sql, (selected_month,))
        top_dba_data = cur.fetchall()
        for row in top_dba_data:
            raw_img = row['debug_img']
            encrypted_img = encrypt_string(raw_img, KEY)
            image_url = f'https://filerepo.clarksonmsda.org:444/fetch/{encrypted_img}'
            grid_data.append({
                'traffic_id': row['traffic_id'],
                'max_dba': float(row['max_dba']),
                'image_url': image_url
            })
    
    # Get summary stats
    vehicle_count = 0
    if selected_month:
        sql = '''SELECT SUM(vehicle_count) AS vehicle_count
                 FROM monthly_summary 
                 WHERE month = %s'''
        cur.execute(sql, (selected_month,))
        stats = cur.fetchone()
        vehicle_count = stats['vehicle_count'] if stats else 0
    
    # Get graph data
    time_labels = []
    max_dba = []
    vehicle_counts = []
    if selected_month:
        sql = '''SELECT day, max_dba, vehicle_count
                 FROM monthly_summary 
                 WHERE month = %s
                 ORDER BY day'''
        cur.execute(sql, (selected_month,))
        daily_data = cur.fetchall()
        time_labels = [str(row['day']) for row in daily_data]
        max_dba = [float(row['max_dba']) if row['max_dba'] else 0 for row in daily_data]
        vehicle_counts = [row['vehicle_count'] for row in daily_data]
    
    graphJSON = create_multigraph(time_labels, max_dba, vehicle_counts, interval='day')
    
    cur.close()
    conn.close()
    
    return render_template('dashboard_month.html', 
                         all_months=all_months,
                         grid_data=grid_data,
                         selected_month=selected_month,
                         vehicle_count=vehicle_count,
                         graphJSON=graphJSON)

@app.route('/by_day')
def by_day():
    conn = pymysql.connect(**db_config, autocommit=True)
    cur = conn.cursor(pymysql.cursors.DictCursor)
    
    # Get all available dates
    sql = '''SELECT DISTINCT date FROM daily_summary ORDER BY date'''
    cur.execute(sql)
    all_dates = [row['date'].strftime('%Y-%m-%d') for row in cur.fetchall()]
    
    # Get top 100 max dBA records with images
    selected_date = request.args.get('date', all_dates[-1] if all_dates else None)
    grid_data = []
    if selected_date:
        sql = '''
        SELECT a.traffic_id, a.max_dba, t.debug_img
        FROM AudioData a
        JOIN TrafficData t ON a.traffic_id = t.traffic_id
        WHERE DATE(t.dto) = %s
        ORDER BY a.max_dba DESC
        LIMIT 100'''
        cur.execute(sql, (selected_date,))
        top_dba_data = cur.fetchall()
        for row in top_dba_data:
            raw_img = row['debug_img']
            encrypted_img = encrypt_string(raw_img, KEY)
            image_url = f'https://filerepo.clarksonmsda.org:444/fetch/{encrypted_img}'
            grid_data.append({
                'traffic_id': row['traffic_id'],
                'max_dba': float(row['max_dba']),
                'image_url': image_url
            })
    
    # Get summary stats
    vehicle_count = 0
    if selected_date:
        sql = '''SELECT SUM(vehicle_count) AS vehicle_count
                 FROM daily_summary 
                 WHERE date = %s'''
        cur.execute(sql, (selected_date,))
        stats = cur.fetchone()
        vehicle_count = stats['vehicle_count'] if stats else 0
    
    # Get graph data
    time_labels = []
    max_dba = []
    vehicle_counts = []
    if selected_date:
        sql = '''SELECT hour, ten_min_interval, max_dba, vehicle_count
                 FROM daily_summary 
                 WHERE date = %s
                 ORDER BY hour, ten_min_interval'''
        cur.execute(sql, (selected_date,))
        interval_data = cur.fetchall()
        time_labels = [f"{row['hour']}:{row['ten_min_interval'] * 10:02d}" for row in interval_data]
        max_dba = [float(row['max_dba']) if row['max_dba'] else 0 for row in interval_data]
        vehicle_counts = [row['vehicle_count'] for row in interval_data]
    
    graphJSON = create_multigraph(time_labels, max_dba, vehicle_counts, interval='10min')
    
    cur.close()
    conn.close()
    
    return render_template('dashboard_day.html', 
                         all_dates=all_dates,
                         grid_data=grid_data,
                         selected_date=selected_date,
                         vehicle_count=vehicle_count,
                         graphJSON=graphJSON)

@app.route('/update_month_data', methods=['POST'])
def update_month_data():
    selected_month = request.form['month']
    if not selected_month or selected_month == 'default':
        return jsonify({
            'vehicle_count': 0,
            'graphJSON': create_multigraph([], [], [], interval='day')
        })
    
    conn = pymysql.connect(**db_config, autocommit=True)
    cur = conn.cursor(pymysql.cursors.DictCursor)
    
    # Get summary stats (only vehicle count)
    sql = '''SELECT SUM(vehicle_count) AS vehicle_count
             FROM monthly_summary 
             WHERE month = %s
             GROUP BY month'''
    cur.execute(sql, (selected_month,))
    stats = cur.fetchone()
    vehicle_count = stats['vehicle_count'] if stats else 0
    
    # Get daily data for graph
    sql = '''SELECT 
                day,
                max_dba,
                vehicle_count
             FROM monthly_summary 
             WHERE month = %s
             ORDER BY day'''
    cur.execute(sql, (selected_month,))
    daily_data = cur.fetchall()
    
    cur.close()
    conn.close()
    
    # Prepare data for multigraph
    time_labels = [str(row['day']) for row in daily_data]
    max_dba_list = [float(row['max_dba']) if row['max_dba'] else 0 for row in daily_data]
    vehicle_counts = [row['vehicle_count'] for row in daily_data]
    
    graphJSON = create_multigraph(time_labels, max_dba_list, vehicle_counts, interval='day')
    
    return jsonify({
        'vehicle_count': vehicle_count,
        'graphJSON': graphJSON
    })

@app.route('/update_day_data', methods=['POST'])
def update_day_data():
    selected_date = request.form['date']
    if not selected_date or selected_date == 'default':
        return jsonify({
            'vehicle_count': 0,
            'graphJSON': create_multigraph([], [], [], interval='10min')
        })
    
    conn = pymysql.connect(**db_config, autocommit=True)
    cur = conn.cursor(pymysql.cursors.DictCursor)
    
    # Get summary stats (only vehicle count)
    sql = '''SELECT SUM(vehicle_count) AS vehicle_count
             FROM daily_summary 
             WHERE date = %s
             GROUP BY date'''
    cur.execute(sql, (selected_date,))
    stats = cur.fetchone()
    vehicle_count = stats['vehicle_count'] if stats else 0
    
    # Get 10-minute interval data
    sql = '''SELECT 
                hour,
                ten_min_interval,
                max_dba,
                vehicle_count
             FROM daily_summary 
             WHERE date = %s
             ORDER BY hour, ten_min_interval'''
    cur.execute(sql, (selected_date,))
    interval_data = cur.fetchall()
    
    cur.close()
    conn.close()
    
    # Prepare data for multigraph
    time_labels = [f"{row['hour']}:{row['ten_min_interval'] * 10:02d}" for row in interval_data]
    max_dba_list = [float(row['max_dba']) if row['max_dba'] else 0 for row in interval_data]
    vehicle_counts = [row['vehicle_count'] for row in interval_data]
    
    graphJSON = create_multigraph(time_labels, max_dba_list, vehicle_counts, interval='10min')
    
    return jsonify({
        'vehicle_count': vehicle_count,
        'graphJSON': graphJSON
    })

@app.route('/view_image/<int:traffic_id>')
def view_image(traffic_id):
    conn = pymysql.connect(**db_config, autocommit=True)
    cur = conn.cursor(pymysql.cursors.DictCursor)
    
    # Fetch image details
    sql = '''SELECT 
                t.traffic_id,
                a.max_dba,
                t.dto,
                t.debug_img
             FROM TrafficData t
             JOIN AudioData a ON t.traffic_id = a.traffic_id
             WHERE t.traffic_id = %s'''
    cur.execute(sql, (traffic_id,))
    result = cur.fetchone()
    
    cur.close()
    conn.close()
    
    if not result:
        return "Image not found", 404
    
    # Prepare image details
    raw_img = result['debug_img']
    encrypted_img = encrypt_string(raw_img, KEY)
    image_url = f'https://filerepo.clarksonmsda.org:444/fetch/{encrypted_img}'
    
    image_details = {
        'traffic_id': result['traffic_id'],
        'max_dba': float(result['max_dba']) if result['max_dba'] else 0,
        'dto': result['dto'].strftime('%Y-%m-%d %H:%M:%S') if result['dto'] else 'N/A',
        'image_url': image_url,
        'encrypted_img': encrypted_img  # Pass encrypted_img for proxy route
    }
    
    return render_template('view_image.html', 
                         image_url=image_url, 
                         traffic_id=traffic_id, 
                         image_details=image_details)

@app.route('/proxy_image/<path:encrypted_img>')
def proxy_image(encrypted_img):
    try:
        url = f'https://filerepo.clarksonmsda.org:444/fetch/{encrypted_img}'
        response = requests.get(url, stream=True)
        if response.status_code != 200:
            return "Image not found", 404
        return send_file(
            BytesIO(response.content),
            mimetype=response.headers.get('Content-Type', 'image/jpeg'),
            as_attachment=True,
            download_name=f'image_{encrypted_img[-10:]}.jpg'
        )
    except Exception as e:
        print(f"Proxy image error: {e}")
        return "Error fetching image", 500

if __name__ == '__main__':
    app.run()