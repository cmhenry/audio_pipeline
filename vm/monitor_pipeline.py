# monitor_pipeline.py - Runs on cloud VM with PostgreSQL
import psycopg2
import datetime
from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/pipeline/status')
def pipeline_status():
    """API endpoint for monitoring pipeline status"""
    conn = psycopg2.connect("postgresql://audio_user:audio_password@localhost/audio_pipeline")
    cur = conn.cursor()
    
    # Get processing stats
    cur.execute("""
        SELECT 
            status,
            COUNT(*) as count,
            MIN(year || '-' || LPAD(month::text, 2, '0') || '-' || LPAD(date::text, 2, '0')) as oldest,
            MAX(year || '-' || LPAD(month::text, 2, '0') || '-' || LPAD(date::text, 2, '0')) as newest
        FROM processing_queue
        GROUP BY status
    """)
    
    status_summary = {row[0]: {
        'count': row[1],
        'oldest': row[2],
        'newest': row[3]
    } for row in cur.fetchall()}
    
    # Get recent completions
    cur.execute("""
        SELECT 
            year || '-' || LPAD(month::text, 2, '0') || '-' || LPAD(date::text, 2, '0') as date,
            processing_end - processing_start as duration,
            (SELECT COUNT(*) FROM audio_files WHERE year = q.year AND month = q.month AND date = q.date) as files_processed
        FROM processing_queue q
        WHERE status = 'completed'
        ORDER BY processing_end DESC
        LIMIT 10
    """)
    
    recent = [{'date': row[0], 'duration': str(row[1]), 'files': row[2]} 
              for row in cur.fetchall()]
    
    conn.close()
    
    return jsonify({
        'status_summary': status_summary,
        'recent_completions': recent,
        'timestamp': datetime.datetime.now().isoformat()
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)