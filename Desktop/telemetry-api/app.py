from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv # type: ignore
import logging
from datetime import datetime

# Load environment variables
load_dotenv()

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'telemetry_data'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

def get_db_connection():
    """Create and return a database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        logger.error(f"Database connection error: {e}")
        return None

def create_sample_table():
    """Create a sample telemetry table if it doesn't exist"""
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sensor_data (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    temperature FLOAT,
                    humidity FLOAT,
                    pressure FLOAT,
                    sensor_id VARCHAR(50)
                )
            """)
            conn.commit()
            logger.info("Sample table created successfully")
        except psycopg2.Error as e:
            logger.error(f"Error creating table: {e}")
        finally:
            conn.close()

@app.route('/')
def home():
    """Health check endpoint"""
    return jsonify({
        "message": "Telemetry API is running",
        "timestamp": datetime.now().isoformat(),
        "status": "healthy"
    })

@app.route('/api/telemetry/latest', methods=['GET'])
def get_latest_telemetry():
    """Get the latest telemetry data"""
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed"}), 500
    
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Get the latest 10 records
        cur.execute("""
            SELECT id, timestamp, temperature, humidity, pressure, sensor_id
            FROM sensor_data 
            ORDER BY timestamp DESC 
            LIMIT 10
        """)
        
        results = cur.fetchall()
        
        # Convert to list of dictionaries
        data = []
        for row in results:
            data.append({
                "id": row['id'],
                "timestamp": row['timestamp'].isoformat() if row['timestamp'] else None,
                "temperature": row['temperature'],
                "humidity": row['humidity'],
                "pressure": row['pressure'],
                "sensor_id": row['sensor_id']
            })
        
        return jsonify({
            "success": True,
            "data": data,
            "count": len(data)
        })
        
    except psycopg2.Error as e:
        logger.error(f"Database query error: {e}")
        return jsonify({"error": "Database query failed"}), 500
    
    finally:
        conn.close()

@app.route('/api/telemetry/insert', methods=['POST'])
def insert_telemetry():
    """Insert new telemetry data (for testing purposes)"""
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed"}), 500
    
    try:
        data = request.get_json()
        
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO sensor_data (temperature, humidity, pressure, sensor_id)
            VALUES (%s, %s, %s, %s)
            RETURNING id, timestamp
        """, (
            data.get('temperature'),
            data.get('humidity'),
            data.get('pressure'),
            data.get('sensor_id', 'default')
        ))
        
        result = cur.fetchone()
        conn.commit()
        
        return jsonify({
            "success": True,
            "message": "Data inserted successfully",
            "id": result[0],
            "timestamp": result[1].isoformat()
        })
        
    except psycopg2.Error as e:
        logger.error(f"Database insert error: {e}")
        return jsonify({"error": "Database insert failed"}), 500
    
    finally:
        conn.close()

@app.route('/api/telemetry/stats', methods=['GET'])
def get_telemetry_stats():
    """Get basic statistics about the telemetry data"""
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed"}), 500
    
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                COUNT(*) as total_records,
                AVG(temperature) as avg_temperature,
                AVG(humidity) as avg_humidity,
                AVG(pressure) as avg_pressure,
                MAX(timestamp) as latest_timestamp
            FROM sensor_data
        """)
        
        result = cur.fetchone()
        
        return jsonify({
            "success": True,
            "stats": {
                "total_records": result[0],
                "avg_temperature": round(result[1], 2) if result[1] else None,
                "avg_humidity": round(result[2], 2) if result[2] else None,
                "avg_pressure": round(result[3], 2) if result[3] else None,
                "latest_timestamp": result[4].isoformat() if result[4] else None
            }
        })
        
    except psycopg2.Error as e:
        logger.error(f"Database query error: {e}")
        return jsonify({"error": "Database query failed"}), 500
    
    finally:
        conn.close()

if __name__ == '__main__':
    # Create sample table on startup
    create_sample_table()
    
    # Run the Flask app
    app.run(debug=True, host='127.0.0.1', port=5000)