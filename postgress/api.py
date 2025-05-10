from flask import Flask, jsonify
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)
CORS(app)

POSTGRES_CONN = {
    'dbname': 'stock_db',
    'user': 'stock_user',
    'password': 'stock_pass',
    'host': 'localhost',
    'port': '5432'
}

def get_db_connection():
    return psycopg2.connect(**POSTGRES_CONN)

@app.route('/')
def home():
    return jsonify({'message': 'Welcome to the Stock Data API. Use /api/stock_data/<symbol> to fetch data.'})

@app.route('/api/stock_data/<symbol>', methods=['GET'])
def get_stock_data(symbol):
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT date, open, high, low, close, volume
            FROM stock_data
            WHERE symbol = %s
            ORDER BY date DESC
            LIMIT 30
        """, (symbol,))
        data = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)