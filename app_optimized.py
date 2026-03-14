# --- Eventlet Monkey Patching (MUST be at the very top before other imports) ---
import eventlet
eventlet.monkey_patch()

import os
from dotenv import load_dotenv
from flask import Flask, render_template, jsonify, request, Response
from flask_socketio import SocketIO, emit
from datetime import datetime, timedelta
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from pytz import timezone as ZoneInfo
import time
import threading
import json
from queue import Queue, Empty
import logging
import sqlite3
import copy

# --- Python Standard Libraries for Broker ---
import pyotp
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from SmartApi.smartConnect import SmartConnect

# --- Backwards Compatibility Patch for SmartWebSocketV2 ---
# Newer versions of websocket-client pass (ws, code, msg) to on_close, 
# but SmartWebSocketV2._on_close only expects (ws).
def _patched_on_close(self, wsapp, code=None, msg=None):
    if self.on_close:
        try:
            self.on_close(wsapp, code, msg)
        except TypeError:
            self.on_close(wsapp)

SmartWebSocketV2._on_close = _patched_on_close
# ----------------------------------------------------------

# --- Basic Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(message)s')
app = Flask(__name__)
app.config['SECRET_KEY'] = 'trading-app-secret-key'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')
load_dotenv()

# ==============================================================================
# --- BROKER CREDENTIALS & SETTINGS (ACTION REQUIRED) ---
# ==============================================================================
API_KEY = os.getenv("API_KEY")
CLIENT_CODE = os.getenv("CLIENT_CODE")
PASS = os.getenv("PASS")
AUTH_TOKEN = os.getenv("AUTH_TOKEN")  # The secret key from your 2FA app

INSTRUMENT_TOKEN = os.getenv("INSTRUMENT_TOKEN")
LOTSIZE = int(os.getenv("LOTSIZE", 75))
# ==============================================================================
# --- DATABASE CONFIGURATION ---
DB_NAME = os.getenv("DB_NAME", "trading_data_ticks.db")
# ==============================================================================

# --- Application State (Shared between threads) ---
tick_queue = Queue()  # Queue for broadcasting ticks to clients
db_write_queue = Queue()  # Queue for batch writing to database
raw_tick_queue = Queue()  # Queue for raw broker messages (decouples on_data from processing)
tick_subscribers = []  # List of subscriber queues for SSE broadcasting
subscribers_lock = threading.Lock()

# Note: SocketIO handles client management internally, no manual tracking needed

previous_tick = {
    "total_traded_volume": None,
    "ltp": None,
    "trade_direction": None,
}

# --- In-Memory Tick Cache (to fill gaps on page refresh) ---
# Stores ticks that have been sent via SSE but not yet flushed to DB.
recent_ticks_cache = []
recent_ticks_cache_lock = threading.Lock()

# --- Broker Connection State ---
broker_state = {
    "connected": False,
    "connecting": False,
    "websocket": None,
    "thread": None,
    "stop_requested": False,
    "reconnect_attempts": 0,
    "max_reconnect_attempts": 5,
    "last_error": None,
}
broker_state_lock = threading.Lock()

# --- Database Functions ---

def init_db():
    """Initializes the SQLite database and creates the ticks table if it doesn't exist."""
    try:
        conn = sqlite3.connect(DB_NAME, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instrument_token TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                price REAL NOT NULL,
                volume REAL NOT NULL,
                direction TEXT NOT NULL
            )
        ''')
        # Create index separately (SQLite doesn't support inline INDEX in CREATE TABLE)
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_instrument_timestamp 
            ON ticks(instrument_token, timestamp)
        ''')
        conn.commit()
        conn.close()
        logging.info(f"Database '{DB_NAME}' initialized successfully.")
    except Exception as e:
        logging.error(f"Error initializing database: {e}")

def db_writer_thread():
    """Background thread that batch writes ticks to the database."""
    batch = []
    batch_size = 100
    batch_timeout = 120  # seconds (increased to reduce disk writes)
    last_write_time = time.time()
    
    while True:
        try:
            # Try to get a tick with a timeout
            try:
                tick = db_write_queue.get(timeout=1)
                if tick is None:  # Poison pill to stop the thread
                    break
                batch.append(tick)
            except Empty:
                pass
            
            # Write batch if it's full or timeout reached
            current_time = time.time()
            if len(batch) >= batch_size or (batch and current_time - last_write_time >= batch_timeout):
                if batch:
                    try:
                        conn = sqlite3.connect(DB_NAME, check_same_thread=False)
                        cursor = conn.cursor()
                        cursor.executemany('''
                            INSERT INTO ticks (instrument_token, timestamp, price, volume, direction)
                            VALUES (?, ?, ?, ?, ?)
                        ''', batch)
                        conn.commit()
                        conn.close()
                        logging.info(f"Batch wrote {len(batch)} ticks to database.")
                        
                        # Clear flushed ticks from cache
                        flushed_timestamps = set(t[1] for t in batch)  # timestamp is index 1
                        with recent_ticks_cache_lock:
                            # Remove ticks that were just written
                            # We compare by timestamp since objects differ
                            original_len = len(recent_ticks_cache)
                            recent_ticks_cache[:] = [
                                t for t in recent_ticks_cache 
                                if t['timestamp'] not in flushed_timestamps
                            ]
                            logging.info(f"Cache cleaned: {original_len} -> {len(recent_ticks_cache)} entries")
                        
                        batch = []
                        last_write_time = current_time
                    except Exception as e:
                        logging.error(f"Error batch writing ticks to DB: {e}")
        except Exception as e:
            logging.error(f"Error in db_writer_thread: {e}")

def load_ticks_from_db(start_time=None, end_time=None):
    """Loads ticks from the database within the specified time range."""
    try:
        conn = sqlite3.connect(DB_NAME, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        query = "SELECT * FROM ticks WHERE instrument_token = ?"
        params = [INSTRUMENT_TOKEN]
        
        if start_time:
            query += " AND timestamp >= ?"
            params.append(start_time)
        if end_time:
            query += " AND timestamp <= ?"
            params.append(end_time)
        
        query += " ORDER BY timestamp ASC"
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        ticks = []
        for row in rows:
            ticks.append({
                'timestamp': row['timestamp'],
                'price': row['price'],
                'volume': row['volume'],
                'direction': row['direction']
            })
        
        conn.close()
        logging.info(f"Loaded {len(ticks)} ticks from database.")
        return ticks
    except Exception as e:
        logging.error(f"Error loading ticks from DB: {e}")
        return []

def get_date_range_from_db():
    """Returns the min and max timestamps available in the database for the instrument."""
    try:
        conn = sqlite3.connect(DB_NAME, check_same_thread=False)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT MIN(timestamp), MAX(timestamp) 
            FROM ticks 
            WHERE instrument_token = ?
        ''', [INSTRUMENT_TOKEN])
        
        row = cursor.fetchone()
        conn.close()
        
        if row and row[0] and row[1]:
            return {"min_timestamp": row[0], "max_timestamp": row[1]}
        return {"min_timestamp": None, "max_timestamp": None}
    except Exception as e:
        logging.error(f"Error getting date range from DB: {e}")
        return {"min_timestamp": None, "max_timestamp": None}

def get_available_dates_from_db():
    """Returns a sorted list of distinct dates (YYYY-MM-DD) available in the database."""
    try:
        conn = sqlite3.connect(DB_NAME, check_same_thread=False)
        cursor = conn.cursor()
        
        # SQLite 'date' function extracts the date part
        cursor.execute('''
            SELECT DISTINCT date(timestamp, 'unixepoch', 'localtime') as day 
            FROM ticks 
            WHERE instrument_token = ? 
            ORDER BY day ASC
        ''', [INSTRUMENT_TOKEN])
        
        rows = cursor.fetchall()
        conn.close()
        
        dates = [row[0] for row in rows if row[0]]
        return dates
    except Exception as e:
        logging.error(f"Error getting available dates from DB: {e}")
        return []

def get_day_boundaries(timestamp):
    """Returns start and end timestamps for the day containing the given timestamp (IST)."""
    # Convert to IST
    dt = datetime.fromtimestamp(timestamp)
    # Market hours: 9:15 AM to 3:30 PM IST
    day_start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = dt.replace(hour=23, minute=59, second=59, microsecond=999999)
    return int(day_start.timestamp()), int(day_end.timestamp())

# --- Tick Broadcasting ---

def broadcast_tick(tick_data):
    """Broadcast tick to all connected SSE subscribers."""
    with subscribers_lock:
        dead_subscribers = []
        for subscriber_queue in tick_subscribers:
            try:
                subscriber_queue.put_nowait(tick_data)
            except Exception:
                dead_subscribers.append(subscriber_queue)
        # Clean up dead subscribers
        for dead in dead_subscribers:
            if dead in tick_subscribers:
                tick_subscribers.remove(dead)

def broadcast_ws_tick(tick_data):
    """Broadcast tick to all connected SocketIO clients."""
    socketio.emit('tick', tick_data)

# --- Tick Processor Thread (decoupled from broker on_data) ---

# Processor-local state (not shared with broker thread)
processor_previous_tick = {
    "total_traded_volume": None,
    "ltp": None,
    "trade_direction": None,
}

def tick_processor_thread():
    """
    Background thread that processes raw broker messages.
    This decouples tick processing from the broker WebSocket's on_data callback
    for better performance and cleaner architecture.
    """
    global processor_previous_tick
    
    while True:
        try:
            # Get raw message from queue (blocking with timeout)
            try:
                message = raw_tick_queue.get(timeout=1)
            except Empty:
                continue
            
            if message is None:  # Poison pill to stop the thread
                logging.info("Tick processor received stop signal")
                break
            
            # --- Processing Logic (moved from on_data) ---
            
            # Check if broker stop was requested
            with broker_state_lock:
                if broker_state["stop_requested"]:
                    continue
            
            # Filter for tick mode messages only
            if "subscription_mode" not in message or message["subscription_mode"] != 2:
                continue
            
            ltp = message.get("last_traded_price", 0) / 100
            total_traded_volume = message.get("volume_trade_for_the_day", 0)
            timestamp = message.get("last_traded_timestamp", time.time())
            
            # Initialize on first tick
            if processor_previous_tick["total_traded_volume"] is None:
                processor_previous_tick["total_traded_volume"] = total_traded_volume
                processor_previous_tick["ltp"] = ltp
                continue
            
            # Skip if no new volume
            if total_traded_volume == processor_previous_tick["total_traded_volume"]:
                continue
            
            # Calculate trade size and direction
            trade_size = (total_traded_volume - processor_previous_tick["total_traded_volume"])
            trade_direction = "BUY" if ltp > processor_previous_tick["ltp"] else (
                "SELL" if ltp < processor_previous_tick["ltp"] else processor_previous_tick["trade_direction"]
            )
            
            if trade_direction and trade_size > 0:
                tick_data = {
                    'timestamp': int(timestamp),
                    'price': ltp,
                    'volume': trade_size / LOTSIZE,
                    'direction': trade_direction
                }
                
                # Queue for database writing
                db_write_queue.put((
                    INSTRUMENT_TOKEN,
                    tick_data['timestamp'],
                    tick_data['price'],
                    tick_data['volume'],
                    tick_data['direction']
                ))
                
                # Add to in-memory cache for refresh continuity
                with recent_ticks_cache_lock:
                    recent_ticks_cache.append(tick_data)
                
                # Broadcast to WebSocket clients (primary)
                broadcast_ws_tick(tick_data)
                
                # Broadcast to SSE subscribers (for backward compatibility)
                broadcast_tick(tick_data)
            
            # Update processor state
            processor_previous_tick["total_traded_volume"] = total_traded_volume
            processor_previous_tick["ltp"] = ltp
            processor_previous_tick["trade_direction"] = trade_direction
            
        except Exception as e:
            logging.error(f"Error in tick_processor_thread: {e}")

# --- Broker Connection and Data Handling ---

def start_broker_connection():
    """Start broker WebSocket connection with auto-reconnect."""
    global previous_tick, broker_state
    
    with broker_state_lock:
        if broker_state["connected"] or broker_state["connecting"]:
            logging.warning("Broker connection already active or in progress")
            return False
        broker_state["connecting"] = True
        broker_state["stop_requested"] = False
        broker_state["last_error"] = None
    
    def connection_worker():
        global previous_tick
        reconnect_attempt = 0
        max_attempts = 5
        
        while not broker_state["stop_requested"] and reconnect_attempt < max_attempts:
            try:
                logging.info(f"Attempting to connect to broker (attempt {reconnect_attempt + 1}/{max_attempts})...")
                
                # Reset previous tick state for new connection
                previous_tick = {
                    "total_traded_volume": None,
                    "ltp": None,
                    "trade_direction": None,
                }
                
                # Authenticate
                smartApi = SmartConnect(API_KEY)
                totp = pyotp.TOTP(AUTH_TOKEN).now()
                session_data = smartApi.generateSession(CLIENT_CODE, PASS, totp)
                
                if not session_data['status']:
                    raise Exception(f"Authentication failed: {session_data.get('message', 'Unknown error')}")
                
                authToken = session_data['data']['jwtToken']
                FEED_TOKEN = smartApi.getfeedToken()
                logging.info("Broker authentication successful.")
                
                # Create WebSocket connection
                sws = SmartWebSocketV2(authToken, API_KEY, CLIENT_CODE, FEED_TOKEN)
                
                connection_established = threading.Event()
                connection_closed = threading.Event()
                
                def on_open(wsapp):
                    logging.info("Broker WebSocket connection opened.")
                    sws.subscribe("tick_stream", 2, [{"exchangeType": 2, "tokens": [INSTRUMENT_TOKEN]}])
                    logging.info(f"Subscribed to instrument token: {INSTRUMENT_TOKEN}")
                    
                    with broker_state_lock:
                        broker_state["connected"] = True
                        broker_state["connecting"] = False
                        broker_state["websocket"] = sws
                        broker_state["reconnect_attempts"] = 0
                    
                    connection_established.set()

                def on_data(wsapp, message):
                    """
                    Minimal on_data handler - just queues raw messages.
                    All processing is done in tick_processor_thread for better decoupling.
                    """
                    try:
                        if broker_state["stop_requested"]:
                            return
                        # Queue raw message for processing in separate thread
                        raw_tick_queue.put_nowait(message)
                    except Exception as e:
                        logging.error(f"Error queuing broker message: {e}")

                def on_error(wsapp, error):
                    logging.error(f"Broker WebSocket error: {error}")
                    with broker_state_lock:
                        broker_state["last_error"] = str(error)
                
                def on_close(wsapp, code=None, msg=None):
                    logging.info(f"Broker WebSocket connection closed. Code: {code}, Message: {msg}")
                    with broker_state_lock:
                        broker_state["connected"] = False
                        broker_state["websocket"] = None
                    connection_closed.set()
                
                sws.on_open = on_open
                sws.on_data = on_data
                sws.on_error = on_error
                sws.on_close = on_close
                
                # Start WebSocket in a separate thread
                ws_thread = threading.Thread(target=sws.connect, daemon=True)
                ws_thread.start()
                
                # Wait for connection to establish or fail
                if connection_established.wait(timeout=30):
                    logging.info("Broker connection established successfully")
                    reconnect_attempt = 0  # Reset on successful connection
                    
                    # Wait until connection closes or stop requested
                    while not broker_state["stop_requested"] and not connection_closed.is_set():
                        connection_closed.wait(timeout=1)
                    
                    if broker_state["stop_requested"]:
                        logging.info("Stop requested, closing broker connection")
                        try:
                            sws.close_connection()
                        except Exception:
                            pass
                        break
                else:
                    raise Exception("Connection timeout - failed to establish connection within 30 seconds")
                
            except Exception as e:
                logging.error(f"Broker connection error: {e}")
                with broker_state_lock:
                    broker_state["connected"] = False
                    broker_state["connecting"] = False
                    broker_state["last_error"] = str(e)
                    broker_state["reconnect_attempts"] = reconnect_attempt + 1
            
            # Check if we should retry
            if not broker_state["stop_requested"] and reconnect_attempt < max_attempts - 1:
                reconnect_attempt += 1
                # Exponential backoff: 1s, 2s, 4s, 8s, 16s
                wait_time = 2 ** reconnect_attempt
                logging.info(f"Reconnecting in {wait_time} seconds...")
                
                # Wait with stop check
                for _ in range(wait_time):
                    if broker_state["stop_requested"]:
                        break
                    time.sleep(1)
            else:
                break
        
        with broker_state_lock:
            broker_state["connected"] = False
            broker_state["connecting"] = False
            if reconnect_attempt >= max_attempts:
                broker_state["last_error"] = f"Max reconnection attempts ({max_attempts}) reached"
                logging.error(broker_state["last_error"])
        
        logging.info("Broker connection worker thread exiting")
    
    # Start connection in background thread
    broker_state["thread"] = threading.Thread(target=connection_worker, name="BrokerThread", daemon=True)
    broker_state["thread"].start()
    return True

def stop_broker_connection():
    """Stop the broker WebSocket connection."""
    global broker_state
    
    with broker_state_lock:
        broker_state["stop_requested"] = True
        
        if broker_state["websocket"]:
            try:
                broker_state["websocket"].close_connection()
            except Exception as e:
                logging.error(f"Error closing websocket: {e}")
        
        broker_state["connected"] = False
        broker_state["connecting"] = False
    
    logging.info("Broker disconnect requested")
    return True

def get_broker_status():
    """Get current broker connection status."""
    with broker_state_lock:
        return {
            "connected": broker_state["connected"],
            "connecting": broker_state["connecting"],
            "reconnect_attempts": broker_state["reconnect_attempts"],
            "max_reconnect_attempts": broker_state["max_reconnect_attempts"],
            "last_error": broker_state["last_error"],
        }

# --- Flask Web Server Routes ---

@app.route('/')
def index():
    return render_template('index_optimized.html')

# --- Broker Control API Endpoints ---

@app.route('/broker/connect', methods=['POST'])
def broker_connect():
    """Start broker WebSocket connection."""
    success = start_broker_connection()
    if success:
        return jsonify({"status": "connecting", "message": "Broker connection initiated"})
    else:
        return jsonify({"status": "error", "message": "Connection already in progress"}), 400

@app.route('/broker/disconnect', methods=['POST'])
def broker_disconnect():
    """Stop broker WebSocket connection."""
    stop_broker_connection()
    return jsonify({"status": "disconnected", "message": "Broker disconnection initiated"})

@app.route('/broker/status', methods=['GET'])
def broker_status():
    """Get current broker connection status."""
    return jsonify(get_broker_status())

# --- History API Endpoints ---

@app.route('/history')
def get_history():
    """Returns historical ticks. Optionally accepts start_time and end_time query parameters."""
    start_time = request.args.get('start_time', type=int)
    end_time = request.args.get('end_time', type=int)
    
    # If no time range specified, return today's data
    if not end_time:
        end_time = int(time.time())
    if not start_time:
        # Get start of current day (midnight)
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start_time = int(today.timestamp())
    
    ticks = load_ticks_from_db(start_time, end_time)
    return jsonify(ticks)

@app.route('/history/dates')
def get_history_dates():
    """Returns the date range available in the database."""
    date_range = get_date_range_from_db()
    return jsonify(date_range)

@app.route('/history/available_dates')
def get_available_dates():
    """Returns list of available dates in YYYY-MM-DD format."""
    dates = get_available_dates_from_db()
    return jsonify(dates)

@app.route('/history/day')
def get_history_day():
    """Returns ticks for a specific day. Accepts 'date' parameter in YYYY-MM-DD format or timestamp."""
    date_param = request.args.get('date')
    timestamp_param = request.args.get('timestamp', type=int)
    
    if timestamp_param:
        start_time, end_time = get_day_boundaries(timestamp_param)
    elif date_param:
        try:
            dt = datetime.strptime(date_param, '%Y-%m-%d')
            start_time, end_time = get_day_boundaries(dt.timestamp())
        except ValueError:
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400
    else:
        # Default to today
        start_time, end_time = get_day_boundaries(time.time())
    
    ticks = load_ticks_from_db(start_time, end_time)
    
    # Merge with in-memory cache (for ticks not yet flushed to DB)
    with recent_ticks_cache_lock:
        for cached_tick in recent_ticks_cache:
            if start_time <= cached_tick['timestamp'] <= end_time:
                # Only add if not already in DB results (check by timestamp)
                if not any(t['timestamp'] == cached_tick['timestamp'] for t in ticks):
                    ticks.append(cached_tick)
    
    # Re-sort after merging
    ticks.sort(key=lambda t: t['timestamp'])
    
    return jsonify({
        "start_time": start_time,
        "end_time": end_time,
        "ticks": ticks
    })

# --- SSE Stream Endpoint ---

@app.route('/stream')
def stream():
    """Server-Sent Events endpoint for streaming live tick updates."""
    def generate():
        # Create a queue for this subscriber
        subscriber_queue = Queue(maxsize=1000)
        
        with subscribers_lock:
            tick_subscribers.append(subscriber_queue)
        
        try:
            while True:
                try:
                    tick = subscriber_queue.get(timeout=30)
                    yield f"data: {json.dumps(tick)}\n\n"
                except Empty:
                    # Send keep-alive
                    yield ": keep-alive\n\n"
        finally:
            # Clean up subscriber on disconnect
            with subscribers_lock:
                if subscriber_queue in tick_subscribers:
                    tick_subscribers.remove(subscriber_queue)
    
    return Response(generate(), mimetype='text/event-stream')

# --- SocketIO Event Handlers ---

@socketio.on('connect')
def handle_connect():
    """Handle client connection."""
    logging.info(f"SocketIO client connected: {request.sid}")

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection."""
    logging.info(f"SocketIO client disconnected: {request.sid}")

@socketio.on('ping')
def handle_ping():
    """Handle ping from client."""
    emit('pong')

if __name__ == '__main__':
    # --- APPLICATION STARTUP SEQUENCE ---
    # 1. Initialize the database schema
    init_db()

    # 2. Start the database writer using eventlet greenlet
    eventlet.spawn(db_writer_thread)
    logging.info("Database writer greenlet started")

    # 3. Start the tick processor using eventlet greenlet (decoupled from broker's on_data)
    eventlet.spawn(tick_processor_thread)
    logging.info("Tick processor greenlet started")

    # 4. DO NOT auto-start broker connection - wait for user to connect via UI
    logging.info("Broker connection is manual - use /broker/connect API to start")
    
    # 5. Start the Flask-SocketIO server (eventlet provides production-ready server)
    logging.info("Starting Flask-SocketIO server with eventlet on http://0.0.0.0:5002")
    socketio.run(app, host='0.0.0.0', port=5002, debug=False)



