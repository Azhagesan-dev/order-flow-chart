"""
CSV to Database Import Script for Trading Ticks

This script imports Time & Sales data from CSV files into the optimized tick database.
It processes the data in the same way as live broker data, determining trade direction
based on price changes.

Usage:
    python import_csv_to_db.py --csv "Nifty Ticklist 26112025.csv" --instrument NIFTY --lotsize 75
"""

import sqlite3
import csv
import argparse
import logging
from datetime import datetime
from dotenv import load_dotenv
import os
import io

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Load environment variables
load_dotenv()

def parse_timestamp(time_str):
    """Parse timestamp from CSV format to Unix timestamp."""
    try:
        # Format: "26-11-2025 15:30:01"
        dt = datetime.strptime(time_str, "%d-%m-%Y %H:%M:%S")
        return int(dt.timestamp())
    except Exception as e:
        logging.warning(f"Error parsing timestamp '{time_str}': {e}")
        return None

def import_csv_to_db(csv_path, db_path, instrument_token, lotsize, replace_existing=False):
    """
    Import CSV data into the ticks database.
    
    Args:
        csv_path: Path to the CSV file
        db_path: Path to the SQLite database
        instrument_token: Instrument token identifier
        lotsize: Lot size for volume normalization
        replace_existing: If True, delete existing data for this instrument before import
    """
    
    logging.info(f"Starting CSV import from: {csv_path}")
    logging.info(f"Target database: {db_path}")
    logging.info(f"Instrument: {instrument_token}, Lot size: {lotsize}")
    
    # Read and parse CSV
    ticks = []
    skipped_count = 0
    
    try:
        # Try different encodings (utf-8-sig first to handle BOM)
        encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']
        csv_content = None
        
        for encoding in encodings:
            try:
                with open(csv_path, 'r', encoding=encoding) as f:
                    csv_content = f.read()
                logging.info(f"Successfully read CSV with encoding: {encoding}")
                break
            except UnicodeDecodeError:
                continue
        
        if csv_content is None:
            raise Exception("Could not read CSV with any supported encoding")
        
        # Parse CSV
        reader = csv.DictReader(io.StringIO(csv_content))
        
        # Strip whitespace and BOM from column names
        reader.fieldnames = [name.strip().lstrip('\ufeff') for name in reader.fieldnames]
        
        logging.info(f"CSV columns found: {reader.fieldnames}")
        
        # Validate required columns
        required_columns = ['Time', 'Last Rate', 'Volume']
        missing_columns = [col for col in required_columns if col not in reader.fieldnames]
        
        if missing_columns:
            raise Exception(f"Missing required columns: {missing_columns}. Found columns: {reader.fieldnames}")
        
        for row in reader:
            try:
                # Parse data
                time_str = row['Time'].strip()
                price = float(row['Last Rate'].strip())
                volume = float(row['Volume'].strip())
                
                # Filter out volume = 1 as requested
                if volume == 1:
                    skipped_count += 1
                    continue
                
                # Parse timestamp
                timestamp = parse_timestamp(time_str)
                if timestamp is None:
                    skipped_count += 1
                    continue
                
                # Normalize volume by lot size
                normalized_volume = volume / lotsize
                
                ticks.append({
                    'timestamp': timestamp,
                    'price': price,
                    'volume': normalized_volume,
                    'time_str': time_str
                })
            except Exception as e:
                logging.warning(f"Error processing row: {e}")
                skipped_count += 1
                continue
        
        logging.info(f"Loaded {len(ticks)} ticks from CSV (skipped {skipped_count} rows)")
        
    except Exception as e:
        logging.error(f"Error reading CSV file: {e}")
        return False
    
    if not ticks:
        logging.error("No valid ticks found in CSV")
        return False
    
    # Sort ticks by timestamp (ascending - oldest first)
    ticks.sort(key=lambda x: x['timestamp'])
    logging.info(f"Sorted ticks chronologically")
    
    # Determine trade direction based on price changes
    previous_price = None
    previous_direction = None
    
    for tick in ticks:
        if previous_price is None:
            tick['direction'] = 'BUY'
        elif tick['price'] > previous_price:
            tick['direction'] = 'BUY'
        elif tick['price'] < previous_price:
            tick['direction'] = 'SELL'
        else:
            tick['direction'] = previous_direction if previous_direction else 'BUY'
        
        previous_price = tick['price']
        previous_direction = tick['direction']
    
    # Connect to database
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
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
        
        # Create index if it doesn't exist
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_instrument_timestamp 
            ON ticks(instrument_token, timestamp)
        ''')
        
        # Replace existing data if requested
        if replace_existing:
            cursor.execute('DELETE FROM ticks WHERE instrument_token = ?', (instrument_token,))
            deleted_count = cursor.rowcount
            logging.info(f"Deleted {deleted_count} existing ticks for instrument {instrument_token}")
        
        # Check for existing data in the time range
        min_ts = ticks[0]['timestamp']
        max_ts = ticks[-1]['timestamp']
        
        cursor.execute('''
            SELECT COUNT(*) FROM ticks 
            WHERE instrument_token = ? 
            AND timestamp >= ? 
            AND timestamp <= ?
        ''', (instrument_token, min_ts, max_ts))
        
        existing_count = cursor.fetchone()[0]
        
        if existing_count > 0 and not replace_existing:
            logging.warning(f"Found {existing_count} existing ticks in this time range")
            response = input("Do you want to (r)eplace, (s)kip, or (m)erge? [r/s/m]: ").lower()
            
            if response == 'r':
                cursor.execute('''
                    DELETE FROM ticks 
                    WHERE instrument_token = ? 
                    AND timestamp >= ? 
                    AND timestamp <= ?
                ''', (instrument_token, min_ts, max_ts))
                logging.info(f"Deleted {cursor.rowcount} existing ticks in time range")
            elif response == 's':
                logging.info("Import cancelled by user")
                conn.close()
                return False
        
        # Prepare batch insert
        batch = []
        for tick in ticks:
            batch.append((
                instrument_token,
                tick['timestamp'],
                tick['price'],
                tick['volume'],
                tick['direction']
            ))
        
        # Insert in batches
        batch_size = 1000
        total_inserted = 0
        
        for i in range(0, len(batch), batch_size):
            chunk = batch[i:i + batch_size]
            cursor.executemany('''
                INSERT INTO ticks (instrument_token, timestamp, price, volume, direction)
                VALUES (?, ?, ?, ?, ?)
            ''', chunk)
            total_inserted += len(chunk)
            logging.info(f"Inserted {total_inserted}/{len(batch)} ticks...")
        
        conn.commit()
        logging.info(f"Successfully imported {total_inserted} ticks to database")
        
        # Display statistics
        cursor.execute('''
            SELECT 
                direction, 
                COUNT(*) as count, 
                SUM(volume) as total_volume,
                MIN(price) as min_price,
                MAX(price) as max_price
            FROM ticks 
            WHERE instrument_token = ?
            AND timestamp >= ?
            AND timestamp <= ?
            GROUP BY direction
        ''', (instrument_token, min_ts, max_ts))
        
        print("\n" + "="*60)
        print("Import Statistics:")
        print("="*60)
        for row in cursor.fetchall():
            direction, count, total_vol, min_price, max_price = row
            print(f"{direction:5s}: {count:6d} ticks, Volume: {total_vol:10.2f}, Price range: {min_price:.2f} - {max_price:.2f}")
        
        # Time range
        min_dt = datetime.fromtimestamp(min_ts)
        max_dt = datetime.fromtimestamp(max_ts)
        print(f"\nTime range: {min_dt} to {max_dt}")
        print(f"Duration: {(max_ts - min_ts) / 60:.1f} minutes")
        print("="*60 + "\n")
        
        conn.close()
        return True
        
    except Exception as e:
        logging.error(f"Database error: {e}")
        if 'conn' in locals():
            conn.close()
        return False

def analyze_csv(csv_path):
    """Analyze CSV file and display statistics without importing."""
    try:
        # Try different encodings (utf-8-sig first to handle BOM)
        encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']
        csv_content = None
        
        for encoding in encodings:
            try:
                with open(csv_path, 'r', encoding=encoding) as f:
                    csv_content = f.read()
                break
            except UnicodeDecodeError:
                continue
        
        if csv_content is None:
            raise Exception("Could not read CSV with any supported encoding")
        
        reader = csv.DictReader(io.StringIO(csv_content))
        # Strip whitespace and BOM from column names
        reader.fieldnames = [name.strip().lstrip('\ufeff') for name in reader.fieldnames]
        
        total_rows = 0
        volume_one_count = 0
        min_price = float('inf')
        max_price = float('-inf')
        total_volume = 0
        timestamps = []
        
        for row in reader:
            total_rows += 1
            price = float(row['Last Rate'].strip())
            volume = float(row['Volume'].strip())
            
            if volume == 1:
                volume_one_count += 1
            
            min_price = min(min_price, price)
            max_price = max(max_price, price)
            total_volume += volume
            
            timestamp = parse_timestamp(row['Time'].strip())
            if timestamp:
                timestamps.append(timestamp)
        
        print("\n" + "="*60)
        print(f"CSV Analysis: {csv_path}")
        print("="*60)
        print(f"Total rows: {total_rows}")
        print(f"Rows with Volume=1: {volume_one_count} ({volume_one_count/total_rows*100:.1f}%)")
        print(f"Rows to import: {total_rows - volume_one_count}")
        print(f"Price range: {min_price:.2f} - {max_price:.2f}")
        print(f"Total volume: {total_volume:,.0f}")
        
        if timestamps:
            timestamps.sort()
            min_dt = datetime.fromtimestamp(timestamps[0])
            max_dt = datetime.fromtimestamp(timestamps[-1])
            print(f"Time range: {min_dt} to {max_dt}")
            print(f"Duration: {(timestamps[-1] - timestamps[0]) / 60:.1f} minutes")
        
        print("="*60 + "\n")
        
    except Exception as e:
        logging.error(f"Error analyzing CSV: {e}")

def main():
    parser = argparse.ArgumentParser(description='Import CSV tick data to database')
    parser.add_argument('--csv', required=True, help='Path to CSV file')
    parser.add_argument('--db', default=None, help='Database path (default: from .env or trading_data_ticks.db)')
    parser.add_argument('--instrument', default=None, help='Instrument token (default: from .env)')
    parser.add_argument('--lotsize', type=int, default=None, help='Lot size (default: from .env or 75)')
    parser.add_argument('--replace', action='store_true', help='Replace all existing data for this instrument')
    parser.add_argument('--analyze-only', action='store_true', help='Only analyze CSV, do not import')
    
    args = parser.parse_args()
    
    # Analyze mode
    if args.analyze_only:
        analyze_csv(args.csv)
        return
    
    # Get configuration
    db_path = args.db or os.getenv('DB_NAME', 'trading_data_ticks.db')
    instrument_token = args.instrument or os.getenv('INSTRUMENT_TOKEN')
    lotsize = args.lotsize or int(os.getenv('LOTSIZE', 75))
    
    if not instrument_token:
        print("Error: Instrument token not specified. Use --instrument or set INSTRUMENT_TOKEN in .env")
        return
    
    print(f"\nImport Configuration:")
    print(f"  CSV file: {args.csv}")
    print(f"  Database: {db_path}")
    print(f"  Instrument: {instrument_token}")
    print(f"  Lot size: {lotsize}")
    print(f"  Replace existing: {args.replace}")
    
    if not args.replace:
        response = input("\nProceed with import? (yes/no): ")
        if response.lower() != 'yes':
            print("Import cancelled")
            return
    
    # Perform import
    success = import_csv_to_db(args.csv, db_path, instrument_token, lotsize, args.replace)
    
    if success:
        print("\n✅ Import completed successfully!")
        print(f"\nYou can now run the application to view the imported data:")
        print(f"  python app_optimized.py")
    else:
        print("\n❌ Import failed. Check the logs above for details.")

if __name__ == '__main__':
    main()
