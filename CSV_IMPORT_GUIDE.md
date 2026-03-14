# CSV Import Guide

## Overview

The `import_csv_to_db.py` script imports Time & Sales data from CSV files into the optimized tick database. It processes the data exactly like live broker data, determining trade direction based on price changes.

## Features

✅ **Automatic Volume Filtering** - Removes rows with Volume = 1  
✅ **Trade Direction Detection** - Determines BUY/SELL based on price changes  
✅ **Conflict Handling** - Options to replace, skip, or merge with existing data  
✅ **Batch Processing** - Efficient bulk inserts  
✅ **Statistics Display** - Shows import summary and data analysis  
✅ **CSV Analysis Mode** - Preview data before importing  

## CSV Format

Your CSV file should have these columns:
```csv
Time,Last Rate,Volume
26-11-2025 15:30:01,26390.00,75
26-11-2025 15:30:00,26390.00,1500
...
```

**Column Mapping**:
- `Time` → Timestamp (format: DD-MM-YYYY HH:MM:SS)
- `Last Rate` → Price (LTP)
- `Volume` → Trade volume

## Usage

### 1. Analyze CSV (Preview)

Before importing, analyze the CSV to see what data it contains:

```bash
python import_csv_to_db.py --csv "Nifty Ticklist 26112025.csv" --analyze-only
```

**Output**:
```
============================================================
CSV Analysis: Nifty Ticklist 26112025.csv
============================================================
Total rows: 10876
Rows with Volume=1: 234 (2.2%)
Rows to import: 10642
Price range: 26372.00 - 26392.00
Total volume: 1,234,567
Time range: 2025-11-26 09:15:00 to 2025-11-26 15:30:01
Duration: 375.0 minutes
============================================================
```

### 2. Import CSV (Basic)

Import using settings from `.env`:

```bash
python import_csv_to_db.py --csv "Nifty Ticklist 26112025.csv"
```

This will:
- Use `INSTRUMENT_TOKEN` from `.env`
- Use `LOTSIZE` from `.env` (default: 75)
- Use `DB_NAME` from `.env` (default: trading_data_ticks.db)
- Prompt for confirmation before importing

### 3. Import with Custom Settings

Override environment variables:

```bash
python import_csv_to_db.py --csv "Nifty Ticklist 26112025.csv" --instrument NIFTY --lotsize 75 --db my_data.db
```

### 4. Replace Existing Data

Replace all existing data for the instrument:

```bash
python import_csv_to_db.py --csv "Nifty Ticklist 26112025.csv" --replace
```

**Warning**: This deletes ALL existing ticks for the instrument before importing!

## Import Process

### Step 1: Data Loading
```
2025-11-27 07:35:00 - Starting CSV import from: Nifty Ticklist 26112025.csv
2025-11-27 07:35:00 - Target database: trading_data_ticks.db
2025-11-27 07:35:00 - Instrument: NIFTY, Lot size: 75
2025-11-27 07:35:01 - Loaded 10642 ticks from CSV (skipped 234 rows)
```

### Step 2: Direction Detection
The script determines trade direction using the same logic as live broker data:
- **Price UP** → BUY (aggressive buyer)
- **Price DOWN** → SELL (aggressive seller)
- **Price UNCHANGED** → Use previous direction

### Step 3: Database Insert
```
2025-11-27 07:35:02 - Inserted 1000/10642 ticks...
2025-11-27 07:35:03 - Inserted 2000/10642 ticks...
...
2025-11-27 07:35:10 - Successfully imported 10642 ticks to database
```

### Step 4: Statistics
```
============================================================
Import Statistics:
============================================================
BUY  :   5321 ticks, Volume:  123456.78, Price range: 26372.00 - 26392.00
SELL :   5321 ticks, Volume:  123456.78, Price range: 26372.00 - 26392.00

Time range: 2025-11-26 09:15:00 to 2025-11-26 15:30:01
Duration: 375.0 minutes
============================================================
```

## Conflict Handling

If data already exists in the time range, you'll be prompted:

```
Found 5000 existing ticks in this time range
Do you want to (r)eplace, (s)kip, or (m)erge? [r/s/m]:
```

**Options**:
- `r` (Replace) - Delete existing ticks in time range, then import
- `s` (Skip) - Cancel import
- `m` (Merge) - Insert new ticks alongside existing ones

## Examples

### Example 1: Daily Import Workflow

```bash
# 1. Analyze the CSV
python import_csv_to_db.py --csv "Nifty_26Nov.csv" --analyze-only

# 2. Import if data looks good
python import_csv_to_db.py --csv "Nifty_26Nov.csv"

# 3. Start the application
python app_optimized.py
```

### Example 2: Multiple Instruments

```bash
# Import NIFTY data
python import_csv_to_db.py --csv "Nifty_26Nov.csv" --instrument NIFTY --lotsize 75

# Import BANKNIFTY data
python import_csv_to_db.py --csv "BankNifty_26Nov.csv" --instrument BANKNIFTY --lotsize 25
```

### Example 3: Replace Corrupted Data

```bash
# Delete and reimport
python import_csv_to_db.py --csv "Nifty_26Nov.csv" --replace
```

## Data Validation

The script performs several validations:

1. **Timestamp Parsing** - Ensures valid date/time format
2. **Volume Filtering** - Removes Volume = 1 rows
3. **Price Validation** - Ensures numeric prices
4. **Chronological Sorting** - Orders ticks by timestamp
5. **Direction Logic** - Applies same rules as live broker

## Troubleshooting

### Issue: "Error parsing timestamp"
**Solution**: Check CSV date format. Should be `DD-MM-YYYY HH:MM:SS`

### Issue: "No valid ticks found"
**Solution**: Verify CSV has correct columns: `Time`, `Last Rate`, `Volume`

### Issue: "Database is locked"
**Solution**: Stop `app_optimized.py` before importing

### Issue: Wrong direction detection
**Solution**: CSV should be sorted chronologically (oldest first). The script will sort automatically.

## Integration with Live Data

The imported CSV data integrates seamlessly with live broker data:

1. **Import historical CSV** - Backfill database with past data
2. **Start live app** - `python app_optimized.py`
3. **Live data appends** - New ticks from broker are added to database
4. **Continuous chart** - Historical + live data displayed together

## Performance

- **Import speed**: ~10,000 ticks/second
- **Memory usage**: Minimal (batch processing)
- **Database size**: ~50-100 bytes per tick

## Best Practices

1. **Always analyze first** - Use `--analyze-only` to preview data
2. **Backup database** - Before using `--replace`
3. **Check time zones** - Ensure CSV timestamps match your timezone
4. **Verify lot size** - Use correct lot size for volume normalization
5. **Monitor statistics** - Check BUY/SELL distribution is reasonable

## Advanced Usage

### Custom Time Range Import

Modify the script to filter by time range:

```python
# In import_csv_to_db.py, add time filtering
if timestamp < start_time or timestamp > end_time:
    continue
```

### Multiple CSV Files

```bash
# Batch import
for file in *.csv; do
    python import_csv_to_db.py --csv "$file"
done
```

### Export After Import

```bash
# Import CSV
python import_csv_to_db.py --csv "data.csv"

# Verify with query
sqlite3 trading_data_ticks.db "SELECT COUNT(*) FROM ticks"
```

## Conclusion

The CSV import script provides a robust way to:
- ✅ Import historical Time & Sales data
- ✅ Process data like live broker ticks
- ✅ Integrate with the optimized system
- ✅ Maintain data consistency

For questions or issues, refer to the main `QUICKSTART.md` guide.
