# Real-Time Footprint Chart for Stock Market Analysis

This project is a web-based, real-time footprint chart visualization tool for stock market data. It is built with a Python Flask backend that connects to a broker's WebSocket for live tick data, processes it into candles and footprint bars, and streams the updates to a modern, interactive frontend built with Lightweight Charts.

![Footprint Chart Screenshot](chart.png)

## Features

- **Real-Time Data Streaming**: Connects to brokers via WebSocket using a decoupled architecture for ultra-low latency processing.
- **Advanced Orderflow Visualization**: Detailed footprint bars, volume profiles, delta modes, and diagonal imbalances.
- **Optimized Performance**: Backend powered by `eventlet` and `SocketIO` for handling high-frequency tick data smoothly.
- **Historical Data Management**: Robust tick-level storage in SQLite with built-in CSV import tools for offline analysis.
- **Interactive Navigation**: "Goto Date" feature and persistent UI settings for a seamless analysis workflow.

## Technology Stack

- **Backend**: Python, Flask, eventlet (Production-grade asynchronous server)
- **Real-time Communication**: SocketIO (Main), Server-Sent Events (Legacy support)
- **Broker Integration**: SmartAPI
- **Database**: SQLite (Tick-level storage)
- **Frontend**: HTML5, JavaScript, Lightweight Charts v4.2+

## Setup and Installation

Follow these steps to get the application running on your local machine.

### 1. Clone the Repository

```bash
git clone https://github.com/Alex-dev-angel/order-flow-chart
cd order-flow-chart
```

### 2. Create a Virtual Environment

It is highly recommended to use a virtual environment to manage project dependencies.

```bash
# For Windows
python -m venv venv
venv\Scripts\activate

# For macOS/Linux
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

Install all the required Python packages using the `requirements.txt` file.

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

The project uses a `.env.example` file as a template for the required credentials.

First, make a copy of this file and name it `.env`:

```bash
# For Windows
copy .env.example .env

# For macOS/Linux
cp .env.example .env
```

Next, open the newly created `.env` file and replace the placeholder values with your actual broker credentials and settings.

```env
# Contents of your .env file
API_KEY="YOUR_API_KEY"
CLIENT_CODE="YOUR_CLIENT_CODE"
PASS="YOUR_LOGIN_PASSWORD"
AUTH_TOKEN="YOUR_2FA_SECRET_KEY"  # The secret key from your 2FA app, not the 6-digit code
INSTRUMENT_TOKEN="53001" # Example: NIFTY Token
LOTSIZE=75 # Example: Lot size for the instrument
DB_NAME="trading_data_ticks.db"
```
**Note**: The `.env` file contains sensitive information and should **never** be committed to version control. The `.gitignore` file should already be configured to ignore it.

### 6. Run the Application

Start the optimized application using the following command:

```bash
python app_optimized.py
```

The application will be accessible at `http://localhost:5002`.

## Data Management

### Importing Historical Data
You can import historical tick data from CSV files into the database:

```bash
python import_csv_to_db.py --csv "your_data.csv" --instrument "NIFTY" --lotsize 75
```
See `CSV_IMPORT_GUIDE.md` for more details.

## How It Works

1.  **Decoupled Ingestion**: Raw tick data is received from the broker and instantly queued.
2.  **Tick Processing**: A dedicated background worker processes the queue, calculating trade direction and delta.
3.  **Batch Storage**: Processed ticks are written to the database in batches to minimize disk I/O and prevent lag.
4.  **Interactive Frontend**: The UI provides specialized orderflow modes (Delta, Profile, Dual Side) and communicates via SocketIO for real-time updates.

---