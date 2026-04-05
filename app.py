import streamlit as st
import pandas as pd
import requests
import datetime
import time
import os
import gzip
import json

import concurrent.futures

# --- Helpers ---
def get_ist_now():
    """Get current time in IST (UTC+5:30)"""
    return datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)

# --- Configuration ---
st.set_page_config(page_title="Intraday Option Scanner", layout="wide")

# Client View Toggle (Hide Sidebar)
client_view = st.checkbox("Enable Client View (Full Page)", value=False)

if client_view:
    st.markdown(
        """
        <style>
            [data-testid="stSidebar"] {display: none;}
            [data-testid="collapsedControl"] {display: none;}
        </style>
        """,
        unsafe_allow_html=True,
    )

# Update time for header
if not client_view:
    update_time = get_ist_now().strftime("%Y-%m-%d %H:%M:%S")
    st.markdown(f"""
        <div style='display: flex; justify-content: space-between; align-items: center; margin-top: -20px; margin-bottom: 10px;'>
            <h3 style='margin: 0;'>Intraday Option Scanner</h3>
            <span style='font-size: 1rem; color: #555;'>Last Updated: {update_time} (IST)</span>
        </div>
    """, unsafe_allow_html=True)

# --- Sidebar Inputs ---
st.sidebar.header("Configuration")

# --- Token Management ---
TOKEN_FILE = ".token_cache"

def get_today_str():
    """Get today's date in IST string format"""
    return str(get_ist_now().date())

def load_cached_creds():
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, "r") as f:
                data = json.load(f)
                
                cached_client_id = data.get("client_id")
                cached_token = data.get("token")
                
                # Token is valid only for today
                if data.get("date") != get_today_str():
                    cached_token = None
                
                return cached_token, cached_client_id
        except:
            pass
    return None, None

def save_creds_to_cache(token, client_id):
    with open(TOKEN_FILE, "w") as f:
        json.dump({"token": token, "client_id": client_id, "date": get_today_str()}, f)

# Input for Access Token and Client ID (Frontend Only)
cached_token, cached_client_id = load_cached_creds()
client_id = st.sidebar.text_input("Enter Client ID", value=cached_client_id if cached_client_id else "")
access_token = st.sidebar.text_input("Enter Access Token", type="password", value=cached_token if cached_token else "")

if access_token and client_id:
    # If user entered new creds, save them
    if access_token != cached_token or client_id != cached_client_id:
        save_creds_to_cache(access_token, client_id)
        st.sidebar.success("✅ Credentials Saved for Today")
    else:
        st.sidebar.success("✅ Credentials Loaded from Cache (Valid for Today)")
else:
    st.warning("Please enter your Client ID and Access Token in the sidebar to proceed.")
    st.stop()

# --- Auto-Refresh Controls ---
st.sidebar.markdown("---")
st.sidebar.header("Auto-Refresh Settings")
atm_mode = st.sidebar.radio("ATM Strike Based On:", ("Fixed (Open Price)", "Dynamic (LTP)"), index=0)
auto_refresh = st.sidebar.checkbox("Enable Auto-Refresh", value=False)
refresh_interval = st.sidebar.number_input("Refresh Interval (seconds)", min_value=5, value=30, step=5)

# --- Instruments Data Synchronization ---
INSTRUMENTS_FILE = 'api-scrip-master.csv'
CACHE_FILE = 'instruments_cache.pkl'

def get_file_date(filepath):
    if not os.path.exists(filepath):
        return None
    return datetime.datetime.fromtimestamp(os.path.getmtime(filepath)).date()

def is_file_fresh(filepath):
    """Check if file exists and is from today"""
    f_date = get_file_date(filepath)
    if f_date:
        return f_date == datetime.date.today()
    return False

# Sidebar: Scrip Master Management
st.sidebar.markdown("---")
st.sidebar.header("Data Management")

SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

def download_scrip_master():
    try:
        response = requests.get(SCRIP_MASTER_URL, timeout=30)
        response.raise_for_status()
        with open(INSTRUMENTS_FILE, 'wb') as f:
            f.write(response.content)
        return True, "Download successful"
    except Exception as e:
        return False, str(e)

file_date = get_file_date(INSTRUMENTS_FILE)
is_fresh = False
if file_date:
    is_fresh = (file_date == datetime.date.today())

# Auto-download if missing or not fresh
if not is_fresh:
    status_placeholder = st.sidebar.empty()
    status_placeholder.info("⏳ Scrip Master outdated/missing. Downloading...")
    
    success, msg = download_scrip_master()
    if success:
        # Clear cache to force reload
        if os.path.exists(CACHE_FILE):
            os.remove(CACHE_FILE)
        st.cache_data.clear()
        
        status_placeholder.success(f"✅ Updated Scrip Master ({get_today_str()})")
        is_fresh = True
    else:
        status_placeholder.error(f"❌ Auto-download failed: {msg}")

# Display status if fresh (or became fresh)
if is_fresh:
    st.sidebar.success(f"✅ Scrip Master: Up to date")
    st.sidebar.caption(f"Date: {get_today_str()}")
else:
    # Fallback UI if download failed
    if st.sidebar.button("� Retry Download"):
        with st.sidebar.status("Retrying..."):
            success, msg = download_scrip_master()
            if success:
                if os.path.exists(CACHE_FILE):
                    os.remove(CACHE_FILE)
                st.cache_data.clear()
                st.success("Updated!")
                st.rerun()
            else:
                st.error(f"Failed: {msg}")

with st.sidebar.expander("📂 Upload Manually"):
    uploaded_file = st.file_uploader("Select 'api-scrip-master.csv'", type=['csv'])
    if uploaded_file is not None:
        try:
            # Save the file
            with open(INSTRUMENTS_FILE, "wb") as f:
                f.write(uploaded_file.getbuffer())
            
            # Clear cache to force reload
            if os.path.exists(CACHE_FILE):
                os.remove(CACHE_FILE)
            st.cache_data.clear()
            
            st.success("File updated! Reloading...")
            time.sleep(1)
            st.rerun()
        except Exception as e:
            st.error(f"Error: {e}")

# --- Data Loading ---
@st.cache_data(ttl=3600*4, show_spinner=False)  # Cache for 4 hours
def load_data():
    df = None
    
    # 1. Try to load from fast pickle cache first
    if is_file_fresh(CACHE_FILE):
        try:
            df = pd.read_pickle(CACHE_FILE)
            # print("DEBUG: Loaded from Pickle Cache")
        except Exception:
            df = None

    # 2. If no cache, load from CSV
    if df is None:
        if not os.path.exists(INSTRUMENTS_FILE):
            st.error(f"Instruments file not found: {INSTRUMENTS_FILE}")
            return pd.DataFrame(), pd.DataFrame()

        # Load and Filter CSV directly
        try:
            # Read CSV
            # Columns: SEM_EXM_EXCH_ID,SEM_SEGMENT,SEM_SMST_SECURITY_ID,SEM_INSTRUMENT_NAME,SEM_EXPIRY_CODE,SEM_TRADING_SYMBOL,
            # SEM_LOT_UNITS,SEM_CUSTOM_SYMBOL,SEM_EXPIRY_DATE,SEM_STRIKE_PRICE,SEM_OPTION_TYPE,SEM_TICK_SIZE,
            # SEM_EXPIRY_FLAG,SEM_EXCH_INSTRUMENT_TYPE,SEM_SERIES,SM_SYMBOL_NAME
            
            # We only need specific columns to save memory
            usecols = [
                'SEM_EXM_EXCH_ID', 'SEM_SEGMENT', 'SEM_SMST_SECURITY_ID', 'SEM_INSTRUMENT_NAME',
                'SEM_EXPIRY_DATE', 'SEM_STRIKE_PRICE', 'SEM_OPTION_TYPE', 'SEM_LOT_UNITS',
                'SM_SYMBOL_NAME', 'SEM_TRADING_SYMBOL'
            ]
            
            chunk_list = []
            chunk_size = 50000
            
            # Read in chunks to filter efficiently
            for chunk in pd.read_csv(INSTRUMENTS_FILE, usecols=usecols, chunksize=chunk_size):
                # Filter for NSE Derivatives (NSE FO)
                # SEM_EXM_EXCH_ID == 'NSE' and SEM_SEGMENT == 'D'
                filtered = chunk[
                    (chunk['SEM_EXM_EXCH_ID'] == 'NSE') & 
                    (chunk['SEM_SEGMENT'] == 'D')
                ]
                if not filtered.empty:
                    chunk_list.append(filtered)
            
            if not chunk_list:
                return pd.DataFrame(), pd.DataFrame()
                
            df = pd.concat(chunk_list, ignore_index=True)
            
            # Rename columns to match app logic
            df.rename(columns={
                'SEM_SMST_SECURITY_ID': 'instrument_key', # We'll use ID as key
                'SM_SYMBOL_NAME': 'name',
                'SEM_EXPIRY_DATE': 'expiry_date',
                'SEM_STRIKE_PRICE': 'strike_price',
                'SEM_LOT_UNITS': 'lot_size',
                'SEM_TRADING_SYMBOL': 'trading_symbol'
            }, inplace=True)

            # Fix for missing name (SM_SYMBOL_NAME) in Dhan CSV
            # Many rows have empty SM_SYMBOL_NAME, so we derive it from trading_symbol
            # Format: SYMBOL-Expiry-Strike-Type or SYMBOL-Expiry-FUT
            if 'name' in df.columns:
                 # Fill NaN or empty strings
                 mask = df['name'].isna() | (df['name'] == '')
                 if mask.any():
                     df.loc[mask, 'name'] = df.loc[mask, 'trading_symbol'].apply(lambda x: str(x).split('-')[0] if pd.notnull(x) else None)
            
            # Determine instrument_type (FUT/CE/PE)
            def get_instr_type(row):
                instr_name = str(row['SEM_INSTRUMENT_NAME']).upper()
                if 'FUT' in instr_name:
                    return 'FUT'
                elif 'OPT' in instr_name:
                    return row['SEM_OPTION_TYPE'] # CE or PE
                return None

            df['instrument_type'] = df.apply(get_instr_type, axis=1)
            
            # Filter out rows where instrument_type is None
            df = df[df['instrument_type'].notna()]
            
            # Convert expiry_date to datetime
            df['expiry_date'] = pd.to_datetime(df['expiry_date'])
            
            # Ensure instrument_key is string
            df['instrument_key'] = df['instrument_key'].astype(str)
            
            # Save to fast cache for next run
            df.to_pickle(CACHE_FILE)
            
        except Exception as e:
            st.error(f"Error loading instruments: {e}")
            return pd.DataFrame(), pd.DataFrame()

    # --- Process DataFrames ---
    
    # 1. Options DF (CE/PE)
    options_df = df[df['instrument_type'].isin(['CE', 'PE'])].copy()
    
    # 2. Futures DF (Current Month FUT)
    df_fut = df[df['instrument_type'] == 'FUT'].copy()
    
    # Filter for Near Month Expiry (Nearest valid expiry >= Today)
    if not df_fut.empty:
        # Use IST date for comparison
        current_date = get_ist_now().date()
        
        # Filter futures that haven't expired yet
        active_futures = df_fut[df_fut['expiry_date'].dt.date >= current_date]
        
        if not active_futures.empty:
            # Find the nearest expiry date across ALL active futures
            nearest_expiry = active_futures['expiry_date'].min()
            
            # Filter only futures matching this nearest expiry
            df_fut = active_futures[active_futures['expiry_date'] == nearest_expiry]
        else:
            df_fut = pd.DataFrame()

    futures_df = df_fut
    
    return futures_df, options_df

# --- Main App Logic ---

# Initialize data with spinner
with st.spinner("Initializing Application and Loading Data..."):
    futures_df, options_df = load_data()

if futures_df.empty or options_df.empty:
    st.error("Failed to load instruments data. Please check your internet connection and restart.")
    st.stop()

# --- API Functions ---
def get_ohlc(instrument_keys, token, client_id):
    url = "https://api.dhan.co/v2/marketfeed/ohlc"
    
    # Ensure keys are integers if possible, Dhan expects ints in list
    try:
        keys_list = [int(k) for k in instrument_keys]
    except:
        keys_list = instrument_keys
        
    payload = {
        "NSE_FNO": keys_list
    }
    
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'access-token': token,
        'client-id': client_id
    }
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=10)
            
            if response.status_code == 429:
                # Rate limit exceeded, wait and retry
                time.sleep(1 + attempt)
                continue
                
            response.raise_for_status()
            data = response.json()
            if data.get('status') == 'success':
                # Return the nested dictionary for NSE_FNO
                # Format: {'49081': {'last_price': ..., 'ohlc': {...}}}
                return data.get('data', {}).get('NSE_FNO', {})
            else:
                break # Non-retriable error
                
        except Exception as e:
            # st.error(f"Error fetching OHLC: {e}") 
            pass
        
        # If we got here (exception or non-200 that isn't 429), break or retry?
        # Usually exception means network error, so maybe retry
        time.sleep(0.5)
        
    return {}

def get_ltp(instrument_keys, token, client_id):
    url = "https://api.dhan.co/v2/marketfeed/quote"
    
    # Ensure keys are valid integers, filter out any bad ones
    keys_list = []
    for k in instrument_keys:
        try:
            keys_list.append(int(k))
        except:
            pass
            
    if not keys_list:
        return {}

    payload = {
        "NSE_FNO": keys_list
    }
    
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'access-token': token,
        'client-id': client_id
    }
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=10)
            
            if response.status_code == 429:
                # Rate Limit
                time.sleep(1 + attempt) # Backoff: 1s, 2s, 3s
                continue
            
            if response.status_code != 200:
                 # Only show error if it's the last attempt or critical
                 if attempt == max_retries - 1:
                     st.error(f"Dhan API HTTP Error {response.status_code}: {response.text[:200]}")
                 return {}

            try:
                data = response.json()
            except:
                if attempt == max_retries - 1:
                    st.error(f"Dhan API Invalid JSON: {response.text[:200]}")
                return {}

            if data.get('status') == 'success':
                 # Format: {'49081': {'last_price': ..., 'volume': ...}}
                result = data.get('data', {}).get('NSE_FNO', {})
                return result
            else:
                 # API returned error status
                 if attempt == max_retries - 1:
                     st.error(f"Dhan API Error Response: {data}")
                 # break # Don't retry logic errors? Actually maybe some are transient
                 return {}
                 
        except Exception as e:
            if attempt == max_retries - 1:
                st.error(f"Dhan API Request Exception: {str(e)}")
            time.sleep(0.5)
            pass
            
    return {}

# --- Main Logic ---


# Determine if we should run
run_once = False
if not client_view:
    run_once = st.button("🔄 Refresh Data", type="primary")
    
should_run = run_once or auto_refresh

# Define column name globally based on selection
price_key = 'open' if atm_mode == "Fixed (Open Price)" else 'close'
future_col_name = "Future Open" if atm_mode == "Fixed (Open Price)" else "Future LTP"

if should_run:
    # --- Time Restriction Check (09:00 AM - 03:40 PM IST) ---
    ist_now = get_ist_now()
    market_start = ist_now.replace(hour=9, minute=0, second=0, microsecond=0)
    market_end = ist_now.replace(hour=15, minute=40, second=0, microsecond=0)
    
    # Check if current time is within trading hours
    is_market_closed = not (market_start <= ist_now <= market_end)
    
    if is_market_closed:
        if not client_view:
            st.warning(f"⚠️ Market Closed ({ist_now.strftime('%H:%M:%S')} IST). Auto-refresh is disabled. Showing final data.")
        # Proceed to fetch data once so the user can see the last state.
    
    if auto_refresh:
        if not client_view:
            st.caption(f"Auto-refreshing every {refresh_interval} seconds...")
        
    # --- Silent Update Logic ---
    # We want to avoid 'shaking' which is caused by the spinner and progress bars appearing/disappearing.
    # If client_view is ON, we suppress the spinner and progress bars.
    
    if client_view:
        # No spinner, no progress bar
        # Just run the logic directly. The user won't see a loading state, but the table will just update.
        # This mimics the "silent update" behavior.
        futures_df_sorted = futures_df.sort_values('expiry_date')
        unique_futures = futures_df_sorted.drop_duplicates(subset=['name'], keep='first')
        
        all_results = []
        
        # Batch processing for Futures OHLC
        chunk_size = 1000 # Dhan API supports up to 1000 keys
        future_records = unique_futures.to_dict('records')
        total_records = len(future_records)
        
        # No visible progress bar for client view
        progress_bar = None
        status_text = None
        fetch_errors = []
        
        # Helper to calculate percentage change
        def calc_pct_change(ltp, cp):
            if ltp is not None and cp and cp > 0:
                return ((ltp - cp) / cp) * 100
            return 0.0

        # We need to map Future Prices first
        future_prices = {} # {symbol_name: price}
        
        # Prepare chunks
        chunks = [future_records[i:i+chunk_size] for i in range(0, total_records, chunk_size)]
        total_chunks = len(chunks)
        
        # Function to process a single chunk of futures
        def fetch_futures_chunk(chunk):
            keys = [r['instrument_key'] for r in chunk]
            ohlc_data = get_ohlc(keys, access_token, client_id)
            results = {}
            if ohlc_data:
                for record in chunk:
                    key = record['instrument_key']
                    # Dhan returns data by key (which is security ID string)
                    data = ohlc_data.get(str(key))
                    if data and 'ohlc' in data:
                        results[record['name']] = data['ohlc'].get(price_key, 0.0)
            return results

        # Parallel Execution for Futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_chunk = {executor.submit(fetch_futures_chunk, chunk): i for i, chunk in enumerate(chunks)}
            
            for future in concurrent.futures.as_completed(future_to_chunk):
                try:
                    chunk_results = future.result()
                    future_prices.update(chunk_results)
                except Exception as e:
                    fetch_errors.append(f"Futures Fetch Error: {str(e)}")

        # Prepare Option Keys to Fetch
        option_keys_to_fetch = []
        symbol_atm_map = {} # {symbol: {atm_strike, ce_key, pe_key}}
        
        relevant_options_df = options_df[
            (options_df['instrument_type'].isin(['CE', 'PE'])) &
            (options_df['name'].isin(future_prices.keys()))
        ]
        
        if not relevant_options_df.empty:
             relevant_options_df['expiry_date_obj'] = relevant_options_df['expiry_date'].dt.date
        
        options_grouped = relevant_options_df.groupby('name')
        
        for i, (symbol, ref_price) in enumerate(future_prices.items()):
            if ref_price <= 0: continue
            
            if symbol not in options_grouped.groups:
                continue
                
            opts_group = options_grouped.get_group(symbol)
            
            f_rec = unique_futures[unique_futures['name'] == symbol].iloc[0]
            f_expiry = f_rec['expiry_date'].date()
            
            # Extract short symbol for display
            short_symbol = f_rec.get('name') or symbol
            
            opts = opts_group[opts_group['expiry_date_obj'] == f_expiry]
            
            if opts.empty: continue
            
            unique_strikes = sorted(opts['strike_price'].unique())
            if not unique_strikes: continue
            
            atm_strike = min(unique_strikes, key=lambda x: abs(x - ref_price))
            
            ce_row = opts[(opts['strike_price'] == atm_strike) & (opts['instrument_type'] == 'CE')]
            pe_row = opts[(opts['strike_price'] == atm_strike) & (opts['instrument_type'] == 'PE')]
            
            ce_key = ce_row.iloc[0]['instrument_key'] if not ce_row.empty else None
            pe_key = pe_row.iloc[0]['instrument_key'] if not pe_row.empty else None
            
            ce_lot = ce_row.iloc[0]['lot_size'] if not ce_row.empty else 0
            pe_lot = pe_row.iloc[0]['lot_size'] if not pe_row.empty else 0
            
            symbol_atm_map[symbol] = {
                'ref_price': ref_price,
                'atm_strike': atm_strike,
                'ce_key': ce_key,
                'pe_key': pe_key,
                'ce_lot': ce_lot,
                'pe_lot': pe_lot,
                'display_symbol': short_symbol
            }
            
            if ce_key: option_keys_to_fetch.append(ce_key)
            if pe_key: option_keys_to_fetch.append(pe_key)
            
        # Batch Fetch Options Data
        options_data_map = {}
        total_opt_keys = len(option_keys_to_fetch)
        
        def fetch_options_chunk(chunk_keys):
            return get_ltp(chunk_keys, access_token, client_id)

        if total_opt_keys > 0:
            opt_chunks = [option_keys_to_fetch[i:i+chunk_size] for i in range(0, total_opt_keys, chunk_size)]
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                future_to_chunk = {executor.submit(fetch_options_chunk, chunk): i for i, chunk in enumerate(opt_chunks)}
                
                for future in concurrent.futures.as_completed(future_to_chunk):
                    try:
                        ltp_data = future.result()
                        if ltp_data:
                            options_data_map.update(ltp_data)
                    except Exception as e:
                        fetch_errors.append(f"Options Fetch Error: {str(e)}")
        
        if fetch_errors:
            st.error(f"Errors occurred during data fetch ({len(fetch_errors)}). Data might be incomplete.")
            with st.expander("View Errors"):
                for err in fetch_errors:
                    st.write(err)
        
    else:
        # Standard View with Spinner and Progress
        with st.spinner("Fetching and Calculating Data..."):
            futures_df_sorted = futures_df.sort_values('expiry_date')
            unique_futures = futures_df_sorted.drop_duplicates(subset=['name'], keep='first')
            
            all_results = []
            
            # Batch processing for Futures OHLC
            chunk_size = 1000  # Dhan API supports up to 1000 keys per request
            future_records = unique_futures.to_dict('records')
            total_records = len(future_records)
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Helper to calculate percentage change
            def calc_pct_change(ltp, cp):
                if ltp is not None and cp and cp > 0:
                    return ((ltp - cp) / cp) * 100
                return 0.0

            # We need to map Future Prices first
            future_prices = {} # {symbol_name: price}
            
            chunks = [future_records[i:i+chunk_size] for i in range(0, total_records, chunk_size)]
            total_chunks = len(chunks)
            
            def fetch_futures_chunk(chunk):
                keys = [r['instrument_key'] for r in chunk]
                ohlc_data = get_ohlc(keys, access_token, client_id)
                results = {}
                if ohlc_data:
                    for record in chunk:
                        key = record['instrument_key']
                        data = ohlc_data.get(str(key))
                        if data and 'ohlc' in data:
                            results[record['name']] = data['ohlc'].get(price_key, 0.0)
                return results

            if status_text:
                status_text.text(f"Fetching Futures Data... (0/{total_records})")
            
            # SEQUENTIAL FETCH to respect Rate Limits
            completed_count = 0
            for chunk in chunks:
                try:
                    chunk_results = fetch_futures_chunk(chunk)
                    future_prices.update(chunk_results)
                except Exception as e:
                    print(f"Error fetching futures chunk: {e}")
                    pass
                
                completed_count += 1
                progress = min((completed_count / total_chunks) * 0.5, 0.5)
                if progress_bar:
                    progress_bar.progress(progress)
                if status_text:
                    status_text.text(f"Fetching Futures Data... ({min(completed_count * chunk_size, total_records)}/{total_records})")
                time.sleep(0.1) # Small delay between chunks

            # Prepare Option Keys to Fetch
            option_keys_to_fetch = []
            symbol_atm_map = {} # {symbol: {atm_strike, ce_key, pe_key}}
            
            relevant_options_df = options_df[
                (options_df['instrument_type'].isin(['CE', 'PE'])) &
                (options_df['name'].isin(future_prices.keys()))
            ]
            
            if not relevant_options_df.empty:
                 relevant_options_df['expiry_date_obj'] = relevant_options_df['expiry_date'].dt.date
            
            options_grouped = relevant_options_df.groupby('name')
            
            for i, (symbol, ref_price) in enumerate(future_prices.items()):
                if ref_price <= 0: continue
                
                if symbol not in options_grouped.groups:
                    continue
                    
                opts_group = options_grouped.get_group(symbol)
                
                f_rec = unique_futures[unique_futures['name'] == symbol].iloc[0]
                f_expiry = f_rec['expiry_date'].date()
                
                # Extract short symbol for display
                short_symbol = f_rec.get('name') or symbol
                
                opts = opts_group[opts_group['expiry_date_obj'] == f_expiry]
                
                if opts.empty: continue
                
                unique_strikes = sorted(opts['strike_price'].unique())
                if not unique_strikes: continue
                
                atm_strike = min(unique_strikes, key=lambda x: abs(x - ref_price))
                
                ce_row = opts[(opts['strike_price'] == atm_strike) & (opts['instrument_type'] == 'CE')]
                pe_row = opts[(opts['strike_price'] == atm_strike) & (opts['instrument_type'] == 'PE')]
                
                ce_key = ce_row.iloc[0]['instrument_key'] if not ce_row.empty else None
                pe_key = pe_row.iloc[0]['instrument_key'] if not pe_row.empty else None
                
                ce_lot = ce_row.iloc[0]['lot_size'] if not ce_row.empty else 0
                pe_lot = pe_row.iloc[0]['lot_size'] if not pe_row.empty else 0
                
                symbol_atm_map[symbol] = {
                    'ref_price': ref_price,
                    'atm_strike': atm_strike,
                    'ce_key': ce_key,
                    'pe_key': pe_key,
                    'ce_lot': ce_lot,
                    'pe_lot': pe_lot,
                    'display_symbol': short_symbol
                }
                
                if ce_key: option_keys_to_fetch.append(ce_key)
                if pe_key: option_keys_to_fetch.append(pe_key)
                
            # Batch Fetch Options Data
            options_data_map = {}
            total_opt_keys = len(option_keys_to_fetch)
            
            def fetch_options_chunk(chunk_keys):
                return get_ltp(chunk_keys, access_token, client_id)

            if total_opt_keys > 0:
                opt_chunks = [option_keys_to_fetch[i:i+chunk_size] for i in range(0, total_opt_keys, chunk_size)]
                total_opt_chunks = len(opt_chunks)
                
                if status_text:
                    status_text.text(f"Fetching Options Data... (0/{total_opt_keys})")
                
                # SEQUENTIAL FETCH to respect Rate Limits
                completed_count = 0
                for chunk in opt_chunks:
                    try:
                        ltp_data = fetch_options_chunk(chunk)
                        if ltp_data:
                            options_data_map.update(ltp_data)
                    except Exception as e:
                        print(f"Error fetching options chunk: {e}")
                        pass
                    
                    completed_count += 1
                    progress = 0.5 + min((completed_count / total_opt_chunks) * 0.5, 0.5)
                    if progress_bar:
                        progress_bar.progress(progress)
                    if status_text:
                        status_text.text(f"Fetching Options Data... ({min(completed_count * chunk_size, total_opt_keys)}/{total_opt_keys})")
                    time.sleep(0.1) # Small delay between chunks

            # Cleanup Progress
            if progress_bar:
                progress_bar.progress(1.0)
            if status_text:
                status_text.text("Finalizing Data...")
            time.sleep(0.5)
            if progress_bar:
                progress_bar.empty()
            if status_text:
                status_text.empty()
    
    # Common Logic to Process Results (Outside the if/else)
    # This part was common in both branches, so we can keep it here.
    # However, since I duplicated the fetching logic (which is slightly different for progress bars),
    # I need to ensure variables like options_data_map, symbol_atm_map, total_opt_keys are available.
    
    if total_opt_keys > 0 and not options_data_map:
            if not client_view:
                st.error("Failed to fetch Options Data (LTP). Please check your Access Token or Internet Connection.")

    # Optimize Lookup for Options
    # Create a fast lookup map for options_data_map
    fast_options_map = options_data_map.copy()
        
    # DEBUG: Show sample of map if empty
    if not fast_options_map and total_opt_keys > 0:
            st.warning(f"Options Data Map is empty! Sent {total_opt_keys} keys.")

    # Construct Final DataFrame
    final_rows = []
    for symbol, info in symbol_atm_map.items():
        row = {
            "Stock Name": info.get('display_symbol', symbol),
            future_col_name: info['ref_price'],
            "ATM Strike": info['atm_strike']
        }
        
        # Helper to get data with fallback
        def get_opt_data(key):
            if not key: return None
            # Try exact match (key is security_id string)
            return fast_options_map.get(str(key))
            
        # Helper to get previous close
        def get_prev_close(d):
            # Dhan API usually returns 'close' or 'previous_close'
            return d.get('close') or d.get('previous_close') or d.get('pc') or d.get('cp') or 0.0

        # Helper to calculate percentage change using net_change if available
        def get_pct_change(d, ltp):
            # 1. Try net_change field
            net_change = d.get('net_change')
            if net_change is not None:
                # If net_change is available, we can calculate prev_close
                prev_close = ltp - net_change
                if prev_close > 0:
                    return (net_change / prev_close) * 100
            
            # 2. Fallback: Try to get previous close explicitly
            prev_close = get_prev_close(d)
            if prev_close > 0:
                return ((ltp - prev_close) / prev_close) * 100
            
            return 0.0

        # CE Data
        ce_key = info['ce_key']
        ce_ltp = 0
        ce_pct = 0
        ce_vol = 0
        ce_ctr = 0
        
        if ce_key:
            # Optimized lookup
            data = get_opt_data(ce_key)
            if data:
                ce_ltp = data.get('last_price', 0)
                ce_vol = data.get('volume', 0)
                ce_pct = get_pct_change(data, ce_ltp)
                # Calculate Contracts
                lot_size = info.get('ce_lot', 0)
                if lot_size > 0 and ce_vol > 0:
                        ce_ctr = ce_vol / lot_size
        
        row["CE LTP"] = ce_ltp
        row["CE Change %"] = round(ce_pct, 2)
        row["CE Volume"] = ce_vol
        row["CE Contracts"] = int(ce_ctr)
        
        # PE Data
        pe_key = info['pe_key']
        pe_ltp = 0
        pe_pct = 0
        pe_vol = 0
        pe_ctr = 0
        
        if pe_key:
            # Optimized lookup
            data = get_opt_data(pe_key)
            if data:
                pe_ltp = data.get('last_price', 0)
                pe_vol = data.get('volume', 0)
                pe_pct = get_pct_change(data, pe_ltp)
                # Calculate Contracts
                lot_size = info.get('pe_lot', 0)
                if lot_size > 0 and pe_vol > 0:
                        pe_ctr = pe_vol / lot_size
                
        row["PE LTP"] = pe_ltp
        row["PE Change %"] = round(pe_pct, 2)
        row["PE Volume"] = pe_vol
        row["PE Contracts"] = int(pe_ctr)
        
        final_rows.append(row)
        
    df_results = pd.DataFrame(final_rows)

    # Ensure ATM Strike is numeric and rounded for clean display
    if not df_results.empty and "ATM Strike" in df_results.columns:
        df_results["ATM Strike"] = df_results["ATM Strike"].astype(float).round(2)
    
    # Fixed Stock Name to prevent table shaking
    stock_col_name = "Stock Name"
    
    # Save snapshot to session state
    st.session_state['data_snapshot'] = {
        'df': df_results,
        'stock_col_name': stock_col_name,
        'future_col_name': future_col_name
    }

# --- Display Logic (from Session State) ---
if 'data_snapshot' in st.session_state and st.session_state['data_snapshot']:
    snapshot = st.session_state['data_snapshot']
    df_results = snapshot['df']
    stock_col_name = snapshot['stock_col_name']
    
    # Use the stored future_col_name if available, otherwise fallback to current global
    snap_future_col = snapshot.get('future_col_name', future_col_name)
    
    # Check if the column actually exists (in case of stale state mismatch)
    if snap_future_col not in df_results.columns:
        # Try to find a column starting with "Future"
        fut_cols = [c for c in df_results.columns if str(c).startswith("Future")]
        if fut_cols:
            snap_future_col = fut_cols[0]
    
    # Split into CE and PE DataFrames
    ce_cols = [c for c in [stock_col_name, snap_future_col, "ATM Strike", "CE LTP", "CE Change %", "CE Volume", "CE Contracts"] if c in df_results.columns]
    pe_cols = [c for c in [stock_col_name, snap_future_col, "ATM Strike", "PE LTP", "PE Change %", "PE Volume", "PE Contracts"] if c in df_results.columns]
    
    df_ce = df_results[ce_cols].copy()
    df_pe = df_results[pe_cols].copy()
    
    # Auto-Sort by Change % (Descending)
    if not df_ce.empty and "CE Change %" in df_ce.columns:
        df_ce = df_ce.sort_values(by="CE Change %", ascending=False)
    if not df_pe.empty and "PE Change %" in df_pe.columns:
        df_pe = df_pe.sort_values(by="PE Change %", ascending=False)

    # Rename columns for compact display
    
    # Create combined Symbol column
    if not df_ce.empty:
        # Format strike to remove decimals if integer
        df_ce['TempStrike'] = df_ce['ATM Strike'].apply(lambda x: f"{int(x)}" if x == int(x) else f"{x}")
        # Assuming stock_col_name is 'Stock Name' in df_ce before rename
        # But wait, rename happens AFTER this block in my previous code?
        # No, I am editing the block where rename happens.
        # df_ce currently has columns from ce_cols which includes stock_col_name ("Stock Name") and "ATM Strike"
        
        # Use underlying symbol if available, otherwise stock name
        # Since we don't have underlying symbol column in df_results explicitly separate from Stock Name (which is Name),
        # We will just use the Stock Name. If it's long, we might need to truncate, but user asked to combine.
        # "JUBLFOOD 525 CE"
        
        df_ce['DisplaySymbol'] = df_ce[stock_col_name].astype(str) + " " + df_ce['TempStrike'] + " CE"
        
    if not df_pe.empty:
        df_pe['TempStrike'] = df_pe['ATM Strike'].apply(lambda x: f"{int(x)}" if x == int(x) else f"{x}")
        df_pe['DisplaySymbol'] = df_pe[stock_col_name].astype(str) + " " + df_pe['TempStrike'] + " PE"

    rename_map_ce = {
        "DisplaySymbol": "Symbol",
        snap_future_col: "Open",
        "CE LTP": "LTP",
        "CE Change %": "Chg%",
        "CE Volume": "Vol",
        "CE Contracts": "Ctr"
    }
    rename_map_pe = {
        "DisplaySymbol": "Symbol",
        snap_future_col: "Open",
        "PE LTP": "LTP",
        "PE Change %": "Chg%",
        "PE Volume": "Vol",
        "PE Contracts": "Ctr"
    }
    
    # Select only the columns we want to show
    # We drop Stock Name and ATM Strike
    
    if not df_ce.empty:
        df_ce = df_ce.rename(columns=rename_map_ce)
        # Reorder to put Symbol first
        cols = ["Symbol", "Open", "LTP", "Chg%", "Vol", "Ctr"]
        # Filter strictly
        df_ce = df_ce[[c for c in cols if c in df_ce.columns]]
        
    if not df_pe.empty:
        df_pe = df_pe.rename(columns=rename_map_pe)
        cols = ["Symbol", "Open", "LTP", "Chg%", "Vol", "Ctr"]
        df_pe = df_pe[[c for c in cols if c in df_pe.columns]]

    # Display Side-by-Side
    
    # Display Last Updated Time (outside table to prevent shaking)
    time_str = get_ist_now().strftime("%H:%M:%S")
    st.markdown(f"<h5 style='text-align: center; color: #333; margin-bottom: 5px;'>Last Updated: {time_str}</h5>", unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    table_height = (max(len(df_ce), len(df_pe)) + 1) * 35 + 3

    with col1:
        if not client_view:
             st.subheader("CE Data (Sorted by Change %)")
        
        # Apply Styling
        styler_ce = df_ce.style.set_properties(**{'background-color': '#e6ffe6', 'color': 'black'})
        # Apply white background to Symbol
        if "Symbol" in df_ce.columns:
            styler_ce = styler_ce.set_properties(subset=["Symbol"], **{'background-color': 'white'})
        
        st.dataframe(
            styler_ce,
            column_config={
                "Symbol": st.column_config.TextColumn(label="Symbol"),
                "Open": st.column_config.NumberColumn(format="%.2f"),
                "LTP": st.column_config.NumberColumn(format="%.2f"),
                "Chg%": st.column_config.NumberColumn(format="%.2f%%"),
                "Ctr": st.column_config.NumberColumn(format="%d"),
            },
            use_container_width=True,
            hide_index=True,
            height=table_height
        )

    with col2:
        if not client_view:
             st.subheader("PE Data (Sorted by Change %)")
             
        # Apply Styling
        styler_pe = df_pe.style.set_properties(**{'background-color': '#ffe6e6', 'color': 'black'})
        # Apply white background to Symbol
        if "Symbol" in df_pe.columns:
            styler_pe = styler_pe.set_properties(subset=["Symbol"], **{'background-color': 'white'})
        
        st.dataframe(
            styler_pe,
            column_config={
                "Symbol": st.column_config.TextColumn(label="Symbol"),
                "Open": st.column_config.NumberColumn(format="%.2f"),
                "LTP": st.column_config.NumberColumn(format="%.2f"),
                "Chg%": st.column_config.NumberColumn(format="%.2f%%"),
                "Ctr": st.column_config.NumberColumn(format="%d"),
            },
            use_container_width=True,
            hide_index=True,
            height=table_height
        )

# Handle Auto-Refresh Loop
if should_run and auto_refresh:
    # is_market_closed is defined inside the if should_run block above
    if not is_market_closed:
        time.sleep(refresh_interval)
        st.rerun()

if not should_run and 'data_snapshot' not in st.session_state:
    if not client_view:
        st.info("Click 'Load All Stocks Data' or enable 'Auto-Refresh' in the sidebar to start.")
