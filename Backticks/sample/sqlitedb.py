import streamlit as st
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from openalgo import api
import time

# Initialize OpenAlgo API
client = api(api_key='df746c00c34b744940773253978b3e9d8b610e428653db230a43085c5311d33d', host='http://127.0.0.1:5000')

# List of symbols to download
symbols = ["RELIANCE", "ICICIBANK", "SBIN", "TATAMOTORS", "TATASTEEL",
           "INFY", "TCS", "MARUTI", "HDFCBANK", "AXISBANK"]

# Set up SQLite database
engine = create_engine('sqlite:///data.db')

# Streamlit UI
st.title("OpenAlgo 1-Minute Historical Data Downloader")
start_date = st.date_input("Start Date", datetime.now().date())
end_date = st.date_input("End Date", datetime.now().date())

if st.button("Download Data"):
    progress_bar = st.progress(0)
    status_text = st.empty()

    for idx, symbol in enumerate(symbols):
        try:
            status_text.text(f"Downloading {symbol} ({idx+1}/{len(symbols)})...")
            
            df = client.history(
                symbol=symbol,
                exchange="NSE",
                interval="1m",
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d')
            )
            

            if not isinstance(df, pd.DataFrame):
                st.warning(f"No data returned for {symbol}")
                continue

            df['symbol'] = symbol
            df.reset_index(inplace=True)
            df.to_sql(symbol, con=engine, if_exists='replace', index=False)
            time.sleep(0.5)  # Sleep to avoid hitting API rate limits

        except Exception as e:
            st.error(f"Error downloading {symbol}: {e}")

        progress_bar.progress((idx + 1) / len(symbols))

    status_text.text("✅ All downloads complete!")
    st.success("Data has been saved to 'data.db'")