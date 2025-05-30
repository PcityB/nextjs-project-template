"""
Historify - Stock Historical Data Management App
HistData.com Data Fetcher Utility
"""
import os
import logging
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from multiprocessing import Queue
import rx
from rx import operators as ops
from reactivex.scheduler import ThreadPoolScheduler

from app.utils.rate_limiter import broker_rate_limiter
from app.models.clickhouse_data import ClickHouseData

try:
    from histdatacom import config, Api
    from histdatacom.concurrency import get_pool_cpu_count
    HISTDATA_AVAILABLE = True
    logging.info('HistData.com API successfully imported')
except ImportError as e:
    HISTDATA_AVAILABLE = False
    logging.error(f'HistData.com API import error: {str(e)}')
    logging.warning('HistData.com API not available. Please install histdata.com-tools.')

class HistDataFetcher:
    """Fetcher for HistData.com forex data"""
    
    def __init__(self):
        """Initialize HistData fetcher"""
        self.clickhouse = ClickHouseData()
        self.batch_queue = Queue()
        self.api = Api() if HISTDATA_AVAILABLE else None
        self.max_workers = get_pool_cpu_count('medium')  # Use medium CPU utilization
        self.scheduler = ThreadPoolScheduler(max_workers=self.max_workers)

    @broker_rate_limiter
    def fetch_historical_data(self, symbol, start_date, end_date, timeframe='T'):
        """Fetch historical forex data from HistData.com
        
        Args:
            symbol: Forex pair (e.g., 'EURUSD')
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            timeframe: Data timeframe (T for tick data, M1 for 1-minute data)
            
        Returns:
            List of forex data points
        """
        if not HISTDATA_AVAILABLE:
            logging.error("HistData.com API is not available")
            raise ValueError("HistData.com API is not available. Please install histdata.com-tools.")

        try:
            # Configure histdata settings
            config.ARGS = {
                'pair': symbol,
                'timeframe': timeframe,
                'format': 'ascii',
                'start': start_date,
                'end': end_date,
                'cpu_utilization': 'medium',
                'batch_size': 5000
            }

            # Download and process data
            with ProcessPoolExecutor(
                max_workers=1,
                initializer=self._init_counters,
                initargs=(self.batch_queue, config.ARGS),
            ) as executor:
                # Get data from histdata.com
                data = self.api.get_data(symbol, start_date, end_date, timeframe)
                
                # Process data using reactive streams
                rx.from_iterable(data).pipe(
                    ops.buffer_with_count(config.ARGS['batch_size']),
                    ops.flat_map(
                        lambda rows: executor.submit(
                            self._process_rows, rows, symbol, timeframe
                        )
                    ),
                ).subscribe(
                    on_next=lambda x: None,
                    on_error=lambda err: logging.error(f"Error processing data: {err}"),
                    scheduler=self.scheduler
                )

                # Insert processed data into ClickHouse
                while True:
                    batch = self.batch_queue.get()
                    if batch is None:
                        break
                    self.clickhouse.insert_forex_data(batch)
                    self.batch_queue.task_done()

            return True

        except Exception as e:
            logging.error(f"Error fetching data from HistData.com: {str(e)}")
            raise ValueError(f"Failed to fetch data from HistData.com: {str(e)}")

    def _init_counters(self, batch_queue: Queue, args: dict) -> None:
        """Initialize pool with access to global variables"""
        global BATCH_QUEUE, ARGS
        BATCH_QUEUE = batch_queue
        ARGS = args

    def _process_rows(self, rows, symbol, timeframe):
        """Process a batch of rows into forex data points"""
        processed = []
        for row in rows:
            processed.append({
                'symbol': symbol,
                'timestamp': row[0],  # Datetime
                'bidquote': row[1],  # Bid price
                'askquote': row[2],  # Ask price
                'format': 'ascii',
                'timeframe': timeframe
            })
        BATCH_QUEUE.put(processed)

    def close(self):
        """Clean up resources"""
        self.clickhouse.close()
