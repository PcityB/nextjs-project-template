"""Download (if needed), format, and import data to ClickHouse."""
from __future__ import annotations

import sys
from collections import namedtuple
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from multiprocessing import Process, Queue
from pathlib import Path
from typing import Any, Iterable, Optional

from clickhouse_driver import Client
from rich import print
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
import rx
from rx import operators as ops
from reactivex.scheduler import ThreadPoolScheduler

from histdatacom import config
from histdatacom.api import Api
from histdatacom.concurrency import ProcessPool, get_pool_cpu_count


class ClickHouse:
    """Download (if needed), format, and import data to ClickHouse."""

    def import_data(self) -> None:
        """Initialize a pool of ClickHouse writers and process pool with a progress bar."""
        writer = ClickHouseWriter(config.ARGS, config.CLICKHOUSE_BATCH_QUEUE)
        writer.start()

        pool = ProcessPool(
            self._import_file,
            config.ARGS,
            "Adding",
            "CSVs to ClickHouse queue...",
            get_pool_cpu_count(config.ARGS["cpu_utilization"]),
            join=False,
            dump=False,
        )

        pool(
            config.CURRENT_QUEUE,
            config.NEXT_QUEUE,
            config.CLICKHOUSE_BATCH_QUEUE,
        )

        with Progress(
            TextColumn(text_format="[cyan]...finishing upload to ClickHouse"),
            SpinnerColumn(),
            SpinnerColumn(),
            SpinnerColumn(),
            TimeElapsedColumn(),
        ) as progress:
            task_id = progress.add_task("waiting", total=0)

            config.CURRENT_QUEUE.join()
            config.CLICKHOUSE_BATCH_QUEUE.put(None)
            config.CLICKHOUSE_BATCH_QUEUE.join()
            progress.advance(task_id, 0.75)

        print("[cyan] done.")
        config.NEXT_QUEUE.dump_to_queue(config.CURRENT_QUEUE)

    def _import_file(
        self,
        record: Any,
        args: dict,
        records_current: Any,
        records_next: Any,
        batch_queue: Queue,
    ) -> None:
        """Import ASCII data to ClickHouse, both for csv and jay.

        Args:
            record: A record from the work queue
            args: config.ARGS
            records_current: config.CURRENT_QUEUE
            records_next: config.NEXT_QUEUE
            batch_queue: config.CLICKHOUSE_BATCH_QUEUE

        Raises:
            Exception: on unknown exception.
        """
        try:
            if (
                record.status != "CLICKHOUSE_UPLOAD"
                and str.lower(record.data_format) == "ascii"
            ):
                jay_path = Path(record.data_dir, ".data")
                if jay_path.exists():
                    self._import_jay(
                        record,
                        args,
                        records_current,
                        records_next,
                        batch_queue,
                    )
                elif "CSV" in record.status:
                    Api.test_for_jay_or_create(record, args)
                    self._import_jay(
                        record,
                        args,
                        records_current,
                        records_next,
                        batch_queue,
                    )

            record.status = "CLICKHOUSE_UPLOAD"
            record.write_memento_file(base_dir=args["default_download_dir"])

            if args.get("delete_after_clickhouse", False):
                Path(record.data_dir, record.zip_filename).unlink()
                Path(record.data_dir, record.jay_filename).unlink()
            records_next.put(record)
        except Exception as err:
            print(f"Unexpected error: {err}")
            record.delete_momento_file()
            raise
        finally:
            records_current.task_done()

    def _import_jay(
        self,
        record: Any,
        args: dict,
        records_current: Any,
        records_next: Any,
        batch_queue: Queue,
    ) -> None:
        """Import a jay file with a ReactiveX pub/sub queue.

        Args:
            record: A record from the work queue
            args: config.ARGS
            records_current: config.CURRENT_QUEUE
            records_next: config.NEXT_QUEUE
            batch_queue: config.CLICKHOUSE_BATCH_QUEUE
        """
        jay = Api.import_jay_data(record.data_dir + record.jay_filename)

        with ProcessPoolExecutor(
            max_workers=1,
            initializer=_init_counters,
            initargs=(batch_queue, args),
        ) as executor:
            rx_data_queue = rx.from_iterable(jay.to_tuples()).pipe(
                ops.buffer_with_count(args["batch_size"]),
                ops.flat_map(
                    lambda rows: executor.submit(
                        self._parse_jay_rows, rows, record
                    )
                ),
            )

            rx_data_queue.subscribe(
                on_next=lambda x: None,
                on_error=lambda err: print(f"Unexpected error: {err}"),
            )

    def _parse_jay_rows(self, rows: Iterable, record: Any) -> None:
        """Create a list by mapping row-by-row from datatable Frame.

        Args:
            rows: datatable.Frame rows
            record: A record from the work queue
        """
        map_func = partial(self._parse_jay_row, record=record)
        parsed_rows = list(map(map_func, rows))
        CLICKHOUSE_BATCH_QUEUE.put(parsed_rows)

    def _parse_jay_row(self, row: tuple, record: Any) -> dict:
        """Convert a jay row into a dictionary for ClickHouse insertion.

        Args:
            row: Row from datatable.Frame
            record: Record from the work queue

        Returns:
            dict: Data formatted for ClickHouse insertion
        """
        base_data = {
            "source": "histdata.com",
            "format": record.data_format,
            "timeframe": record.data_timeframe,
            "fxpair": record.data_fxpair,
        }

        if record.data_timeframe == "M1":
            _row = namedtuple(
                "_row",
                ["datetime", "open", "high", "low", "close", "vol"],
            )
            named_row = _row(
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                row[5],
            )
            return {
                **base_data,
                "timestamp": str(named_row.datetime),
                "openbid": named_row.open,
                "highbid": named_row.high,
                "lowbid": named_row.low,
                "closebid": named_row.close,
                "vol": named_row.vol,
                "bidquote": None,
                "askquote": None,
            }
        else:  # T timeframe
            _row = namedtuple("_row", ["datetime", "bid", "ask", "vol"])
            named_row = _row(row[0], row[1], row[2], row[3])
            return {
                **base_data,
                "timestamp": str(named_row.datetime),
                "openbid": None,
                "highbid": None,
                "lowbid": None,
                "closebid": None,
                "bidquote": named_row.bid,
                "askquote": named_row.ask,
                "vol": named_row.vol,
            }


def _init_counters(
    clickhouse_batch_queue_: Queue,
    args_: dict,
) -> None:
    """Initialize pool with access to these global variables.

    Args:
        clickhouse_batch_queue_ (Queue): ReactiveX queue
        args_ (dict): config.ARGS
    """
    global CLICKHOUSE_BATCH_QUEUE
    CLICKHOUSE_BATCH_QUEUE = clickhouse_batch_queue_
    global ARGS
    ARGS = args_

class ClickHouseWriter(Process):
    """Write data from the batch queue to ClickHouse."""

    def __init__(self, args: dict, batch_queue: Optional[Queue]):
        """Initialize a process for the ClickHouse client.

        Args:
            args: config.ARGS
            batch_queue: config.CLICKHOUSE_BATCH_QUEUE
        """
        Process.__init__(self)
        self.args = args
        self.batch_queue = batch_queue
        self.client = Client(
            host=args["CLICKHOUSE_HOST"],
            port=args["CLICKHOUSE_PORT"],
            user=args["CLICKHOUSE_USER"],
            password=args["CLICKHOUSE_PASSWORD"],
            database=args["CLICKHOUSE_DATABASE"],
        )
        self.scheduler = ThreadPoolScheduler(
            max_workers=get_pool_cpu_count(args["cpu_utilization"])
        )


    def run(self) -> None:
        """Process batches from config.CLICKHOUSE_BATCH_QUEUE."""
        try:
            while True:
                try:
                    batch = self.batch_queue.get()
                except EOFError:
                    break

                if batch is None:
                    self.terminate()
                    self.batch_queue.task_done()
                    break

                try:
                    self._write_batch(batch)
                except Exception as err:
                    print(f"Error writing batch to ClickHouse: {err}")
                finally:
                    self.batch_queue.task_done()
        except KeyboardInterrupt:
            self.terminate()

    def _write_batch(self, batch: list[dict]) -> None:
        """Write a batch of records to ClickHouse.

        Args:
            batch: List of records to insert
        """
        try:
            self.client.execute(
                f"""
                INSERT INTO {self.args['CLICKHOUSE_TABLE']} (
                    timestamp, source, format, timeframe, fxpair,
                    openbid, highbid, lowbid, closebid,
                    bidquote, askquote, vol
                ) VALUES
                """,
                batch,
                types_check=True,
            )
        except Exception as err:
            print(f"Error during batch insert: {err}")
            raise

    def terminate(self) -> None:
        """Terminate the ClickHouse subprocess."""
        self.client.disconnect()
        self.close()
