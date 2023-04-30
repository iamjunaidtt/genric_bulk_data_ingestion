from concurrent import futures
import csv
import queue
from dbutils import DBHandler
from thelogger import getmylogger

logger = getmylogger(__name__)


class BulkIngestionHandler:

    def __init__(self,multi_insert_limit,workers):
        self.MULTIROW_INSERT_LIMIT = multi_insert_limit
        self.WORKERS = workers
        self.dbhandler = DBHandler()

    # Read CSV file
    def read_csv(self,csv_path):
        with open(csv_path, encoding="utf-8", newline="") as in_file:
            reader = csv.reader(in_file, delimiter=",")
            next(reader)
            for row in reader:
                yield row

    def process_row(self,row, batch, table_name, conn_params):
        batch.put(row)
        if batch.full():
            self.dbhandler.multi_row_insert(batch, table_name, conn_params)
        return batch

    def load_csv(self,csv_file, table_def, conn_params):
        # Optional, drops table if it exists before creating
        self.dbhandler.make_table(table_def, conn_params)

        batch = queue.Queue(self.MULTIROW_INSERT_LIMIT)

        with futures.ThreadPoolExecutor(max_workers=self.WORKERS) as executor:
            todo = []

            for row in self.read_csv(csv_file):
                future = executor.submit(
                    self.process_row, row, batch, table_def["name"], conn_params
                )
                todo.append(future)

            for future in futures.as_completed(todo):
                result = future.result()

        # Handle left overs
        if not result.empty():
            self.dbhandler.multi_row_insert(result, table_def["name"], conn_params)

