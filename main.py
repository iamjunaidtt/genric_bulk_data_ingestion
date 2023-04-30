import os
from data_ingestion import BulkIngestionHandler


if __name__ == "__main__":

    # initiate database handler object
    bulk_ingestion = BulkIngestionHandler(multi_insert_limit=1000,workers=4)

    # Define the Table Schema
    table_def = {
        "name": "organization",
        "columns": {

            "Index": "INTEGER", "Organization Id": "VARCHAR(MAX)", "Name": "VARCHAR(MAX)", "Website": "VARCHAR(MAX)",
            "Country": "VARCHAR(MAX)", "Description": "VARCHAR(MAX)", "Founded": "INTEGER", "Industry": "VARCHAR(MAX)",
            "Number of employees": "INTEGER"

        },
    }

    # Database connection parameters
    conn_params = {
        "server": "localhost",
        "database": "******",
        "user": "******",
        "password": "******",
        "port": 1433,
        "driver": "******",
    }
    # Data file path
    file_path = os.path.join(os.getcwd(),'data.csv')

    bulk_ingestion.load_csv(file_path, table_def, conn_params)





