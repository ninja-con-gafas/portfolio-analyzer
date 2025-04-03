import csv
import logging
import os
import re
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from process.BSE import get_securities
from config.global_variables import BSE_SEGMENTS_AND_INTERNAL_CODES
from datetime import datetime
from psycopg2 import connect, DatabaseError
from tempfile import gettempdir
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

"""
Create and configure an Airflow DAG to fetch and ingest securities data from Bombay Stock Exchange (BSE).

The DAG performs the following tasks:
- Drops the existing securities table.
- Fetches securities data for different segments from BSE.
- Ingests the fetched data into the database.
- Indexes the table for optimized queries.

The DAG runs on a monthly schedule, starting from the first day of the current month.
"""

def drop_existing_table(**kwargs) -> None:
     
    """
    Drop the existing securities table if it exists.

    Parameters:
        **kwargs (Dict[Any, Any]): Additional arguments passed from Airflow.

    Returns:
        None
    """

    logger.info("Dropping existing securities table if it exists")
    try:
        configuration: Dict[str, str] = get_database_configurations()
        connection = connect(**configuration)
        cursor = connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS securities;")
        connection.commit()
        logger.info("Existing table dropped successfully.")
    except DatabaseError as e:
        logger.error(f"Database error while dropping table: {e}")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def fetch_and_save(segment: str, **kwargs) -> None:

    """
    Fetch securities data and save it to a CSV file.

    Parameters:
        segment (str): The segment of securities to fetch.
        **kwargs (Dict[Any, Any]): Additional arguments passed from Airflow.

    Returns:
        None
    """

    logger.info(f"Fetching securities for segment: {segment}")
    securities = get_securities(segment=segment)
    path: str = get_securities_file_path()
    csv_file = os.path.join(path, f"{sanitize_segment(segment)}_securities.csv")
    logger.info(f"Saving securities data to {csv_file}")
    save_securities_to_csv(securities=securities, segment=segment, csv_file=csv_file)

def get_database_configurations() -> Dict[str, str]:

    """
    Retrieve the database connection configurations for the Portfolio Analyzer.

    Fetches connection details from Airflow's connection store using the 
    connection ID 'portfolioanalyzer_db'.

    Parameters:
        None

    Returns:
        Dict[str, str]: A dictionary containing the following database configurations:
            - dbname (str): The name of the database.
            - user (str): The username for authentication.
            - password (str): The password for authentication.
            - host (str): The database host address.
            - port (str): The database port number.
    """

    connection = BaseHook.get_connection("portfolioanalyzer_db")
    
    return {
        "dbname": connection.schema,
        "user": connection.login,
        "password": connection.password,
        "host": connection.host,
        "port": connection.port
        }

def get_securities_file_path() -> str:

    """
    Returns directory to store securities data as a CSV file.

    Returns:
        str: The path to the directory.
    """

    temporary_directory = os.path.join(gettempdir(), "portfolio-analyzer")
    os.makedirs(temporary_directory, exist_ok=True)
    return temporary_directory

def index_securities() -> None:

    """
    Create indexes on the `securities` table for efficient querying.

    Indexes are created on:
        - scrip_code
        - isin_number
        - scrip_id

    Parameters:
        None

    Returns:
        None
    """
    
    logger.info("Connecting to the database to create indexes")
    
    try:
        configuration: Dict[str, str] = get_database_configurations()
        connection = connect(**configuration)
        cursor = connection.cursor()
        logger.info("Database connection established.")

        # Create indexes on relevant columns
        logger.info("Creating indexes on securities table")
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_securities_scrip_code ON securities(scrip_code);
            CREATE INDEX IF NOT EXISTS idx_securities_isin_number ON securities(isin_number);
            CREATE INDEX IF NOT EXISTS idx_securities_scrip_id ON securities(scrip_id);
        """)
        connection.commit()
        logger.info("Indexes created successfully.")

    except DatabaseError as e:
        logger.error(f"Database error: {e}")
        if connection:
            connection.rollback()
            logger.info("Transaction rolled back due to error.")
    
    finally:
        if cursor:
            cursor.close()
            logger.info("Database cursor closed.")
        if connection:
            connection.close()
            logger.info("Database connection closed.")

def ingest_securities(csv_file: str, **kwargs) -> None:

    """
    Upload securities data from a CSV file and store it in the database.

    Parameters:
        csv_file (str): The path to the CSV file containing security data.
        **kwargs (Dict[Any, Any]): Additional arguments passed from Airflow.

    Returns:
        None
    """
    
    logger.info(f"Using CSV file: {csv_file}")
    
    if not os.path.exists(csv_file):
        logger.error(f"CSV file not found: {csv_file}")
        return
    
    logger.info("Connecting to the database")
    try:
        configuration: Dict[str, str] = get_database_configurations()
        connection = connect(**configuration)
        cursor = connection.cursor()
        logger.info("Database connection established.")

        # Create table if not exists
        logger.info("Ensuring securities table exists")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS securities (
                id SERIAL PRIMARY KEY,
                segment TEXT,
                scrip_code TEXT,
                security_name TEXT,
                status TEXT,
                group_name TEXT,
                face_value NUMERIC,
                isin_number TEXT,
                industry TEXT,
                scrip_id TEXT,
                nsurl TEXT,
                issuer_name TEXT,
                market_cap NUMERIC
            )
        """)
        connection.commit()
        logger.info("Table check complete.")

        # Insert data from CSV into the database
        logger.info("Inserting securities data from CSV into the database")
        with open(csv_file, mode="r", encoding="utf-8") as file:
            next(file)  # Skip header row
            cursor.copy_expert(
                """
                COPY securities (segment, scrip_code, security_name, status, group_name,
                    face_value, isin_number, industry, scrip_id, nsurl,
                    issuer_name, market_cap)
                FROM STDIN WITH CSV
                """, file
            )
        connection.commit()
        logger.info("All securities data committed successfully from CSV.")
    except DatabaseError as e:
        logger.error(f"Database error: {e}")
        if connection:
            connection.rollback()
            logger.info("Transaction rolled back due to error.")
    finally:
        if cursor:
            cursor.close()
            logger.info("Database cursor closed.")
        if connection:
            connection.close()
            logger.info("Database connection closed.")

def sanitize_segment(segment: str) -> str:

    """
    Convert special characters in segment names to '_'.

    Parameters:
        segment (str): The original segment name.

    Returns:
        str: Sanitized segment name.
    """
    
    return re.sub(r'[^a-zA-Z0-9]', '_', segment).lower()

def save_securities_to_csv(securities: List[Dict[str, Optional[str]]], segment: str, csv_file: str) -> None:

    """
    Save securities data to a CSV file.

    Parameters:
        securities (List[Dict[str, Optional[str]]]): Response from the API containing securities data.
        segment (str): The segment of securities to be saved.
        csv_file (str): The path to the CSV file to save the response.

    Returns:
        None
    """

    logger.info(f"Saving securities data to {csv_file}")
    with open(csv_file, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        header = [
            "segment", "scrip_code", "security_name", "status", "group_name", 
            "face_value", "isin_number", "industry", "scrip_id", "nsurl", 
            "issuer_name", "market_cap"
        ]
        writer.writerow(header)
        
        for security in securities:
            record = [
                segment,
                security.get("SCRIP_CD") or None,
                security.get("Scrip_Name") or None,
                security.get("Status") or None,
                security.get("GROUP") or None,
                security.get("FACE_VALUE") or None,
                security.get("ISIN_NUMBER") or None,
                security.get("INDUSTRY") or None,
                security.get("scrip_id") or None,
                security.get("NSURL") or None,
                security.get("Issuer_Name") or None,
                security.get("Mktcap") or None
            ]
            writer.writerow(record)
    
    logger.info(f"Securities data saved successfully to {csv_file}")

time_zone: ZoneInfo = ZoneInfo("Asia/Kolkata")
current_date: datetime = datetime.now(time_zone)
start_date = datetime(current_date.year, current_date.month, 1, tzinfo=time_zone)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": start_date,
    "retries": 0
}

dag: DAG = DAG("get_securities_from_bse", default_args=default_args, schedule_interval="@monthly", catchup=True)

drop_table = PythonOperator(
task_id="drop_securities_table",
python_callable=drop_existing_table,
provide_context=True,
dag=dag,
)

fetch_tasks = []
ingest_tasks = []

for segment in BSE_SEGMENTS_AND_INTERNAL_CODES.keys():
    fetch = PythonOperator(
        task_id=f"fetch_{sanitize_segment(segment)}",
        python_callable=fetch_and_save,
        op_kwargs={"segment": segment},
        provide_context=True,
        dag=dag,
    )
    
    ingest = PythonOperator(
        task_id=f"ingest_{sanitize_segment(segment)}",
        python_callable=ingest_securities,
        op_kwargs={"csv_file": os.path.join(get_securities_file_path(), f"{sanitize_segment(segment)}_securities.csv")},
        provide_context=True,
        dag=dag,
    )
    
    fetch_tasks.append(fetch)
    ingest_tasks.append(ingest)
    fetch >> ingest

create_index = PythonOperator(
    task_id="index_securities_table",
    python_callable=index_securities,
    dag=dag,
)

chain(drop_table, fetch_tasks, ingest_tasks, create_index)