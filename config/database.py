import logging
import os
from typing import Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_database_properties_securities() -> Dict[str, str]:

        """
        Retrieve database connection properties for the `securities` database.

        Returns:
            Dict[str, str]: A dictionary containing database connection properties including the `username`, `password`, and JDBC `driver`.
        """

        logger.info("Getting database connection properties for the `securities` database")
        return {"user": os.environ["POSTGRES_PORTFOLIOANALYZER_USERNAME"],
                "password": os.environ["POSTGRES_PORTFOLIOANALYZER_PASSWORD"],
                "driver": "org.postgresql.Driver"}

def get_jdbc_postgresql_url() -> str:

        """
        Construct the JDBC URL for connecting to the PostgreSQL `portfolioanalyzer` database.

        Returns:
            str: The JDBC connection URL for PostgreSQL `portfolioanalyzer` database.
        """

        logger.info("Getting JDBC URL for connecting to portfolioanalyzer database")
        return f"jdbc:postgresql://{os.environ["POSTGRES_HOST"]}/{os.environ["POSTGRES_PORTFOLIOANALYZER_DATABASE"]}"