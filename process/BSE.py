import logging
from config.global_variables import BSE_SEGMENTS, BSE_SEGMENTS_AND_INTERNAL_CODES, BSE_STATUSES
from pyspark.sql import DataFrame, SparkSession
import requests
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_script_codes(spark: SparkSession, jdbc_postgresql_url:str, database_properties_securities: Dict[str, str]
                     , tickers: List[str], segment: str = "Equity T+1", status: str = None, debug: bool = False) -> Dict[str, Optional[str]]:

    """
    Fetch the BSE script codes given a list of ticker symbols, from `securities` table in PostgreSQL database.

    Parameters:
        spark (SparkSession): Spark session instance.
        tickers (List[str]): List of ticker symbols.
        segment (str): The segment to filter by ('Equity T+1', 'Equity T+0', 'Derivatives', 'Exchange Traded Funds',
                                            'Debt or Others', 'Currency Derivatives', 'Commodity', 'Electronic Gold Receipts',
                                            'Hybrid Security', 'Municipal Bonds', 'Preference Shares', 'Debentures and Bonds',
                                            'Equity - Institutional Series', 'Commercial Papers', 'Social Stock Exchange', default: 'Equity T+1').
        status (str): The status to filter by ('Active', 'Suspended', 'Delisted', default: None).
        debug (bool): Flag to enable debug logging.

    Returns:
        Dict[str, Optional[str]]: A dictionary with tickers as keys and script codes as values.
    """

    if debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug mode is enabled.")

    if segment not in BSE_SEGMENTS:
        logger.warning(f"Invalid segment '{segment}', using 'Equity T+1' as default segment filter.")
        segment = BSE_SEGMENTS.get(segment, "Equity T+1")
        logger.debug(f"Segment: f{segment}")

    if status not in BSE_STATUSES:
        logger.warning(f"Invalid status '{status}', using no status filter.")
        status = None
        logger.debug(f"Status: f{status}")

    query = f"""
            (SELECT scrip_id, scrip_code
            FROM securities
            WHERE segment = '{BSE_SEGMENTS.get(segment)}'
            {f"AND status = '{BSE_STATUSES.get(status)}'" if status else ""}
            AND scrip_id IN ({','.join([f"'{ticker}'" for ticker in tickers])})) AS securities_filtered
            """
    logger.debug(f"Query: {query}")
    
    logger.info(f"Getting script codes for {tickers} from the `securities` database")
    script_codes: DataFrame = spark.read.jdbc(url=jdbc_postgresql_url, table=query, properties=database_properties_securities)

    script_codes: Dict[str, str] = {row['scrip_id']: row['scrip_code'] for row in script_codes.collect()}

    for ticker in tickers:
        if ticker not in script_codes:
            logger.warning(f"Ticker '{ticker}' not found in database.")
            script_codes[ticker] = None

    logger.info(f"Script codes {script_codes} fetched from the `securities` database")
    return script_codes


def get_securities(segment: str = "Equity T+1", status: str = None, debug: bool = False) -> List[Dict[str, Optional[str]]]:

    """
    Fetch the information of all the securities listed on the BSE for a given segment.

    Parameters:
        segment (str): The segment to filter by ('Equity T+1', 'Equity T+0', 'Derivatives', 'Exchange Traded Funds',
                                                'Debt or Others', 'Currency Derivatives', 'Commodity', 'Electronic Gold Receipts',
                                                'Hybrid Security', 'Municipal Bonds', 'Preference Shares', 'Debentures and Bonds',
                                                'Equity - Institutional Series', 'Commercial Papers', 'Social Stock Exchange', default: 'Equity').
        status (str): The status to filter by ('Active', 'Suspended', 'Delisted', default: None, meaning no filter).
        debug (bool): Flag to enable debug logging.

    Returns:
        List[Dict[str, Optional[str]]]: Response from the API.
    """

    if debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug mode is enabled.")

    if segment not in BSE_SEGMENTS_AND_INTERNAL_CODES:
        logger.warning(f"Invalid segment '{segment}', using 'Equity T+1' as default segment filter.")
        segment = BSE_SEGMENTS_AND_INTERNAL_CODES.get(segment, "Equity")

    if status not in BSE_STATUSES:
        logger.warning(f"Invalid status '{status}', ignoring status filter.")
        status = BSE_STATUSES.get(status, None)

    url = "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w"

    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Origin': 'https://www.bseindia.com',
        'Referer': 'https://www.bseindia.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0',
        'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Microsoft Edge";v="134"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    params = {
        'Group': '',
        'Scripcode': '',
        'industry': '',
        'segment': segment,
        'status': status if status else '',
    }

    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        securities = response.json()
        logger.debug(f"Fetched {len(securities)} securities.")
        return securities
    except requests.RequestException as e:
        logger.error(f"Request error: {e}")
    except ValueError as e:
        logger.error(f"JSON parsing error: {e}")