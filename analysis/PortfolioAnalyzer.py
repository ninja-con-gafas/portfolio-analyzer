import logging
import os
from datetime import date, datetime, timedelta
from finance.securities import get_corporate_events, get_historical_data
from pandas import DataFrame as pandas_DataFrame
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, date_format, first, lit, min, round, sum as spark_sum, to_date, to_timestamp, when
from typing import Dict, List, Optional, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PortfolioAnalyzer:

    """
    PortfolioAnalyzer processes and analyzes a stock portfolio based on trade records, corporate actions, and historical 
    prices to calculate portfolio holdings over time.

    Attributes:
        spark (SparkSession): Spark session instance.
        tradebook_path (str): Path to the tradebook CSV file.
        broker (str): Name of the broker (e.g., Zerodha).
        tradebook (DataFrame): Processed and adjusted trade data.
        symbols (List[str]): Unique stock symbols present in the portfolio.
        tenure (Tuple[date, date]): Start and end dates of portfolio activity.
        earliest_trade_dates (Dict[str, datetime]): First recorded trade date for each stock.
        historical_data (Dict[str, DataFrame]): Historical price data for stocks in the portfolio.
        jdbc_postgresql_url (str): JDBC URL for connecting to the PostgreSQL database.
        database_properties_securities (Dict[str, str]): Connection properties for the `securities` database.
        corporate_events (Dict[str, DataFrame]): Corporate actions impacting stock quantity (stock splits and bonuses).
        dates (DataFrame): Market trading dates relevant to the portfolio.
        quotes (DataFrame): Aggregated historical stock prices.
        trades (DataFrame): Compiled trade quantities across different periods.
        holdings (DataFrame): Portfolio holdings over time, adjusted for corporate actions.
    """
    
    def __init__(self, broker: str, tradebook_path: str):

        """
        Initializes the PortfolioAnalyzer with tradebook data and market history.

        Args:
            broker (str): The name of the broker.
            tradebook_path (str): Path to the tradebook CSV file.
        """

        logger.info("Initializing PortfolioAnalyzer")
        self.spark = (SparkSession.builder
                      .appName("portfolio-analyzer")
                      .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
                      .getOrCreate())
        self.tradebook_path: str = tradebook_path
        self.broker: str = broker
        self.tradebook: DataFrame = self.load_tradebook()
        self.symbols: List[str] = self.extract_symbols()
        self.tenure: Tuple[date, date] = self.calculate_tenure()
        self.earliest_trade_dates: Dict[str, datetime] = self.fetch_earliest_trade_dates()
        self.historical_data: Dict[str, DataFrame] = self.download_historical_data()
        self.jdbc_postgresql_url: str = self.get_jdbc_postgresql_url()
        self.database_properties_securities :Dict[str, str] = self.get_database_properties_securities()
        self.corporate_events: Dict[str, DataFrame] = self.get_corporate_events()
        self.tradebook: DataFrame = self.adjust_tradebook_for_corporate_actions()
        self.dates: DataFrame = self.extract_market_days()
        self.quotes: DataFrame = self.compile_quotes()
        self.trades: DataFrame = self.compile_trades()
        self.holdings: DataFrame = self.calculate_holdings()
        logger.info("PortfolioAnalyzer initialized successfully")

    def adjust_tradebook_for_corporate_actions(self) -> DataFrame:

        """
        Adjusts tradebook quantities based on corporate events (bonus and split).

        Returns:
            DataFrame: Adjusted tradebook DataFrame.
        """

        logger.info("Taking into account corporate events")
        consolidated_corporate_events = None
        for symbol, events in self.corporate_events.items():
            events = (events.withColumn("ex_date", to_date(col("ex_date"), "dd MMM yyyy"))
                      .withColumn("symbol", lit(symbol)))
            consolidated_corporate_events = (events if consolidated_corporate_events is None 
                                             else consolidated_corporate_events.unionByName(events))

        if consolidated_corporate_events is None:
            logger.info("No corporate events for given securities")
            return self.tradebook
        
        logger.info("Calculating effective quantities based on stock splits and bonuses")
        return (self.tradebook.join(consolidated_corporate_events, on="symbol", how="left")
                .withColumn("quantity",
                            when(col("trade_date") < col("ex_date"), 
                                 col("quantity") * col("ratio"))
                                 .otherwise(col("quantity")))
                                 .drop("ex_date", "ratio", "details", "type", "amount"))

    def calculate_holdings(self) -> DataFrame:

        """
        Computes the portfolio holdings over time by aggregating trade quantities and market prices.

        Returns:
            DataFrame: A Spark DataFrame containing the daily portfolio valuation.

        Schema:
            root
            |-- timestamp: date (nullable = true)
            |-- *symbol: double (nullable = true)

            *symbol represents all the column for each stock symbol.
        """

        logger.info("Calculating holdings")
        quantity_columns = [f"{symbol}_quantity" for symbol in self.symbols]
        quote_columns = [f"{symbol}_quote" for symbol in self.symbols]
        window = Window.orderBy("timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)

        return (self.trades.join(self.quotes, on="timestamp", how="inner")
                .fillna(0)
                .select("timestamp", 
                        *quote_columns, 
                        *[spark_sum(col(col_name)).over(window).alias(f"{col_name}_balance") for col_name in quantity_columns])
                .select("timestamp",
                        *[round((col(f"{symbol}_quote") * col(f"{symbol}_quantity_balance")), 2).alias(symbol) for symbol in self.symbols]))

    def calculate_tenure(self) -> Tuple:

        """
        Determines the start and end date of the portfolio's trade history.
        
        Returns:
            Tuple[date, date]: The first and last trade dates plus one day for end.
        """

        logger.info("Calculating tenure")
        return (self.tradebook.agg({"trade_date": "min"}).collect()[0][0], 
                self.tradebook.agg({"trade_date": "max"}).collect()[0][0] + timedelta(days=1))
    
    def compile_quotes(self) -> DataFrame:

        """
        Combines historical quotes for all tracked symbols into a single DataFrame.
        
        Returns:
            DataFrame: A DataFrame containing market quotes per day.

        Schema:
            root
            |-- timestamp: date (nullable = true)
            |-- *symbol_quote: double (nullable = true)

            *symbol_quote represents all the column for each stock symbol.
        """

        logger.info("Compiling quotes")
        quotes = self.dates
        for ticker, data in self.historical_data.items():
            logger.info(f"Compiling quotes for {ticker}")
            quotes = (quotes.join(data.withColumnRenamed("quote", f"{ticker}_quote"), on="timestamp", how="inner")
                      .drop("symbol"))
        return quotes.withColumn("timestamp", to_date(col("timestamp")))

    def compile_trades(self) -> DataFrame:

        """
        Aggregates trade data by symbol and date, ensuring alignment with market dates.
        
        Returns:
            DataFrame: A Spark DataFrame containing daily trade activity.

        Schema:
            root
            |-- timestamp: date (nullable = true)
            |-- *symbol_quantity: long (nullable = true)

            *symbol_quantity represents all the column for each stock symbol.
        """

        logger.info("Compiling trades")
        return (self.tradebook.groupBy("trade_date")
                .pivot("symbol")
                .agg(first("quantity"))
                .selectExpr("trade_date", 
                            *[f"`{symbol}` as {symbol}_quantity" for symbol in self.symbols])
                .join(self.dates, self.dates.timestamp == col("trade_date"), how="outer")
                .drop("trade_date")
                .withColumn("timestamp", to_date(col("timestamp")))
                .fillna(0))
    
    def download_historical_data(self) -> Dict[str, DataFrame]:

        """
        Downloads historical price data for all portfolio symbols.
        
        Returns:
            Dict[str, DataFrame]: A dictionary mapping stock symbols to historical price data.
        """

        logger.info("Downloading historical data")
        historical_data = {}
        for symbol in self.symbols:
            logger.info(f"Downloading historical data for {symbol}")
            historical_data[symbol] = (self.spark.createDataFrame(get_historical_data(ticker=f"{symbol}.NS", start_date=str(self.tenure[0]), 
                                                                                      end_date=str(self.tenure[1]),
                                                                                      frequency="1d", quote_type="adjclose"))
                                       .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
                                       .withColumn("timestamp", date_format(col("timestamp"), "yyyy-MM-dd"))
                                       .withColumn("quote", round(col("quote"), 2))
                                       .withColumn("symbol", lit(symbol)))
        return historical_data
    
    def extract_market_days(self) -> DataFrame:

        """
        Extracts available market trading days from the historical data.
        
        Returns:
            DataFrame: A DataFrame containing unique trading dates.

        Schema:
            root
            |-- timestamp: string (nullable = true)
        """

        logger.info("Extracting market days")
        return list(self.historical_data.values())[0].select("timestamp")

    def extract_symbols(self) -> List[str]:

        """
        Extracts unique stock symbols from the tradebook.
        
        Returns:
            List[str]: A list of stock symbols present in the tradebook.
        """

        logger.info("Extracting symbols")
        return [row["symbol"] for row in self.tradebook.select("symbol").distinct().collect()]
    
    def fetch_earliest_trade_dates(self) -> Dict[str, datetime]:

        """
        Fetch the earliest trade date for each stock symbol from the tradebook.

        Returns:
            Dict[str, datetime]: A dictionary mapping each stock symbol to its earliest trade date.
        """

        logger.info("Fetching the earliest trade date for each stock symbol")
        return {row["symbol"]: row["start_date"] for row in 
                ((self.tradebook.groupBy("symbol")
                  .agg(min("trade_date")
                       .alias("start_date")))
                       .collect())}
    
    def get_corporate_events(self) -> Dict[str, DataFrame]:

        """
        Fetch corporate events (stock splits and bonus issues) for each stock symbol.

        Returns:
            Dict[str, DataFrame]: A dictionary mapping stock symbols to their respective corporate events DataFrame.

        Schema:
            root
            |-- details: string (nullable = true)
            |-- ex_date: string (nullable = true)
            |-- ratio: double (nullable = true)
            |-- type: string (nullable = true)
            |-- amount: double (nullable = true)
        """

        logger.info("Fetching corporate events (stock splits and bonus issues) for each stock symbol")
        script_codes: Dict[str, str] = self.get_script_codes(self.symbols)
        return {
            symbol: x
            for symbol in self.symbols
            if (x := self.spark.createDataFrame(
                get_corporate_events(
                    scriptcode=script_codes.get(symbol),
                    start_date=str(self.earliest_trade_dates.get(symbol)),
                    end_date=datetime.today().strftime('%Y-%m-%d'))
                    ).filter("type IN ('split', 'bonus')")
                    ).count() > 0}
    
    def get_dates(self) -> DataFrame:

        """
        Get the DataFrame containing the market trading days.

        Returns:
            DataFrame: A DataFrame with market trading dates.

        Schema:
            root
            |-- timestamp: string (nullable = true)
        """

        logger.info("Getting dates")
        return self.dates

    def get_historical_data(self) -> Dict[str, DataFrame]:

        """
        Get the historical stock price data for all tracked symbols.

        Returns:
            Dict[str, DataFrame]: A dictionary mapping stock symbols to their historical price DataFrames.
        """

        logger.info("Getting historical data")
        return self.historical_data
    
    def get_holdings(self) -> DataFrame:

        """
        Get the calculated holdings over time.

        Returns:
            DataFrame: A DataFrame representing the portfolio holdings for each symbol over time.

        Schema:
            root
            |-- timestamp: date (nullable = true)
            |-- *symbol: double (nullable = true)

            *symbol represents all the column for each stock symbol.
        """

        logger.info("Getting holdings")
        return self.holdings
    
    def get_holdings_as_pandas_dataframe(self) -> pandas_DataFrame:

        """
        Converts Spark DataFrame holdings to a Pandas DataFrame.
        
        Returns:
            pandas_DataFrame: Holdings data in Pandas format.

        Schema:
            root
            |-- timestamp: date (nullable = true)
            |-- *symbol: double (nullable = true)

            *symbol represents all the column for each stock symbol.
        """

        logger.info("Converting holdings to Pandas DataFrame")
        return self.holdings.toPandas()
    
    def get_jdbc_postgresql_url(self) -> str:

        """
        Construct the JDBC URL for connecting to the PostgreSQL `portfolioanalyzer` database.

        Returns:
            str: The JDBC connection URL for PostgreSQL `portfolioanalyzer` database.
        """

        logger.info("Getting JDBC URL for connecting to portfolioanalyzer database")
        return f"jdbc:postgresql://{os.environ["POSTGRES_HOST"]}/{os.environ["POSTGRES_PORTFOLIOANALYZER_DATABASE"]}"
    
    def get_quotes(self) -> DataFrame:
        
        """
        Get the compiled stock quotes over time.

        Returns:
            DataFrame: A DataFrame containing stock prices for all symbols over time.

        Schema:
            root
            |-- timestamp: date (nullable = true)
            |-- *symbol_quote: double (nullable = true)

            *symbol_quote represents all the column for each stock symbol.    
        """

        logger.info("Getting quotes")
        return self.quotes
    
    def get_database_properties_securities(self):

        """
        Retrieve database connection properties for the `securities` database.

        Returns:
            Dict[str, str]: A dictionary containing database connection properties including the `username`, `password`, and JDBC `driver`.
        """

        logger.info("Getting database connection properties for the `securities` database")
        return {"user": os.environ["POSTGRES_PORTFOLIOANALYZER_USERNAME"],
                "password": os.environ["POSTGRES_PORTFOLIOANALYZER_PASSWORD"],
                "driver": "org.postgresql.Driver"}
    
    def get_script_codes(self, tickers: List[str], segment: str = "Equity T+1", status: str = "Active") -> Dict[str, Optional[str]]:

        """
        Fetch the BSE script codes given a list of ticker symbols, from `securities` table in PostgreSQL database.

        Parameters:
            tickers (List[str]): List of ticker symbols.
            segment (str): The segment to filter by ('Equity T+1', 'Equity T+0', 'Derivatives', 'Exchange Traded Funds',
                                                'Debt or Others', 'Currency Derivatives', 'Commodity', 'Electronic Gold Receipts',
                                                'Hybrid Security', 'Municipal Bonds', 'Preference Shares', 'Debentures and Bonds',
                                                'Equity - Institutional Series', 'Commercial Papers', 'Social Stock Exchange', default: 'Equity T+1').
            status (str): The status to filter by ('Active', 'Suspended', 'Delisted', default: Active).

        Returns:
            Dict[str, Optional[str]]: A dictionary with tickers as keys and script codes as values.
        """

        VALID_SEGMENTS = {
            "Equity T+1": "Equity T+1",
            "Equity T+0": "Equity T+0",
            "Derivatives": "Derivatives",
            "Exchange Traded Funds": "Exchange Traded Funds",
            "Debt or Others": "Debt or Others",
            "Currency Derivatives": "Currency Derivatives",
            "Commodity": "Commodity",
            "Electronic Gold Receipts": "Electronic Gold Receipts",
            "Hybrid Security": "Hybrid Security",
            "Municipal Bonds": "Municipal Bonds",
            "Preference Shares": "Preference Shares",
            "Debentures and Bonds": "Debentures and Bonds",
            "Equity - Institutional Series": "Equity - Institutional Series",
            "Commercial Papers": "Commercial Papers",
            "Social Stock Exchange": "Social Stock Exchange"
        }

        VALID_STATUSES = {"Active": "Active", "Suspended": "Suspended", "Delisted": "Delisted"}

        if segment not in VALID_SEGMENTS:
            logger.warning(f"Invalid segment '{segment}', using 'Equity T+1' as default segment filter.")
            segment = VALID_SEGMENTS.get(segment, "Equity")

        if status not in VALID_STATUSES:
            logger.warning(f"Invalid status '{status}', using 'Active' as default status filter.")
            status = VALID_STATUSES.get(status, "Active")

        query = f"""
                (SELECT scrip_id, scrip_code
                FROM securities
                WHERE segment = '{VALID_SEGMENTS.get(segment)}'
                AND status = '{VALID_STATUSES.get(status)}'
                AND scrip_id IN ({','.join([f"'{ticker}'" for ticker in tickers])})) AS securities_filtered
                """
        
        logger.info(f"Getting script codes for {tickers} from the `securities` database")
        script_codes: DataFrame = self.spark.read.jdbc(url=self.jdbc_postgresql_url, table=query, properties=self.database_properties_securities)

        script_codes: Dict[str, str] = {row['scrip_id']: row['scrip_code'] for row in script_codes.collect()}

        for ticker in tickers:
            if ticker not in script_codes:
                logger.warning(f"Ticker '{ticker}' not found in database.")
                script_codes[ticker] = None

        logger.info(f"Script codes {script_codes} fetched from the `securities` database")
        return script_codes
    
    def get_symbols(self) -> List[str]:
        
        """
        Get the list of unique stock symbols from the tradebook.

        Returns:
            List[str]: A list of stock symbols in the portfolio.
        """
        
        logger.info("Getting symbols")
        return self.symbols
    
    def get_tenure(self) -> Tuple:

        """
        Get the portfolio tenure, i.e., the date range of recorded trades.

        Returns:
            Tuple[date, date]: The start and end date of portfolio activity.
        """

        logger.info("Getting tenure")
        return self.tenure
    
    def get_tradebook(self) -> DataFrame:

        """
        Get the tradebook containing all trade transactions.

        Returns:
            DataFrame: A Spark DataFrame representing the tradebook.

        Schema:
            root
            |-- trade_date: date (nullable = true)
            |-- symbol: string (nullable = true)
            |-- average_price: double (nullable = true)
            |-- quantity: long (nullable = true)
        """

        logger.info("Getting tradebook")
        return self.tradebook
    
    def get_tradebook_as_pandas_dataframe(self) -> pandas_DataFrame:

        """
        Converts Spark DataFrame tradebook to a Pandas DataFrame.
        
        Returns:
            pandas_DataFrame: Tradebook data in Pandas format.

        Schema:
            root
            |-- trade_date: date (nullable = true)
            |-- symbol: string (nullable = true)
            |-- average_price: double (nullable = true)
            |-- quantity: long (nullable = true)
        """

        logger.info("Converting tradebook to Pandas DataFrame")
        return self.tradebook.toPandas()
    
    def get_trades(self) -> DataFrame:

        """
        Get the compiled trade transactions over time.

        Returns:
            DataFrame: A DataFrame with processed trade transactions per market day.

        Schema:
            root
            |-- timestamp: date (nullable = true)
            |-- *symbol_quantity: long (nullable = true)

            *symbol_quantity represents all the column for each stock symbol.
        """

        logger.info("Getting trades")
        return self.trades

    def load_tradebook(self) -> DataFrame:

        """
        Loads and processes the tradebook from a CSV file.
        
        Returns:
            DataFrame: A Spark DataFrame containing cleaned trade data.

        Schema:
            root
            |-- trade_date: date (nullable = true)
            |-- symbol: string (nullable = true)
            |-- average_price: double (nullable = true)
            |-- quantity: long (nullable = true)
        """

        logger.info("Loading tradebook")
        if self.broker == "Zerodha":
            return (self.spark.read.csv(self.tradebook_path, inferSchema=True, header=True)
                .drop("isin", "exchange", "segment", "series", "auction", "trade_id", "order_id", "order_execution_time")
                .withColumn("trade_date", to_date(col("trade_date"), "yyyy-MM-dd"))
                .withColumn("quantity", when(col("trade_type") == "buy", col("quantity")).otherwise(-col("quantity")))
                .withColumn("weighted_price", col("price") * col("quantity"))
                .groupBy("trade_date", "symbol")
                .agg((spark_sum("weighted_price") / spark_sum("quantity")).alias("average_price"),
                        spark_sum("quantity").alias("quantity"))
                .filter(col("quantity") != 0)
                .drop("weighted_price"))
