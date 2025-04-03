import logging
from process.BSE import get_script_codes
from config.database import get_database_properties_securities, get_jdbc_postgresql_url
from config.global_variables import NSE_SEGMENTS_AND_SERIES
from datetime import date, datetime, timedelta
from finance.securities import get_corporate_events, get_historical_data
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, date_format, first, lit, min, round, sum as spark_sum, to_date, to_timestamp, when
from typing import Callable, Dict, List, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Equity:

    """
    Performs time series analysis on an equity portfolio using trade records, corporate actions, and historical prices.

    Attributes:
        spark (SparkSession): Spark session instance.
        tradebook_path (str): Path to the tradebook CSV file.
        broker (str): Name of the broker (e.g., Zerodha).
        series  (str): The series corresponding to the segment of the security.
        tradebook (DataFrame): Processed and adjusted trade data.
        symbols (List[str]): Unique stock symbols present in the portfolio.
        tenure (Tuple[date, date]): Start and end dates of portfolio activity.
        earliest_trade_dates (Dict[str, datetime]): First recorded trade date for each stock.
        historical_data (Dict[str, DataFrame]): Historical price data for stocks in the portfolio.
        corporate_events (Dict[str, DataFrame]): Corporate actions impacting stock quantity (stock splits and bonuses).
        dates (DataFrame): Market trading dates relevant to the portfolio.
        quotes (DataFrame): Aggregated historical stock prices.
        trades (DataFrame): Compiled trade quantities across different periods.
        holdings (DataFrame): Portfolio holdings over time, adjusted for corporate actions.
    """
    
    def __init__(self, spark: SparkSession, broker: str, segment: str, tradebook_path: str):

        """
        Initializes the PortfolioAnalyzer with tradebook data and market history.

        Parameters:
            spark (SparkSession): Spark session instance.
            broker (str): The name of the broker.
            segment (str): The segment of the security.
            tradebook_path (str): Path to the tradebook CSV file.
        """

        logger.info("Initializing PortfolioAnalyzer")
        self.spark = spark
        self.tradebook_path: str = tradebook_path
        self.broker: str = broker
        self.series: tuple = NSE_SEGMENTS_AND_SERIES.get(segment)
        self.tradebook: DataFrame = self.load_tradebook()
        self.symbols: List[str] = self.extract_symbols()
        self.tenure: Tuple[date, date] = self.calculate_tenure()
        self.earliest_trade_dates: Dict[str, datetime] = self.extract_earliest_trade_dates()
        self.historical_data: Dict[str, DataFrame] = self.download_historical_data()
        self.corporate_events: Dict[str, DataFrame] = self.fetch_corporate_events()
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

        logger.info("Taking into account stock splits and bonuses")
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
    
    def extract_earliest_trade_dates(self) -> Dict[str, datetime]:

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
    
    def fetch_corporate_events(self) -> Dict[str, DataFrame]:

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
        script_codes: Dict[str, str] = get_script_codes(spark=self.spark,
                                                        jdbc_postgresql_url=get_jdbc_postgresql_url(), 
                                                        database_properties_securities=get_database_properties_securities(), 
                                                        tickers=self.symbols, 
                                                        segment="Equity T+1")
        return {symbol: x
                for symbol in self.symbols
                if (events := get_corporate_events(
                    scriptcode=script_codes.get(symbol),
                    start_date=str(self.earliest_trade_dates.get(symbol)),
                    end_date=datetime.today().strftime('%Y-%m-%d'))) 
                and 
                    (x := self.spark.createDataFrame(events).filter("type IN ('split', 'bonus')")).count() > 0}
    
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

        logger.info("Getting corporate events for stocks.")
        return self.corporate_events

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
    
    def get_earliest_trade_dates(self) -> Dict[str, datetime]:

        """
        Get the first recorded trade date for each stock.

        Returns:
            Dict[str, datetime]: A dictionary mapping stock symbols to their first recorded trade date.
        """

        logger.info("Getting first recorded trade date for each stock.")
        return self.earliest_trade_dates

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
        
        def load_zerodha_tradebook(self) -> DataFrame:

            """
            Loads the tradebook for Zerodha.

            Returns:
                DataFrame: A Spark DataFrame containing cleaned trade data.
            """

            logger.info(f"Loading tradebook, broker: {self.broker}, series: {self.series}")
            return (self.spark.read.csv(self.tradebook_path, inferSchema=True, header=True)
                    .drop("isin", "exchange", "segment", "auction", "trade_id", "order_id", "order_execution_time")
                    .filter(col("series").isin(*self.series))
                    .withColumn("trade_date", to_date(col("trade_date"), "yyyy-MM-dd"))
                    .withColumn("quantity", when(col("trade_type") == "buy", col("quantity")).otherwise(-col("quantity")))
                    .withColumn("weighted_price", col("price") * col("quantity"))
                    .groupBy("trade_date", "symbol")
                    .agg((spark_sum("weighted_price") / spark_sum("quantity")).alias("average_price"),
                        spark_sum("quantity").alias("quantity"))
                    .drop("weighted_price"))
        
        TRADEBOOK_LOADERS: Dict[str, Callable] = {}

        if not TRADEBOOK_LOADERS:
            TRADEBOOK_LOADERS["Zerodha"] = load_zerodha_tradebook

        logger.info("Loading tradebook")

        # Fetch the appropriate tradebook loader
        loader = TRADEBOOK_LOADERS.get(self.broker)
        
        if loader:
            return loader(self)
