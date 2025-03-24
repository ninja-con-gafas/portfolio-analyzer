import logging
from datetime import date, timedelta
from finance.securities import get_historical_data
from pandas import DataFrame as pandas_DataFrame
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, date_format, first, lit, round, sum as spark_sum, to_date, to_timestamp, when
from typing import Dict, List, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PortfolioAnalyzer:

    """
    Analyzes a stock portfolio based on trade history and historical market data.
    
    Attributes:
        spark (SparkSession): Spark session instance.
        tradebook_path (str): Path to the tradebook CSV file.
        broker (str): Name of the broker (e.g., Zerodha).
        tradebook (DataFrame): Processed trade data.
        tenure (Tuple[date, date]): Start and end dates of the portfolio's activity.
        symbols (List[str]): List of unique stock symbols in the portfolio.
        historical_data (Dict[str, DataFrame]): Historical price data for each stock.
        dates (DataFrame): Market trading dates.
        quotes (DataFrame): Compiled historical quotes.
        trades (DataFrame): Compiled trade quantities.
        holdings (DataFrame): Portfolio holdings over time.
    """
    
    def __init__(self, broker: str, tradebook_path: str):

        """
        Initializes the PortfolioAnalyzer with tradebook data and market history.

        Args:
            broker (str): The name of the broker.
            tradebook_path (str): Path to the tradebook CSV file.
        """

        logger.info("Initializing PortfolioAnalyzer")
        self.spark: SparkSession = SparkSession.builder.appName("portfolio-analyzer").getOrCreate()
        self.tradebook_path: str = tradebook_path
        self.broker: str = broker
        self.tradebook: DataFrame = self.load_tradebook()
        self.tenure: Tuple[date, date] = self.calculate_tenure()
        self.symbols: List[str] = self.extract_symbols()
        self.historical_data: Dict[str, DataFrame] = self.download_historical_data()
        self.dates: DataFrame = self.extract_market_days()
        self.quotes: DataFrame = self.compile_quotes()
        self.trades: DataFrame = self.compile_trades()
        self.holdings: DataFrame = self.calculate_holdings()
        logger.info("PortfolioAnalyzer initialized successfully")

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
    
    def get_dates(self) -> DataFrame:

        """
        Get the DataFrame containing the market trading days.

        Returns:
            DataFrame: A DataFrame with market trading dates.
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
        """

        logger.info("Getting holdings")
        return self.holdings
    
    def get_holdings_as_pandas_dataframe(self) -> pandas_DataFrame:

        """
        Converts Spark DataFrame holdings to a Pandas DataFrame.
        
        Returns:
            pandas_DataFrame: Holdings data in Pandas format.
        """

        logger.info("Converting holdings to Pandas DataFrame")
        return self.holdings.toPandas()
    
    def get_quotes(self) -> DataFrame:
        
        """
        Get the compiled stock quotes over time.

        Returns:
            DataFrame: A DataFrame containing stock prices for all symbols over time.
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
        """

        logger.info("Getting tradebook")
        return self.tradebook
    
    def get_tradebook_as_pandas_dataframe(self) -> pandas_DataFrame:

        """
        Converts Spark DataFrame tradebook to a Pandas DataFrame.
        
        Returns:
            pandas_DataFrame: Tradebook data in Pandas format.
        """

        logger.info("Converting tradebook to Pandas DataFrame")
        return self.tradebook.toPandas()
    
    def get_trades(self) -> DataFrame:

        """
        Get the compiled trade transactions over time.

        Returns:
            DataFrame: A DataFrame with processed trade transactions per market day.
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
