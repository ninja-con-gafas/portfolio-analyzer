"""
This module centralizes various constants used across multiple programs.

Attributes:
    BSE_SEGMENTS (Dict[str, str]): A dictionary representing possible segments of securities on the BSE.
    BSE_SEGMENTS_AND_INTERNAL_CODES (Dict[str, str]): A mapping of Bombay Stock Exchange (BSE) segment names to their respective internal codes.
    BSE_STATUSES (Dict[str, str]): A dictionary representing possible statuses of securities on the BSE.
    LIST_OF_SUPPORTED_BROKERS (list): A list of supported brokers.
    LIST_OF_SUPPORTED_TRADEBOOK_FILE_FORMATS (list): A list of supported file formats for tradebook.
    NSE_SEGMENTS_AND_SERIES (dict[str, list[str]]): A mapping of National Stock Exchange (NSE) segment categories to series codes.
    NSE_SEGMENTS_AND_SERIES_INFO_TEXT (str): A brief information about National Stock Exchange (NSE) segments and series.
"""

BSE_SEGMENTS = {"Equity T+1": "Equity T+1",
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
                "Social Stock Exchange": "Social Stock Exchange"}


BSE_SEGMENTS_AND_INTERNAL_CODES = {"Equity T+1": "Equity",
                                   "Equity T+0": "EQT0",
                                   "Derivatives": "DER",
                                   "Exchange Traded Funds": "MF",
                                   "Debt or Others": "DB",
                                   "Currency Derivatives": "CR",
                                   "Commodity": "CO",
                                   "Electronic Gold Receipts": "EGR",
                                   "Hybrid Security": "HS",
                                   "Municipal Bonds": "MB",
                                   "Preference Shares": "Preference Shares",
                                   "Debentures and Bonds": "Debentures and Bonds",
                                   "Equity - Institutional Series": "Equity - Institutional Series",
                                   "Commercial Papers": "Commercial Papers",
                                   "Social Stock Exchange": "SSE"}

BSE_STATUSES = {"Active": "Active", "Suspended": "Suspended", "Delisted": "Delisted"}

LIST_OF_SUPPORTED_BROKERS = ["Zerodha"]

LIST_OF_SUPPORTED_TRADEBOOK_FILE_FORMATS = ["csv"]

NSE_SEGMENTS_AND_SERIES = {
    "Fully Paid Equity Shares or Exchange Traded Funds": ['A', 'EQ'],
    # "Mutual Funds (Close-ended)": ['MF', 'ME'],
    # "Government Securities": ['G', 'GB', 'GS', 'SG', 'TB'],
}

NSE_SEGMENTS_AND_SERIES_INFO_TEXT = (
    """The NSE series is a classification system used by the National Stock Exchange of India (NSE) to categorize
    securities based on their trading characteristics. Each series represents a specific segment of security, such as
    equity shares, debt instruments, mutual funds, or government bonds, and outlines the trading mechanism,
    settlement process, and regulatory requirements associated with them. This system helps traders easily identify
    and trade securities in compliance with NSE guidelines.
    For more details, visit the [NSE Series Guide](https://www.nseindia.com/market-data/legend-of-series).""")
