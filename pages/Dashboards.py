import logging
from altair import Chart, condition, selection_point, value
from pandas import DataFrame
from PortfolioAnalyzer import PortfolioAnalyzer
from streamlit import altair_chart, error, session_state, set_page_config, spinner, stop, success, title

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure the Streamlit page
set_page_config(page_title="Dashboards", layout="wide")

def get_holdings() -> None:

    """
    Creates holdings data by retrieving and processesing tradebook data for portfolio analysis.
    
    - Checks if tradebook path is available in session state.
    - Processes tradebook using PortfolioAnalyzer.
    - Stores holdings data as a pandas DataFrame in session state.
    
    Raises:
        RuntimeError: If tradebook is missing in session state.
    """

    if "tradebook_path" not in session_state:
        logger.error("No tradebook found in session state.")
        error("No tradebook found. Please upload a file first.")
        stop()

    broker: str = session_state["broker"]
    tradebook_path: str = session_state["tradebook_path"]
    logger.info(f"Processing tradebook for broker: {broker}, path: {tradebook_path}")

    analyzer = PortfolioAnalyzer(broker=broker, tradebook_path=tradebook_path)
    session_state["holdings"] = analyzer.get_holdings_as_pandas_dataframe()

def get_melted_holdings() -> None:

    """
    Transforms holdings data into a long-format DataFrame for visualization.

    - Renames `timestamp` to `Date` and converts it to string.
    - Melts the holdings DataFrame to have columns: Date, Security, Valuation.
    
    Raises:
        Exception: If an error occurs during transformation.
    """

    try:
        if "holdings" not in session_state:
            logger.error("Holdings data not found in session state.")
            error("Holdings data not available.")
            stop()
        
        session_state["melted_holdings"] = (session_state["holdings"]
                                            .rename(columns={"timestamp": "Date"})
                                            .assign(Date=lambda df: df["Date"].astype(str))
                                            .melt(id_vars=["Date"], var_name="Security", value_name="Valuation"))
    except Exception as e:
        logger.error(f"Error transforming holdings data: {e}")
        error(f"An error occurred while processing the tradebook: {e}")
        stop()

def get_holdings_chart() -> None:

    """
    Creates an interactive Altair line chart to visualize portfolio holdings over time.

    - Uses selection filtering for highlighting securities.
    - Displays valuation trends over time.
    - Enables tooltip interactivity.
    """

    if "melted_holdings" not in session_state:
        logger.error("Melted holdings data not found in session state.")
        error("Chart data unavailable.")
        stop()

    logger.info("Creating holdings chart.")
    selection = selection_point(fields=["Security"], bind="legend")

    session_state["holdings_chart"] = (Chart(session_state["melted_holdings"], 
                                             width="container", 
                                             height=700, 
                                             title="Portfolio Holdings Over Time")
                                       .mark_line()
                                       .encode(x="Date:N",
                                               y="Valuation:Q",
                                               color="Security:N",
                                               tooltip=["Date", "Security", "Valuation"],
                                               opacity=condition(selection, value(1), value(0.1)))
                                       .add_params(selection)
                                       .interactive())

def main() -> None:

    """
    Streamlit entry point for Dashboards.

    - Displays a title.
    - Loads and processes tradebook data if not already cached.
    - Generates and displays an Altair chart.
    """

    title("📈 Dashboards")
    logger.info("Dashboards page loaded.")
    
    if "holdings_chart" not in session_state:
        with spinner("🔄 Processing tradebook... Please wait."):
            get_holdings()
            success("✅ Holdings data processed successfully!")
            get_melted_holdings()
            get_holdings_chart()
    
    logger.info("Displaying holdings chart.")
    altair_chart(session_state["holdings_chart"], use_container_width=True)
    logger.info("Chart displayed successfully.")

if __name__ == "__main__":
    main()
