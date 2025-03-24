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

def get_melted_holdings() -> DataFrame:

    """
    Retrieve and transform holdings data into a long-format DataFrame 
    for visualization.

    - Checks if a tradebook has been uploaded.
    - Loads and processes tradebook data using PortfolioAnalyzer.
    - Melts the holdings data to make it suitable for Altair visualization.

    Returns:
        DataFrame: A long-format DataFrame with columns: Date, Security, Valuation.

    Raises:
        Exception: If an error occurs while processing the tradebook.
    """

    if "tradebook_path" not in session_state:
        logger.error("No tradebook found in session state.")
        error("No tradebook found. Please upload a file first.")
        stop()

    broker: str = session_state["broker"]
    tradebook_path: str = session_state["tradebook_path"]
    logger.info(f"Processing tradebook for broker: {broker}, path: {tradebook_path}")

    try:
        analyzer: PortfolioAnalyzer = PortfolioAnalyzer(broker=broker, tradebook_path=tradebook_path)
        logger.info("PortfolioAnalyzer initialized successfully.")
        
        return (analyzer.get_holdings_as_pandas_dataframe()
                .rename(columns={"timestamp": "Date"})
                .assign(Date=lambda df: df["Date"].astype(str))
                .melt(id_vars=["Date"], var_name="Security", value_name="Valuation"))

    except Exception as e:
        logger.error(f"An error occurred while processing the tradebook: {e}")
        error(f"An error occurred while processing the tradebook: {e}")
        stop()

def get_holdings_chart(holdings_melted: DataFrame) -> Chart:

    """
    Create an interactive Altair line chart to visualize portfolio holdings over time.

    - Uses a selection filter for highlighting securities.
    - Displays valuation trends over time.
    - Enables tooltip interactivity.

    Args:
        holdings_melted (DataFrame): A long-format DataFrame containing holdings data.

    Returns:
        Chart: An Altair chart object for visualization.
    """

    logger.info("Creating holdings chart.")
    selection = selection_point(fields=["Security"], bind="legend")

    return (Chart(holdings_melted, width="container", height=700, title="Portfolio Holdings Over Time")
            .mark_line()
            .encode(x="Date:N",
                    y="Valuation:Q",
                    color="Security:N",
                    tooltip=["Date", "Security", "Valuation"],
                    opacity=condition(selection, value(1), value(0.1)))
            .add_params(selection)
            .interactive())

def main():

    """
    Streamlit entry point for Dashboards.

    - Displays a title and checks for cached chart data.
    - Loads holdings data and generates an Altair chart.
    - Caches the chart to optimize performance.
    """

    title("📈 Dashboards")
    logger.info("Dashboards page loaded.")

    if "cached_holdings_chart" in session_state and session_state["cached_holdings_chart"] is not None:
        logger.info("Using cached holdings chart.")
        altair_chart(session_state["cached_holdings_chart"], use_container_width=True)
        return

    with spinner("🔄 Processing tradebook... Please wait."):
        holdings_melted: DataFrame = get_melted_holdings()
    
    success("✅ Holdings data processed successfully!")
    logger.info("Holdings data processed successfully.")

    holdings_chart: Chart = get_holdings_chart(holdings_melted)
    session_state["cached_holdings_chart"] = holdings_chart
    altair_chart(holdings_chart, use_container_width=True)
    logger.info("Chart displayed successfully.")

if __name__ == "__main__":
    main()
