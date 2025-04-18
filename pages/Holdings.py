from altair import Chart, condition, selection_point, value
from config.ui_components import set_sidebar
from config.StreamlitLogHandler import StreamlitLogHandler
from logging import basicConfig, getLogger, INFO
from process.Equity import Equity
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_trunc, sum as spark_sum
from streamlit import altair_chart, error, selectbox, session_state, set_page_config, spinner, stop, success, title

# Configure logging
basicConfig(level=INFO)
logger = getLogger(__name__)

# Configure the Streamlit page
set_page_config(page_title="Holdings", layout="wide")

@StreamlitLogHandler.decorate
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

    logger.info(f"Processing tradebook for broker: {session_state["broker"]}, segment: {session_state["segment"]}, path: {session_state["tradebook_path"]}")

    analyzer = Equity(spark=session_state["spark"], broker=session_state["broker"], segment=session_state["segment"],
                      tradebook_path=session_state["tradebook_path"])
    session_state["tradebook"] = analyzer.get_tradebook()
    session_state["holdings"] = analyzer.get_holdings()

def get_holdings_chart() -> None:

    """
    Creates an interactive Altair line chart to visualize portfolio holdings over time.

    - Uses selection filtering for highlighting securities.
    - Displays valuation trends over time.
    - Enables tooltip interactivity.
    """

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

def get_latest_holdings_pie_chart() -> None:

    """
    Creates an interactive Altair pie chart to visualize the latest holding pattern.

    - Uses selection filtering for highlighting securities.
    - Displays the share of various securities of the total valuation.
    - Enables tooltip interactivity.
    """

    selection = selection_point(fields=["Security"], bind="legend")
    latest_date = session_state["melted_holdings"]["Date"].max()
    latest_holdings = session_state["melted_holdings"][session_state["melted_holdings"]["Date"] == latest_date]

    total_valuation = latest_holdings["Valuation"].sum()
    latest_holdings["Percentage"] = round((latest_holdings["Valuation"] / total_valuation * 100), 2)

    logger.info("Creating latest holdings pie chart.")
    session_state["latest_holdings_pie_chart"] = (Chart(latest_holdings,
                                                        width=400,
                                                        height=400,
                                                        title="Latest Holdings Pattern")
                                                        .mark_arc()
                                                        .encode(theta="Valuation:Q",
                                                                color="Security:N",
                                                                tooltip=["Security", "Valuation", "Percentage"],
                                                                opacity=condition(selection, value(1), value(0.1)))        
                                                        .add_params(selection)
                                                        .interactive())

def get_melted_holdings() -> None:

    """
    Resamples holdings data using PySpark based on the selected time granularity and transforms holdings data into a 
    long-format DataFrame for visualization.

    - Renames `timestamp` to `Date` and converts it to string.
    - Melts the holdings DataFrame to have columns: Date, Security, Valuation.

    Parameters:
        None

    Returns:
        None

    Raises:
        Exception: If an error occurs during transformation.
    """

    granularity_map = {
        "Daily": None,
        "Weekly": "week",
        "Monthly": "month",
        "Yearly": "year"
    }

    granularity = session_state["granularity"]
    holdings: DataFrame = session_state["holdings"]

    try:
        session_state["melted_holdings"] = ((holdings.withColumn("timestamp", date_trunc(granularity_map[granularity], col("timestamp")))
                                            if granularity_map[granularity] is not None else holdings)
                                            .groupBy("timestamp")
                                            .agg(*[spark_sum(col(security)).alias(security) for security in holdings.columns if security != "timestamp"])
                                            .toPandas()
                                            .rename(columns={"timestamp": "Date"})
                                            .assign(Date=lambda x: x["Date"].astype(str)).melt(id_vars=["Date"], var_name="Security", value_name="Valuation"))
    except Exception as e:
        logger.error(f"Error transforming holdings data: {e}")
        error(f"An error occurred while processing the tradebook: {e}")
        stop()

def get_valuation_chart() -> None:

    """
    Creates an interactive Altair line chart to visualize portfolio valuation over time.

    - Displays valuation trends over time.
    - Enables tooltip interactivity.
    """

    logger.info("Creating valuation chart.")
    session_state["valuation_chart"] = (Chart(session_state["melted_holdings"],
                                              width="container",
                                              height=700,
                                              title="Portfolio Valuation Over Time")
                                        .mark_line()
                                        .encode(x="Date:N",
                                                y="sum(Valuation):Q",
                                                tooltip=["Date", "sum(Valuation)"])
                                        .interactive())

def main() -> None:

    """
    Streamlit entry point for Holdings page.

    - Displays a title.
    - Loads and processes tradebook data if not already cached.
    - Generates and displays `Portfolio Holdings Over Time` chart.
    - Generates and displays `Portfolio Valuation Over Time` chart.
    - Generates and displays `Latest Holdings Pattern` pie chart.
    """

    title("💰 Holdings")
    set_sidebar()
    logger.info("Holdings page loaded.")
    
    if "holdings" not in session_state:
        with spinner("🔄 Processing tradebook. Please wait."):
            get_holdings()
            success("✅ Holdings data processed successfully!")

    session_state["granularity"] = selectbox("Select Time Granularity", ["Daily", "Weekly", "Monthly", "Yearly"], index=0)

    with spinner(f"🔁 Generating charts for {session_state["granularity"]} granularity..."):
        get_melted_holdings()
        get_holdings_chart()
        get_valuation_chart()
        get_latest_holdings_pie_chart()
        logger.info("Charts data generated successfully.")
        success(f"✅ Charts updated to {session_state["granularity"]} view!")

    altair_chart(session_state["holdings_chart"], use_container_width=True)
    altair_chart(session_state["valuation_chart"], use_container_width=True)
    altair_chart(session_state["latest_holdings_pie_chart"], use_container_width=True)

if __name__ == "__main__":
    main()
