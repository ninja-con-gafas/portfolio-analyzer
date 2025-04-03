import logging
import os
from config.global_variables import (LIST_OF_SUPPORTED_BROKERS, LIST_OF_SUPPORTED_TRADEBOOK_FILE_FORMATS, NSE_SEGMENTS_AND_SERIES, 
                                     NSE_SEGMENTS_AND_SERIES_INFO_TEXT)
from config.ui_components import set_sidebar
from pyspark.sql import SparkSession
from streamlit import file_uploader, selectbox, session_state, set_page_config, spinner, success, switch_page, title
from streamlit.runtime.uploaded_file_manager import UploadedFile
from tempfile import gettempdir, NamedTemporaryFile
from time import sleep

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure the Streamlit page
set_page_config(page_title="Home", layout="wide")

def get_tradebook_path() -> None:
    
    """
    Save the uploaded tradebook file to a temporary location and store the path in session state.

    Raises:
        ValueError: If no file is uploaded before calling this function.
    """

    uploaded_file: UploadedFile = session_state["uploaded_file"]
    logger.info("Saving uploaded tradebook to a temporary file.")
    temporary_directory = os.path.join(gettempdir(), "portfolio-analyzer")
    os.makedirs(temporary_directory, exist_ok=True)
    with NamedTemporaryFile(dir=temporary_directory, delete=False, suffix=".csv") as temporary_file:
        temporary_file.write(uploaded_file.getbuffer())
        session_state["tradebook_path"] = temporary_file.name
        logger.info(f"Tradebook saved to temporary file: {temporary_file.name}")

def main() -> None:

    """
    Streamlit entry point for Home page.
    
    - Displays a title and broker selection dropdown.
    - Allows users to upload a tradebook file.
    - Saves the uploaded file path and broker selection in session state.
    - Redirects to the Holdings page after a short delay.
    """

    title("📊 Analyze Your Portfolio")
    set_sidebar()
    logger.info("Home page loaded.")

    logger.info("Initializing Apache Spark Session for `portfolio-analyzer`")
    with spinner("🔄Initializing Apache Spark Session. Please wait."):
        session_state["spark"] = (SparkSession.builder
                                .appName("portfolio-analyzer")
                                .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
                                .getOrCreate())
    
    session_state["broker"] = selectbox("Select Stock Broker", LIST_OF_SUPPORTED_BROKERS)
    logger.info(f"Selected broker: {session_state["broker"]}")
    
    session_state["segment"] = selectbox("Select Segment", 
                                         options=list(NSE_SEGMENTS_AND_SERIES.keys()),
                                         format_func=lambda key: f"{key} ({', '.join(NSE_SEGMENTS_AND_SERIES[key])})",
                                         help=NSE_SEGMENTS_AND_SERIES_INFO_TEXT)
    logger.info(f"Selected segment: {session_state["segment"]},  corresponding series: {NSE_SEGMENTS_AND_SERIES.get(session_state["segment"])}")

    session_state["uploaded_file"] = file_uploader("Upload Tradebook", type=LIST_OF_SUPPORTED_TRADEBOOK_FILE_FORMATS)
    
    if session_state["uploaded_file"]:
        logger.info(f"File {session_state["uploaded_file"].name} uploaded successfully.")
        success(f"✅ File {session_state["uploaded_file"].name} uploaded successfully!")
        
        get_tradebook_path()

        logger.info("Redirecting to the Holdings.")
        sleep(1)  # Short delay before redirecting to the Holdings

        switch_page("pages/Holdings.py")

if __name__ == "__main__":
    main()
