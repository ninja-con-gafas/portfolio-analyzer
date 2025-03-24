import logging
from streamlit import file_uploader, selectbox, session_state, set_page_config, success, switch_page, title
from streamlit.runtime.uploaded_file_manager import UploadedFile
from tempfile import NamedTemporaryFile
from time import sleep

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure the Streamlit page
set_page_config(page_title="Portfolio Analyzer", layout="wide")

# List of supported brokers and file formats
LIST_OF_SUPPORTED_BROKERS = ["Zerodha"]
LIST_OF_SUPPORTED_FILE_FORMATS = ["csv"]

def get_tradebook_path() -> None:
    
    """
    Save the uploaded tradebook file to a temporary location and store the path in session state.

    Raises:
        ValueError: If no file is uploaded before calling this function.
    """

    uploaded_file: UploadedFile = session_state["uploaded_file"]

    logger.info("Saving uploaded tradebook to a temporary file.")
    with NamedTemporaryFile(delete=False, suffix=".csv") as tmp_file:
        tmp_file.write(uploaded_file.getbuffer())
        session_state["tradebook_path"] = tmp_file.name
        logger.info(f"Tradebook saved to temporary file: {tmp_file.name}")

def main():

    """
    Streamlit app entry point that allows users to upload their tradebook and 
    select a stock broker for portfolio analysis.
    
    - Displays a title and broker selection dropdown.
    - Allows users to upload a tradebook file.
    - Saves the uploaded file path and broker selection in session state.
    - Redirects to the dashboard after a short delay.
    """

    logger.info("Starting Portfolio Analyzer.")
    title("📊 Analyze Your Portfolio")
    
    broker: str = selectbox("Select Stock Broker", LIST_OF_SUPPORTED_BROKERS, index=0)
    logger.info(f"Selected broker: {broker}")
    
    uploaded_file = file_uploader("Upload Tradebook", type=LIST_OF_SUPPORTED_FILE_FORMATS)
    
    if uploaded_file:
        logger.info(f"File {uploaded_file.name} uploaded successfully.")
        success(f"✅ File {uploaded_file.name} uploaded successfully!")

        session_state["uploaded_file"] = uploaded_file
        session_state["broker"] = broker
        session_state["cached_holdings_chart"] = None  # Reset cached chart

        get_tradebook_path()

        logger.info("Redirecting to the dashboard.")
        sleep(1)  # Short delay before redirecting to the dashboard

        switch_page("pages/Dashboards.py")

if __name__ == "__main__":
    main()
