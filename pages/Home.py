import logging
from pages.ui_components import set_sidebar
from streamlit import file_uploader, selectbox, session_state, set_page_config, success, switch_page, title
from streamlit.runtime.uploaded_file_manager import UploadedFile
from tempfile import NamedTemporaryFile
from time import sleep

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure the Streamlit page
set_page_config(page_title="Home", layout="wide")

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
    
    broker: str = selectbox("Select Stock Broker", LIST_OF_SUPPORTED_BROKERS)
    logger.info(f"Selected broker: {broker}")
    
    uploaded_file = file_uploader("Upload Tradebook", type=LIST_OF_SUPPORTED_FILE_FORMATS)
    
    if uploaded_file:
        logger.info(f"File {uploaded_file.name} uploaded successfully.")
        success(f"✅ File {uploaded_file.name} uploaded successfully!")

        session_state["uploaded_file"] = uploaded_file
        session_state["broker"] = broker

        get_tradebook_path()

        logger.info("Redirecting to the Holdings.")
        sleep(1)  # Short delay before redirecting to the Holdings

        switch_page("pages/Holdings.py")

if __name__ == "__main__":
    main()
