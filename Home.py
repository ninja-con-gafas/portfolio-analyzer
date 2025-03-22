from streamlit import file_uploader, selectbox, session_state, set_page_config, success, switch_page, title
from streamlit.runtime.uploaded_file_manager import UploadedFile
from tempfile import NamedTemporaryFile
from time import sleep

# Configure the Streamlit page
set_page_config(page_title="Portfolio Analyzer", layout="wide")

# List of supported brokers and file formats
LIST_OF_SUPPORTED_BROKERS = ["Zerodha"]
LIST_OF_SUPPORTED_FILE_FORMATS = ["csv"]

def get_tradebook_path(uploaded_file: UploadedFile) -> str:
    
    """
    Save the uploaded tradebook file to a temporary location and return its path.

    Args:
        uploaded_file (UploadedFile): The uploaded file object from Streamlit.

    Returns:
        str: The file path of the temporarily stored tradebook.
    """

    with NamedTemporaryFile(delete=False, suffix=".csv") as tmp_file:
        tmp_file.write(uploaded_file.getbuffer())
        return tmp_file.name

def main():

    """
    Streamlit app entry point that allows users to upload their tradebook and 
    select a stock broker for portfolio analysis.
    
    - Displays a title and broker selection dropdown.
    - Allows users to upload a tradebook file.
    - Saves the uploaded file path and broker selection in session state.
    - Redirects to the dashboard after a short delay.
    """

    title("📊 Analyze Your Portfolio")
    
    broker: str = selectbox("Select Stock Broker", LIST_OF_SUPPORTED_BROKERS, index=0)
    uploaded_file: UploadedFile = file_uploader("Upload Tradebook", type=LIST_OF_SUPPORTED_FILE_FORMATS)

    if uploaded_file is not None:
        success(f"✅ File '{uploaded_file.name}' uploaded successfully...!")
        
        session_state["tradebook_path"] = get_tradebook_path(uploaded_file)
        session_state["broker"] = broker
        session_state["cached_holdings_chart"] = None
        
        sleep(1)  # Sleep for 1 second before redirecting to the dashboard
        
        switch_page("pages/Dashboards.py")

if __name__ == "__main__":
    main()
