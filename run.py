import logging
from pages.ui_components import set_sidebar
from streamlit import switch_page

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main() -> None:
    
    """
    Streamlit entry point for `Portfolio Analyzer` application.
    
    - Sets sidebar pages.
    - Redirects to Home page.
    """
    
    logger.info("Starting Portfolio Analyzer.")
    set_sidebar()
    switch_page("pages/Home.py")

if __name__ == "__main__":
    main()
