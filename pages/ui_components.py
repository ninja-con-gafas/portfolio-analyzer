from streamlit import sidebar

def set_sidebar() -> None:

    """
    Configures the sidebar navigation for the application.
    """

    sidebar.page_link("pages/Home.py", label="Home")
    sidebar.page_link("pages/Holdings.py", label="Holdings")