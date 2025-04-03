"""
This module centralizes configuration of UI components.

Attributes:
    set_sidebar (Callable): Configures the sidebar navigation for the application.
"""

from streamlit import sidebar

def set_sidebar() -> None:

    """
    Configures the sidebar navigation for the application.

    Parameters:
        None

    Returns:
        None
    """

    sidebar.page_link("pages/Home.py", label="Home")
    sidebar.page_link("pages/Holdings.py", label="Holdings")