from logging import Formatter, getLogger, Handler, LogRecord
from streamlit import empty
from streamlit.runtime.scriptrunner import get_script_run_ctx
from typing import Callable, Any

class StreamlitLogHandler(Handler):
    
    """
    Description:
        Custom log handler for Streamlit that captures and displays logs in a styled, scrollable box on the UI.

    Parameters:
        placeholder (streamlit.delta_generator.DeltaGenerator): Streamlit placeholder to inject the log box into.
    """

    def __init__(self, placeholder) -> None:
        super().__init__()
        self.placeholder = placeholder
        self.log_lines = []

    @staticmethod
    def decorate(func: Callable[..., Any]) -> Callable[..., Any]:

        """
        Function decorator that injects StreamlitLogHandler to stream logs in real time.

        Parameters:
            func (Callable[..., Any]): Target function to wrap.

        Returns:
            Callable[..., Any]: The wrapped function with logging streamed to the Streamlit UI.
        """
        
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            log_placeholder = empty()
            handler = StreamlitLogHandler(log_placeholder)
            formatter = Formatter("%(asctime)s | %(levelname)s | %(message)s", "%H:%M:%S")
            handler.setFormatter(formatter)

            root_logger = getLogger()
            root_logger.addHandler(handler)

            try:
                return func(*args, **kwargs)
            finally:
                root_logger.removeHandler(handler)
                handler.flush()
                log_placeholder.empty()

        return wrapped

    def emit(self, record: LogRecord) -> None:
        
        """
        Description:
            Emit a log record to the Streamlit UI. Skips rendering if not in an active Streamlit context.

        Parameters:
            record (LogRecord): Log record generated by logger.

        Returns:
            None
        """

        if get_script_run_ctx() is None:
            return

        message = self.format(record)
        self.log_lines.append(message)
        formatted_logs = "\n".join(self.log_lines[-200:])  # Keep only last 200 lines

        log_display = f"""
<style>
.log-box {{
    background-color: #111;
    color: #0f0;
    padding: 10px;
    border-radius: 8px;
    font-family: monospace;
    font-size: 13px;
    height: 400px;
    overflow-y: scroll;
    white-space: pre-wrap;
    border: 1px solid #333;
}}
</style>
<div class="log-box">{formatted_logs}</div>
"""
        self.placeholder.markdown(log_display, unsafe_allow_html=True)

    def flush(self):
        
        """
        Description:
            Clears the internal log buffer.

        Parameters:
            None

        Returns:
            None
        """

        self.log_lines.clear()