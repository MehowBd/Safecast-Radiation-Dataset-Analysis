import logging
from colorama import Fore, Style, init, Back

init()

class ColoredFormatter(logging.Formatter):
    """A colorful formatter."""

    LOG_COLORS = {
        'DEBUG': Fore.CYAN,
        'INFO': Fore.GREEN,
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'CRITICAL': Fore.WHITE + Back.RED + Style.BRIGHT
    }

    def format(self, record):
        color = self.LOG_COLORS.get(record.levelname, Fore.WHITE)
        formatter = logging.Formatter(f"{color}%(levelname)s: %(message)s{Style.RESET_ALL}")
        return formatter.format(record)

def setup_logger(name='application', level=logging.DEBUG):  # Changed level to DEBUG
    """Setup a logger with a colored formatter."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Console handler with color
    ch = logging.StreamHandler()
    ch.setLevel(level)  # Ensure the handler also has DEBUG level
    ch.setFormatter(ColoredFormatter())

    logger.addHandler(ch)
    return logger

# Example of setting up the logger
if __name__ == "__main__":
    logger = setup_logger()
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")
