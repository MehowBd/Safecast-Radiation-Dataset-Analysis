import logging
import colorlog

def setup_logger():
    """Setup logger with color and custom levels."""
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(
        '%(log_color)s%(levelname)s: %(message)s',
        log_colors={
            'INFO': 'blue',
            'SUCCESS': 'green',  # custom level for success messages
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white',
        }
    ))

    logger = logging.getLogger('DataProcessor')
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)  # Set to DEBUG to capture all types of logs
    # Adding custom level for success
    logging.SUCCESS = 25  # Setting a level between INFO (20) and WARNING (30)
    logging.addLevelName(logging.SUCCESS, 'SUCCESS')
    
    def success(self, message, *args, **kws):
        if self.isEnabledFor(logging.SUCCESS):
            self._log(logging.SUCCESS, message, args, **kws)
    
    logging.Logger.success = success

    return logger
