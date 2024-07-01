import logging 

# Function to configure a logger
def configure_logger(name, log_file, level=logging.DEBUG):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Create file handler which logs even debug messages
    fh = logging.FileHandler(log_file)
    fh.setLevel(level)
    
    # Create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    
    # Create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    
    # Add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    
    return logger




