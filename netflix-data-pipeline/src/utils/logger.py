"""
Logging utilities for the Netflix data pipeline
Provides structured JSON logging with contextual information
"""

import logging
import json
import sys
from datetime import datetime
from pathlib import Path


class JSONFormatter(logging.Formatter):
    """Custom formatter that outputs logs in JSON format"""
    
    def format(self, record):
        log_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
            'message': record.getMessage()
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
        
        # Add extra fields if present
        if hasattr(record, 'extra_data'):
            log_data.update(record.extra_data)
        
        return json.dumps(log_data)


def setup_logger(name, log_file=None, level=logging.INFO):
    """
    Set up a logger with both console and file handlers
    
    Args:
        name: Logger name
        log_file: Path to log file (optional)
        level: Logging level
    
    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Remove existing handlers
    logger.handlers = []
    
    # Console handler with simple format
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # File handler with JSON format
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(JSONFormatter())
        logger.addHandler(file_handler)
    
    return logger


def log_with_context(logger, level, message, **context):
    """
    Log a message with additional context data
    
    Args:
        logger: Logger instance
        level: Log level (e.g., logging.INFO)
        message: Log message
        **context: Additional context as keyword arguments
    """
    extra = {'extra_data': context}
    logger.log(level, message, extra=extra)


# Create default logger
default_logger = setup_logger('netflix_pipeline', 'logs/pipeline.log')
