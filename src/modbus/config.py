"""
Configuration and utilities for the Modbus abstraction layer
"""

import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class ModbusConfig:
    """Configuration for Modbus client connections"""
    host: str
    port: int = 502
    timeout: float = 5.0
    client_type: str = "modbus_tk"
    retry_attempts: int = 3
    inter_request_delay: float = 0
    device_delays: Optional[Dict[int, float]] = None
    
    def __post_init__(self):
        if self.device_delays is None:
            self.device_delays = {}


def setup_modbus_logging(level: int = logging.INFO) -> None:
    """
    Configure logging for the Modbus abstraction layer.
    
    Args:
        level: Logging level (default: INFO)
    """
    logger = logging.getLogger('modbus')
    logger.setLevel(level)
    

def validate_modbus_config(config: ModbusConfig) -> bool:
    """
    Validate Modbus configuration parameters.
    
    Args:
        config: ModbusConfig instance to validate
        
    Returns:
        True if configuration is valid
        
    Raises:
        ValueError: If configuration is invalid
    """
    if not config.host:
        raise ValueError("Host cannot be empty")
    
    if not (1 <= config.port <= 65535):
        raise ValueError(f"Port must be between 1-65535, got {config.port}")
    
    if config.timeout <= 0:
        raise ValueError(f"Timeout must be positive, got {config.timeout}")
    
    if config.retry_attempts < 0:
        raise ValueError(f"Retry attempts cannot be negative, got {config.retry_attempts}")
    
    if config.inter_request_delay < 0:
        raise ValueError(f"Inter-request delay cannot be negative, got {config.inter_request_delay}")
    
    if config.client_type not in ["modbus_tk", "pymodbus"]:
        raise ValueError(f"Unsupported client type: {config.client_type}")
    
    return True
