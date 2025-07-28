"""
Modbus abstraction layer - provides a unified interface for different Modbus libraries
"""

from .base import ModbusClientInterface, ModbusException
from .factory import create_modbus_client, create_modbus_client_with_fallback, get_available_clients
from .config import ModbusConfig, setup_modbus_logging, validate_modbus_config
from .modbus_coordinator import ModbusCoordinator, get_coordinator, initialize_coordinator
from .modbus_wrapper import create_modbus_master, connect_modbus, setup_modbus_logger

__all__ = [
    'ModbusClientInterface', 
    'ModbusException',
    'create_modbus_client', 
    'create_modbus_client_with_fallback',
    'get_available_clients',
    'ModbusConfig',
    'setup_modbus_logging',
    'validate_modbus_config',
    'ModbusCoordinator',
    'get_coordinator',
    'initialize_coordinator',
    'create_modbus_master',
    'connect_modbus',
    'setup_modbus_logger'
]
