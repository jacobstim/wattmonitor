"""
Factory module for creating Modbus client instances
"""

import logging
from typing import Optional
from .base import ModbusClientInterface


def create_modbus_client(
    host: str, 
    port: int = 502, 
    client_type: str = "modbus_tk",
    **kwargs
) -> ModbusClientInterface:
    """
    Create a Modbus client instance of the specified type.
    
    Args:
        host: Modbus server hostname or IP address
        port: Modbus server port (default: 502)
        client_type: Type of client to create ("modbus_tk" or "pymodbus")
        **kwargs: Additional arguments passed to the client constructor
        
    Returns:
        Configured Modbus client instance
        
    Raises:
        ValueError: If client_type is not supported
        ImportError: If required library is not available
    """
    logger = logging.getLogger(__name__)
    
    if client_type.lower() == "modbus_tk":
        try:
            from .modbus_tk_client import ModbusTkClient
            logger.info(f"Creating modbus_tk client for {host}:{port}")
            return ModbusTkClient(host, port, **kwargs)
        except ImportError as e:
            raise ImportError(f"modbus_tk library not available: {e}")
    
    elif client_type.lower() == "pymodbus":
        try:
            from .pymodbus_client import PymodbusClient
            logger.info(f"Creating pymodbus client for {host}:{port}")
            return PymodbusClient(host, port, **kwargs)
        except ImportError as e:
            raise ImportError(f"pymodbus library not available: {e}")
    
    else:
        raise ValueError(f"Unsupported client type: {client_type}. Use 'modbus_tk' or 'pymodbus'")


def get_available_clients() -> list:
    """
    Get list of available Modbus client types.
    
    Returns:
        List of available client type strings
    """
    available = []
    
    try:
        from .modbus_tk_client import ModbusTkClient
        available.append("modbus_tk")
    except ImportError:
        pass
    
    try:
        from .pymodbus_client import PymodbusClient
        available.append("pymodbus")
    except ImportError:
        pass
    
    return available


def create_modbus_client_with_fallback(
    host: str,
    port: int = 502,
    preferred_client: str = "modbus_tk",
    **kwargs
) -> tuple[ModbusClientInterface, str]:
    """
    Create a Modbus client with automatic fallback to available libraries.
    
    Args:
        host: Modbus server hostname or IP address
        port: Modbus server port (default: 502)
        preferred_client: Preferred client type
        **kwargs: Additional arguments passed to the client constructor
        
    Returns:
        Tuple of (client_instance, actual_client_type)
        
    Raises:
        ImportError: If no Modbus libraries are available
    """
    logger = logging.getLogger(__name__)
    available_clients = get_available_clients()
    
    if not available_clients:
        raise ImportError("No Modbus libraries available. Install modbus_tk or pymodbus.")
    
    # Try preferred client first
    if preferred_client in available_clients:
        try:
            client = create_modbus_client(host, port, preferred_client, **kwargs)
            logger.info(f"Using preferred Modbus client: {preferred_client}")
            return client, preferred_client
        except Exception as e:
            logger.warning(f"Failed to create preferred client {preferred_client}: {e}")
    
    # Fallback to any available client
    for client_type in available_clients:
        if client_type != preferred_client:  # Don't retry the preferred one
            try:
                client = create_modbus_client(host, port, client_type, **kwargs)
                logger.info(f"Using fallback Modbus client: {client_type}")
                return client, client_type
            except Exception as e:
                logger.warning(f"Failed to create fallback client {client_type}: {e}")
    
    raise ImportError("Failed to create any Modbus client implementation")
