"""
Modbus abstraction layer for wattmonitor - provides unified access to modbus_tk and pymodbus
"""

import logging
import itertools
from time import sleep
from typing import Optional

from . import (
    ModbusClientInterface, 
    ModbusException,
    create_modbus_client_with_fallback,
    ModbusConfig,
    setup_modbus_logging
)


class WattMonitorModbusClient:
    """
    Wrapper class that provides the old modbus_tk interface for wattmonitor
    while using the new abstraction layer underneath.
    """
    
    def __init__(self, host: str, port: int = 502, client_type: str = "modbus_tk"):
        self.host = host
        self.port = port
        self.client_type = client_type
        self._client: Optional[ModbusClientInterface] = None
        self._logger = logging.getLogger(__name__)
        
    def open(self):
        """Open connection (compatibility method for modbus_tk interface)"""
        if self._client is None:
            self._client, actual_type = create_modbus_client_with_fallback(
                self.host, 
                self.port, 
                preferred_client=self.client_type
            )
            
            if actual_type != self.client_type:
                self._logger.info(f"Using {actual_type} instead of requested {self.client_type}")
        
        if not self._client.connect():
            raise ModbusException("Failed to connect to Modbus server")
    
    def close(self):
        """Close connection"""
        if self._client:
            self._client.disconnect()
    
    def set_timeout(self, timeout: float):
        """Set timeout (compatibility method)"""
        if self._client:
            self._client.set_timeout(timeout)
    
    @property
    def is_connected(self) -> bool:
        """Check if connected"""
        return self._client is not None and self._client.is_connected()
    
    # For compatibility with existing meter code that might access the client directly
    def execute(self, slave_id: int, function_code: int, starting_address: int, quantity_of_x: int, **kwargs):
        """
        Execute modbus command (compatibility method for modbus_tk interface)
        Only supports READ_HOLDING_REGISTERS for now
        """
        if not self._client:
            raise ModbusException("Not connected to Modbus server")
        
        # For now, we only support holding register reads (function code 3)
        # This covers the majority of use cases in wattmonitor
        if function_code == 3:  # READ_HOLDING_REGISTERS
            return self._client.read_holding_registers(slave_id, starting_address, quantity_of_x)
        else:
            raise ModbusException(f"Function code {function_code} not supported in abstraction layer")


def create_modbus_master(host: str, port: int = 502, client_type: str = "modbus_tk") -> WattMonitorModbusClient:
    """
    Create a Modbus master (client) instance.
    
    Args:
        host: Modbus server hostname or IP
        port: Modbus server port
        client_type: Type of client to use ("modbus_tk" or "pymodbus")
        
    Returns:
        WattMonitorModbusClient instance
    """
    return WattMonitorModbusClient(host, port, client_type)


def connect_modbus(master: WattMonitorModbusClient, logger: logging.Logger):
    """
    Connect to Modbus server with retry logic.
    This replaces the original connect_modbus function in wattmonitor.py
    """
    backoff = itertools.chain((1, 2, 4, 8, 16, 32, 64, 128, 256, 300), itertools.repeat(300))
    while True:
        try:
            master.open()
            logger.info("Connected to Modbus server")
            return
        except (ModbusException, Exception) as exc:
            delay = next(backoff)
            logger.error("Modbus connection failed: %s. Retrying in %d seconds...", exc, delay)
            sleep(delay)


def setup_modbus_logger(level: int = logging.DEBUG) -> logging.Logger:
    """
    Setup modbus logger (replaces modbus_tk.utils.create_logger)
    """
    setup_modbus_logging(level)
    return logging.getLogger('modbus')
