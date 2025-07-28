"""
Abstract base interface for Modbus client implementations
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any


class ModbusClientInterface(ABC):
    """
    Abstract interface for Modbus client implementations.
    This provides a unified API regardless of the underlying library (modbus_tk, pymodbus, etc.)
    """
    
    @abstractmethod
    def connect(self) -> bool:
        """
        Establish connection to the Modbus server.
        
        Returns:
            True if connection successful, False otherwise
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """
        Close the connection to the Modbus server.
        """
        pass
    
    @abstractmethod
    def is_connected(self) -> bool:
        """
        Check if client is currently connected.
        
        Returns:
            True if connected, False otherwise
        """
        pass
    
    @abstractmethod
    def read_holding_registers(self, slave_id: int, address: int, count: int) -> List[int]:
        """
        Read holding registers from a Modbus device.
        
        Args:
            slave_id: Modbus slave/unit ID
            address: Starting register address
            count: Number of registers to read
            
        Returns:
            List of register values
            
        Raises:
            ModbusException: On communication or protocol errors
        """
        pass
    
    @abstractmethod
    def set_timeout(self, timeout: float) -> None:
        """
        Set the communication timeout.
        
        Args:
            timeout: Timeout value in seconds
        """
        pass
    
    @abstractmethod
    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get connection information for diagnostics.
        
        Returns:
            Dictionary with connection details
        """
        pass


class ModbusException(Exception):
    """
    Base exception for all Modbus communication errors.
    Provides a unified exception type regardless of underlying library.
    """
    
    def __init__(self, message: str, error_code: Optional[int] = None):
        super().__init__(message)
        self.error_code = error_code
        self.message = message
    
    def __str__(self):
        if self.error_code is not None:
            return f"{self.message} (Code: {self.error_code})"
        return self.message
