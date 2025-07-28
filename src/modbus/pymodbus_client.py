"""
Modbus client implementation using pymodbus library
"""

import logging
from typing import List, Dict, Any
from .base import ModbusClientInterface, ModbusException

try:
    from pymodbus.client.sync import ModbusTcpClient
    from pymodbus.exceptions import ModbusException as PymodbusException
    from pymodbus.exceptions import ConnectionException
    PYMODBUS_AVAILABLE = True
except ImportError:
    try:
        # Try newer pymodbus API
        from pymodbus.client import ModbusTcpClient
        from pymodbus.exceptions import ModbusException as PymodbusException
        from pymodbus.exceptions import ConnectionException
        PYMODBUS_AVAILABLE = True
    except ImportError:
        PYMODBUS_AVAILABLE = False


class PymodbusClient(ModbusClientInterface):
    """
    Modbus client implementation using pymodbus library.
    """
    
    def __init__(self, host: str, port: int = 502):
        if not PYMODBUS_AVAILABLE:
            raise ImportError("pymodbus library is not available")
        
        self.host = host
        self.port = port
        self._client = None
        self._connected = False
        self._timeout = 5.0
        self._logger = logging.getLogger(__name__)
    
    def connect(self) -> bool:
        """Establish TCP connection to Modbus server"""
        try:
            self._client = ModbusTcpClient(
                host=self.host, 
                port=self.port,
                timeout=self._timeout
            )
            
            # Connect to the server
            connection_result = self._client.connect()
            
            if connection_result:
                self._connected = True
                self._logger.info(f"Connected to Modbus TCP server {self.host}:{self.port}")
                return True
            else:
                self._connected = False
                self._logger.error(f"Failed to connect to {self.host}:{self.port}")
                return False
                
        except Exception as e:
            self._connected = False
            self._logger.error(f"Failed to connect to {self.host}:{self.port}: {e}")
            return False
    
    def disconnect(self) -> None:
        """Close connection to Modbus server"""
        if self._client:
            try:
                self._client.close()
            except Exception as e:
                self._logger.warning(f"Error during disconnect: {e}")
            finally:
                self._client = None
                self._connected = False
                self._logger.info("Disconnected from Modbus server")
    
    def is_connected(self) -> bool:
        """Check if client is connected"""
        return self._connected and self._client is not None and self._client.connected
    
    def read_holding_registers(self, slave_id: int, address: int, count: int) -> List[int]:
        """Read holding registers using pymodbus"""
        if not self.is_connected():
            raise ModbusException("Not connected to Modbus server")
        
        try:
            response = self._client.read_holding_registers(
                address=address,
                count=count,
                slave=slave_id 
            )
            
            # Check for errors in the response
            if response.isError():
                raise ModbusException(f"Modbus read error: {response}")
            
            return response.registers
            
        except PymodbusException as e:
            raise ModbusException(f"Pymodbus error: {str(e)}")
        except ConnectionException as e:
            self._connected = False  # Mark as disconnected
            raise ModbusException(f"Connection error: {str(e)}")
        except Exception as e:
            raise ModbusException(f"Communication error: {str(e)}")
    
    def set_timeout(self, timeout: float) -> None:
        """Set communication timeout"""
        self._timeout = timeout
        if self._client:
            self._client.timeout = timeout
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get connection information"""
        return {
            'library': 'pymodbus',
            'host': self.host,
            'port': self.port,
            'connected': self._connected,
            'timeout': self._timeout
        }
