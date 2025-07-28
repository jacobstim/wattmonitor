"""
Modbus client implementation using modbus_tk library
"""

import logging
from typing import List, Dict, Any
from .base import ModbusClientInterface, ModbusException

try:
    import modbus_tk
    import modbus_tk.modbus_tcp as modbus_tcp
    import modbus_tk.defines as cst
    MODBUS_TK_AVAILABLE = True
except ImportError:
    MODBUS_TK_AVAILABLE = False


class ModbusTkClient(ModbusClientInterface):
    """
    Modbus client implementation using modbus_tk library.
    """
    
    def __init__(self, host: str, port: int = 502):
        if not MODBUS_TK_AVAILABLE:
            raise ImportError("modbus_tk library is not available")
        
        self.host = host
        self.port = port
        self._master = None
        self._connected = False
        self._timeout = 5.0
        self._logger = logging.getLogger(__name__)
    
    def connect(self) -> bool:
        """Establish TCP connection to Modbus server"""
        try:
            self._master = modbus_tcp.TcpMaster(host=self.host, port=self.port)
            self._master.set_timeout(self._timeout)
            
            # Test connection with a simple read to device 1
            # This will raise an exception if connection fails
            try:
                # Try to read 1 register from address 0 on device 1
                # This is just a connection test - the actual read may fail
                # but if we get a Modbus response (even an error), connection is OK
                self._master.execute(1, cst.READ_HOLDING_REGISTERS, 0, 1)
            except modbus_tk.modbus.ModbusError:
                # Modbus errors are OK - means we're connected but device/register doesn't exist
                pass
            except Exception as e:
                # Network/connection errors are not OK
                self._logger.warning(f"Connection test failed: {e}")
                raise
            
            self._connected = True
            self._logger.info(f"Connected to Modbus TCP server {self.host}:{self.port}")
            return True
            
        except Exception as e:
            self._connected = False
            self._logger.error(f"Failed to connect to {self.host}:{self.port}: {e}")
            return False
    
    def disconnect(self) -> None:
        """Close connection to Modbus server"""
        if self._master:
            try:
                self._master.close()
            except Exception as e:
                self._logger.warning(f"Error during disconnect: {e}")
            finally:
                self._master = None
                self._connected = False
                self._logger.info("Disconnected from Modbus server")
    
    def is_connected(self) -> bool:
        """Check if client is connected"""
        return self._connected and self._master is not None
    
    def read_holding_registers(self, slave_id: int, address: int, count: int) -> List[int]:
        """Read holding registers using modbus_tk"""
        if not self.is_connected():
            raise ModbusException("Not connected to Modbus server")
        
        try:
            result = self._master.execute(
                slave_id, 
                cst.READ_HOLDING_REGISTERS, 
                address,
                quantity_of_x=count,
                expected_length=count
            )
            return list(result)
            
        except modbus_tk.modbus.ModbusError as e:
            error_code = e.get_exception_code() if hasattr(e, 'get_exception_code') else None
            raise ModbusException(f"Modbus error: {str(e)}", error_code)
        except Exception as e:
            raise ModbusException(f"Communication error: {str(e)}")
    
    def set_timeout(self, timeout: float) -> None:
        """Set communication timeout"""
        self._timeout = timeout
        if self._master:
            self._master.set_timeout(timeout)
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get connection information"""
        return {
            'library': 'modbus_tk',
            'host': self.host,
            'port': self.port,
            'connected': self._connected,
            'timeout': self._timeout
        }
