"""
Base meter class that uses the centralized Modbus coordinator
"""

import modbus_tk.defines as cst
from modbus_coordinator import get_coordinator


class BaseMeter:
    """
    Base class for all meter implementations.
    Uses the centralized Modbus coordinator for thread-safe communication.
    """
    
    def __init__(self, modbus_master, address=1):
        self._modbus = modbus_master  # Keep for compatibility, but don't use directly
        self._address = address
        self._coordinator = get_coordinator()
        
    def modbus_id(self):
        return self._address
    
    def _readregister(self, register, size, datatype=""):
        """
        Read Modbus registers using the centralized coordinator.
        This method is thread-safe and replaces direct modbus access.
        """
        if self._coordinator is None:
            raise RuntimeError("Modbus coordinator not initialized. Call initialize_coordinator() first.")
            
        return self._coordinator.read_registers(self._address, register, size, datatype)
