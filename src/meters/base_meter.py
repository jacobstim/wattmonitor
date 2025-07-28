"""
Base meter class that uses the centralized Modbus coordinator
"""

import sys
import os

# Add the src directory to the path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from .data_types import RegisterConfig


class BaseMeter:
    """
    Base class for all meter implementations.
    Uses the centralized Modbus coordinator for thread-safe communication.
    """
    
    def __init__(self, modbus_master, address=1):
        self._modbus = modbus_master  # Keep for compatibility, but don't use directly
        self._address = address
        # Import locally to avoid circular import
        from modbus.modbus_coordinator import get_coordinator
        self._coordinator = get_coordinator()
        
    def modbus_id(self):
        return self._address
    
    def _read_register_config(self, config: RegisterConfig):
        """
        Preferred method: Read Modbus registers using abstract data type configuration.
        This method provides type safety and cleaner abstraction.
        
        Args:
            config: RegisterConfig specifying register address, count, and data type
            
        Returns:
            Decoded value according to the specified data type
        """
        if self._coordinator is None:
            raise RuntimeError("Modbus coordinator not initialized. Call initialize_coordinator() first.")
            
        return self._coordinator.read_register_config(self._address, config)
