"""
Modbus Coordinator - Centralized, single-threaded Modbus communication
This module manages all Modbus communication to prevent race conditions
while maintaining the modularity of individual meter definitions.
"""

import threading
import logging
from typing import Dict, List, Tuple, Any
from datetime import datetime
import time


class ModbusRequest:
    """Represents a single Modbus register read request"""
    def __init__(self, meter_id: int, register: int, size: int, datatype: str = ""):
        self.meter_id = meter_id
        self.register = register
        self.size = size
        self.datatype = datatype
        self.timestamp = datetime.now()
        self.result = None
        self.error = None


class ModbusCoordinator:
    """
    Centralized Modbus communication coordinator.
    Ensures all Modbus requests are executed sequentially in a single thread.
    """
    
    def __init__(self, modbus_master):
        self.modbus_master = modbus_master
        self._request_queue = []
        self._response_cache = {}  # Cache responses for a short time to avoid duplicate reads
        self._cache_timeout = 3.0  # Cache responses for 3 seconds
        self._lock = threading.Lock()
        self._logger = logging.getLogger(__name__)
        
        # Add inter-request delays to prevent communication mix-ups
        self._inter_request_delay = 0.05  # 50ms default delay between requests
        self._device_delays = {
            # Special delays for specific device types that need more time
            30: 0.15,  # CSMB devices need longer delays (150ms)
        }
        self._last_request_time = 0
        self._retry_attempts = 3  # Number of retry attempts for failed requests
        
    def read_registers(self, meter_id: int, register: int, size: int, datatype: str = "") -> Any:
        """
        Read Modbus registers for a specific meter.
        This method is thread-safe and caches recent responses.
        
        Args:
            meter_id: Modbus slave ID of the meter
            register: Starting register address
            size: Number of registers to read
            datatype: Data format string for modbus_tk
            
        Returns:
            Register data or raises exception on error
        """
        # Create cache key
        cache_key = (meter_id, register, size, datatype)
        
        with self._lock:
            # Check if we have a recent cached response
            if cache_key in self._response_cache:
                cached_result, timestamp = self._response_cache[cache_key]
                if time.time() - timestamp < self._cache_timeout:
                    self._logger.debug(f"Cache hit for meter {meter_id}, register {register}")
                    return cached_result
                else:
                    # Remove expired cache entry
                    del self._response_cache[cache_key]
            
            # Implement inter-request delay to prevent communication mix-ups
            self._wait_for_bus_ready(meter_id)
            
            # Retry logic for failed requests
            last_exception = None
            for attempt in range(self._retry_attempts):
                try:
                    # Execute the Modbus request
                    import modbus_tk.defines as cst
                    
                    self._logger.debug(f"Reading meter {meter_id}, register 0x{register:04X}, size {size} (attempt {attempt + 1})")
                    
                    if datatype:
                        result = self.modbus_master.execute(
                            meter_id, cst.READ_HOLDING_REGISTERS, register, quantity_of_x=size, expected_length=size, data_format=datatype
                        )
                    else:
                        result = self.modbus_master.execute(
                            meter_id, cst.READ_HOLDING_REGISTERS, register, quantity_of_x=size, expected_length=size
                        )
                    
                    # Validate the response - check if we got a reasonable result
                    if result is None:
                        raise Exception("Received null response")
                    
                    # Additional validation: check if result is empty or invalid
                    elif hasattr(result, '__len__') and len(result) == 0:
                        raise Exception("Received empty response")
                    
                    # Only cache the result if we've successfully validated it
                    else:
                         # Cache the result with current timestamp
                        self._logger.debug(f"Caching result for meter {meter_id}, register 0x{register:04X}, value: {result}")
                        self._response_cache[cache_key] = (result, time.time())
                        self._last_request_time = time.time()
                    
                    #self._logger.debug(f"Successfully read meter {meter_id}, register 0x{register:04X}: {len(result) if hasattr(result, '__len__') else 'scalar'} values")
                    return result
                    
                except Exception as e:
                    last_exception = e
                    error_msg = str(e)
                                        
                    # Check for specific error conditions that indicate communication mix-ups
                    if "Invalid unit id" in error_msg:
                        self._logger.warning(f"Communication mix-up detected for meter {meter_id}, register 0x{register:04X} (attempt {attempt + 1}): {e}")                       
                        # Clear any potentially corrupted cache entries for this meter
                        self._clear_cache_for_meter(meter_id)
                        # Extended delay for mix-up recovery
                        time.sleep(0.3)

                    elif "Exception code = 11" in error_msg:
                        self._logger.warning(f"Communication timeout for meter {meter_id}, register 0x{register:04X} (attempt {attempt + 1}): {e}")                       
                        
                    else:
                        self._logger.warning(f"Modbus read failed for meter {meter_id}, register 0x{register:04X} (attempt {attempt + 1}): {e}")
                    
                    if attempt < self._retry_attempts - 1:
                        # Wait before retry, with exponential backoff
                        retry_delay = 0.1 * (2 ** attempt)
                        time.sleep(retry_delay)
            
            # All retry attempts failed
            self._logger.error(f"All {self._retry_attempts} attempts failed for meter {meter_id}, register 0x{register:04X}: {last_exception}")
            raise last_exception
    
    def cleanup_cache(self):
        """Remove expired cache entries"""
        current_time = time.time()
        with self._lock:
            expired_keys = [
                key for key, (_, timestamp) in self._response_cache.items()
                if current_time - timestamp > self._cache_timeout
            ]
            for key in expired_keys:
                del self._response_cache[key]
    
    def _wait_for_bus_ready(self, meter_id: int):
        """
        Wait for the Modbus bus to be ready for the next request.
        Implements device-specific delays to prevent communication mix-ups.
        """
        current_time = time.time()
        
        # Calculate required delay based on device type
        required_delay = self._device_delays.get(meter_id, self._inter_request_delay)
        
        # Calculate time since last request
        time_since_last = current_time - self._last_request_time
        
        # Wait if needed
        if time_since_last < required_delay:
            sleep_time = required_delay - time_since_last
            #self._logger.debug(f"Waiting {sleep_time:.3f}s for bus ready (meter {meter_id})")
            time.sleep(sleep_time)
    
    def _clear_cache_for_meter(self, meter_id: int):
        """Clear all cached responses for a specific meter"""
        keys_to_remove = [
            key for key in self._response_cache.keys()
            if key[0] == meter_id  # meter_id is the first element of the cache key tuple
        ]
        for key in keys_to_remove:
            try:
                del self._response_cache[key]
            except KeyError:
                # Key might have been removed by another thread, ignore
                pass
        
        if keys_to_remove:
            self._logger.debug(f"Cleared {len(keys_to_remove)} cache entries for meter {meter_id}")
        else:
            self._logger.debug(f"No cache entries found to clear for meter {meter_id}")
    
    def clear_all_cache(self):
        """Clear all cached responses (useful for communication recovery)"""
        cache_size = len(self._response_cache)
        self._response_cache.clear()
        if cache_size > 0:
            self._logger.info(f"Cleared all {cache_size} cache entries for communication recovery")
    
    def configure_device_delays(self, device_delays: Dict[int, float]):
        """
        Configure custom delays for specific device IDs.
        
        Args:
            device_delays: Dictionary mapping device IDs to delay times in seconds
        """
        self._device_delays.update(device_delays)
        self._logger.info(f"Updated device delays: {self._device_delays}")
    
    def set_inter_request_delay(self, delay: float):
        """Set the default delay between requests"""
        self._inter_request_delay = delay
        self._logger.info(f"Set inter-request delay to {delay}s")
 
# Global coordinator instance
_coordinator = None

def get_coordinator():
    """Get the global Modbus coordinator instance"""
    global _coordinator
    return _coordinator

def initialize_coordinator(modbus_master):
    """Initialize the global Modbus coordinator"""
    global _coordinator
    _coordinator = ModbusCoordinator(modbus_master)
    return _coordinator
