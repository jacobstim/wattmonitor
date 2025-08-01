"""
Modbus Coordinator - Centralized, single-threaded Modbus communication
This module manages all Modbus communication to prevent race conditions
while maintaining the modularity of individual meter definitions.
"""

import threading
import logging
import socket
from typing import Dict, List, Tuple, Any
import time

# Import abstract data types
from meters.data_types import DataType, RegisterConfig, BatchRegisterConfig, BatchReadResult
from meters.measurements import MeasurementType
# Import the new modbus abstraction layer
from . import ModbusClientInterface, ModbusException


class ModbusTCPConnectionError(Exception):
    """Exception raised when TCP connection to Modbus server is lost"""
    pass


class ModbusCoordinator:
    """
    Centralized Modbus communication coordinator.
    Ensures all Modbus requests are executed sequentially in a single thread.
    """
    
    def __init__(self, modbus_client: ModbusClientInterface):
        self.modbus_client = modbus_client
        self._request_queue = []
        self._response_cache = {}  # Cache responses for a short time to avoid duplicate reads
        self._cache_timeout = 3.0  # Cache responses for 3 seconds
        self._lock = threading.Lock()
        self._logger = logging.getLogger(__name__)
        
        # Add inter-request delays to prevent communication mix-ups
        self._inter_request_delay = 0.05  # 50ms default delay between requests
        self._device_delays = {}  # Custom delays for specific device IDs}
        self._last_request_time = 0
        self._retry_attempts = 3  # Number of retry attempts for failed requests
        
    def _is_tcp_connection_error(self, exception: Exception) -> bool:
        """Check if the exception indicates a TCP connection failure"""
        # Check for standard network/socket exceptions
        if isinstance(exception, (
            ConnectionResetError,        # errno 104 on Unix, WinError 10054 on Windows
            ConnectionRefusedError,      # errno 111 on Unix, WinError 10061 on Windows  
            ConnectionAbortedError,      # errno 103 on Unix, WinError 10053 on Windows
            TimeoutError,               # errno 110 on Unix, WinError 10060 on Windows
            OSError,                    # General OS errors including network issues
            socket.error,               # Socket-specific errors
            BrokenPipeError             # errno 32 on Unix, WinError 109 on Windows
        )):
            return True
        
        # Check for ModbusException indicating connection loss
        if exception.__class__.__name__ == 'ModbusException':
            error_msg = str(exception).lower()
            if "not connected" in error_msg:
                return True

        return False

        
    def read_register_config(self, meter_id: int, config: RegisterConfig) -> Any:
        """
        Read Modbus registers using abstract data type configuration.
        This is the preferred method for new meter implementations.
        
        Args:
            meter_id: Modbus slave ID of the meter
            config: RegisterConfig specifying what and how to read
            
        Returns:
            Decoded register data according to the specified data type
        """
        cache_key = (meter_id, config.register, config.count, config.data_type.value)
        
        with self._lock:
            # Check cache first
            if cache_key in self._response_cache:
                cached_result, timestamp = self._response_cache[cache_key]
                if time.time() - timestamp < self._cache_timeout:
                    self._logger.debug(f"Cache hit for meter {meter_id}, register {config.register}")
                    return cached_result
                else:
                    del self._response_cache[cache_key]

            # Check if Modbus client is connected
            if not self.modbus_client.is_connected():
                self._logger.debug(f"Modbus client not connected. Skipping read_register_config for meter {meter_id}.")
                raise ModbusTCPConnectionError("Modbus client is not connected")

            # Implement inter-request delay
            self._wait_for_bus_ready(meter_id)
            
            # Retry logic
            last_exception = None
            for attempt in range(self._retry_attempts):
                try:
                    self._logger.debug(f"Reading meter {meter_id}, register 0x{config.register:04X}, size {config.count}, type {config.data_type.value} (attempt {attempt + 1})")
                    
                    # Execute the Modbus read using the configured implementation
                    result = self._execute_modbus_read(meter_id, config.register, config.count)
                    
                    # Validate response
                    if result is None:
                        raise Exception("Received null response")
                    elif hasattr(result, '__len__') and len(result) == 0:
                        raise Exception("Received empty response")
                    
                    # Convert to abstract data type
                    converted_result = self._convert_to_datatype(result, config)
                    
                    # Cache successful result
                    self._response_cache[cache_key] = (converted_result, time.time())
                    self._last_request_time = time.time()
                    
                    return converted_result
                    
                except Exception as e:
                    last_exception = e
                    error_msg = str(e)
                    
                    # Check for TCP connection failures first - no retries for these
                    if self._is_tcp_connection_error(e):
                        self._logger.error(f"TCP connection failure detected for meter {meter_id}, register 0x{config.register:04X}: {e}")
                        self.clear_all_cache()  # Clear cache since connection is broken
                        raise ModbusTCPConnectionError(f"TCP connection lost: {error_msg}") from e
                                                            
                    # Check for specific error conditions
                    if "Invalid unit id" in error_msg:
                        self._logger.warning(f"Communication mix-up detected for meter {meter_id}, register 0x{config.register:04X} (attempt {attempt + 1}): {e}")                       
                        self._clear_cache_for_meter(meter_id)
                        time.sleep(0.3)
                    elif "Exception code = 11" in error_msg:
                        self._logger.warning(f"Communication timeout for meter {meter_id}, register 0x{config.register:04X} (attempt {attempt + 1}): {e}")                       
                    else:
                        self._logger.warning(f"Modbus read failed for meter {meter_id}, register 0x{config.register:04X} (attempt {attempt + 1}): {e}")
                    
                    if attempt < self._retry_attempts - 1:
                        retry_delay = 0.1 * (2 ** attempt)
                        time.sleep(retry_delay)
            
            # All attempts failed
            self._logger.error(f"All {self._retry_attempts} attempts failed for meter {meter_id}, register 0x{config.register:04X}: {last_exception}")
            raise last_exception

    def read_batch_registers(self, meter_id: int, batch_config: BatchRegisterConfig, register_configs: Dict[MeasurementType, RegisterConfig]) -> BatchReadResult:
        """
        Read a batch of Modbus registers in a single operation for efficiency.
        
        Args:
            meter_id: Modbus slave ID of the meter
            batch_config: BatchRegisterConfig specifying the contiguous range to read
            register_configs: Dictionary mapping measurement names to their RegisterConfig
            
        Returns:
            BatchReadResult containing the raw data and methods to extract individual measurements
        """
        cache_key = f"batch_{meter_id}_{batch_config.start_register}_{batch_config.total_count}"
        
        with self._lock:
            # Check cache first
            if cache_key in self._response_cache:
                cached_result, timestamp = self._response_cache[cache_key]
                if time.time() - timestamp < self._cache_timeout:
                    self._logger.debug(f"Batch cache hit for meter {meter_id}, registers {batch_config.start_register}-{batch_config.start_register + batch_config.total_count - 1}")
                    return cached_result
                else:
                    del self._response_cache[cache_key]

            # Check if Modbus client is connected
            if not self.modbus_client.is_connected():
                self._logger.debug(f"Modbus client not connected. Skipping read_batch_registers for meter {meter_id}.")
                raise ModbusTCPConnectionError("Modbus client is not connected")

            # Implement inter-request delay
            self._wait_for_bus_ready(meter_id)
            
            # Retry logic
            last_exception = None
            for attempt in range(self._retry_attempts):
                try:
                    self._logger.debug(f"Batch reading meter {meter_id}, registers 0x{batch_config.start_register:04X}-0x{batch_config.start_register + batch_config.total_count - 1:04X} ({batch_config.description}) (attempt {attempt + 1})")
                    
                    # Execute the Modbus read using the configured implementation
                    result = self._execute_modbus_read(meter_id, batch_config.start_register, batch_config.total_count)
                    
                    # Validate response
                    if result is None:
                        raise Exception("Received null response")
                    elif hasattr(result, '__len__') and len(result) != batch_config.total_count:
                        raise Exception(f"Expected {batch_config.total_count} registers, got {len(result)}")
                    
                    # Create batch result
                    batch_result = BatchReadResult(result, batch_config, register_configs)
                    
                    # Cache successful result
                    self._response_cache[cache_key] = (batch_result, time.time())
                    self._last_request_time = time.time()
                    
                    # Also cache individual measurements to avoid redundant reads
                    self._cache_individual_measurements_from_batch(meter_id, batch_result)
                    
                    return batch_result
                    
                except Exception as e:
                    last_exception = e
                    error_msg = str(e)
                    
                    # Check for TCP connection failures first - no retries for these
                    if self._is_tcp_connection_error(e):
                        self._logger.error(f"TCP connection failure detected for meter {meter_id}, batch read: {e}")
                        self.clear_all_cache()  # Clear cache since connection is broken
                        raise ModbusTCPConnectionError(f"TCP connection lost: {error_msg}") from e

                    # Check for specific error conditions
                    if "Invalid unit id" in error_msg:
                        self._logger.warning(f"Communication mix-up detected for meter {meter_id}, batch read (attempt {attempt + 1}): {e}")                       
                        self._clear_cache_for_meter(meter_id)
                        time.sleep(0.3)
                    elif "Exception code = 11" in error_msg:
                        self._logger.warning(f"Communication timeout for meter {meter_id}, batch read (attempt {attempt + 1}): {e}")                       
                    else:
                        self._logger.warning(f"Modbus batch read failed for meter {meter_id} (attempt {attempt + 1}): {e}")
                    
                    if attempt < self._retry_attempts - 1:
                        retry_delay = 0.1 * (2 ** attempt)
                        time.sleep(retry_delay)
            
            # All attempts failed
            self._logger.error(f"All {self._retry_attempts} attempts failed for meter {meter_id}, batch read: {last_exception}")
            raise last_exception
    
    def _cache_individual_measurements_from_batch(self, meter_id: int, batch_result: BatchReadResult):
        """Cache individual measurement values from a batch read to avoid redundant individual reads"""
        timestamp = time.time()
        
        for measurement_name in batch_result.batch_config.measurements:
            # Find corresponding register config
            if measurement_name in batch_result.register_configs:
                config = batch_result.register_configs[measurement_name]
                cache_key = (meter_id, config.register, config.count, config.data_type.value)
                
                # Extract and cache the individual measurement
                try:
                    value = batch_result.get_measurement(measurement_name)
                    self._response_cache[cache_key] = (value, timestamp)
                    self._logger.debug(f"Cached individual measurement {measurement_name.value} from batch read")
                except Exception as e:
                    self._logger.warning(f"Failed to cache individual measurement {measurement_name.value} from batch: {e}")
    
    
    def _execute_modbus_read(self, meter_id: int, register: int, count: int) -> List[int]:
        """
        Execute a Modbus holding register read using the abstracted client.
        
        Args:
            meter_id: Modbus slave ID
            register: Starting register address
            count: Number of registers to read
            
        Returns:
            List of register values
            
        Raises:
            Exception: On communication or protocol errors
        """
        return self.modbus_client.read_holding_registers(meter_id, register, count)
    
    def _convert_to_datatype(self, raw_registers: List[int], config: RegisterConfig) -> Any:
        """Convert raw register values to the specified abstract data type"""
        import struct
        
        if config.data_type == DataType.RAW_REGISTERS:
            return raw_registers
        
        # Handle string conversion
        if config.data_type == DataType.STRING:
            # Convert registers to string (each register = 2 bytes)
            byte_data = b''.join(struct.pack('>H', reg) for reg in raw_registers)
            # Remove null bytes and decode
            return byte_data.replace(b'\x00', b'').decode('ascii', errors='ignore').strip()
        
        # For numeric types, pack registers into bytes first
        if len(raw_registers) == 1:
            # Single register
            byte_data = struct.pack('>H', raw_registers[0])
        elif len(raw_registers) == 2:
            # Two registers - handle endianness
            if config.word_order == "big":
                byte_data = struct.pack('>HH', raw_registers[0], raw_registers[1])
            else:
                byte_data = struct.pack('>HH', raw_registers[1], raw_registers[0])
        elif len(raw_registers) == 4:
            # Four registers for 64-bit values
            if config.word_order == "big":
                byte_data = struct.pack('>HHHH', *raw_registers)
            else:
                byte_data = struct.pack('>HHHH', *reversed(raw_registers))
        else:
            raise ValueError(f"Unsupported register count for {config.data_type}: {len(raw_registers)}")
        
        # Convert based on data type
        try:
            if config.data_type == DataType.UINT16:
                return struct.unpack('>H', byte_data)[0]
            elif config.data_type == DataType.INT16:
                return struct.unpack('>h', byte_data)[0]
            elif config.data_type == DataType.UINT32:
                return struct.unpack('>I', byte_data)[0]
            elif config.data_type == DataType.INT32:
                return struct.unpack('>i', byte_data)[0]
            elif config.data_type == DataType.UINT64:
                return struct.unpack('>Q', byte_data)[0]
            elif config.data_type == DataType.INT64:
                return struct.unpack('>q', byte_data)[0]
            elif config.data_type == DataType.FLOAT32:
                return struct.unpack('>f', byte_data)[0]
            elif config.data_type == DataType.FLOAT64:
                return struct.unpack('>d', byte_data)[0]
            else:
                raise ValueError(f"Unknown data type: {config.data_type}")
                
        except struct.error as e:
            self._logger.error(f"Failed to convert registers {raw_registers} to {config.data_type}: {e}")
            # Fallback to raw registers
            return raw_registers
    
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
            self._logger.debug(f"Waiting {sleep_time:.3f}s for bus ready (meter {meter_id})")
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
            self._logger.debug(f"Cleared all {cache_size} cache entries for communication recovery")
    
    def configure_device_delays(self, device_delays: Dict[int, float]):
        """
        Configure custom delays for specific device IDs.
        
        Args:
            device_delays: Dictionary mapping device IDs to delay times in seconds
        """
        self._device_delays.update(device_delays)
        self._logger.debug(f"Updated device delays: {self._device_delays}")
    
    def set_inter_request_delay(self, delay: float):
        """Set the default delay between requests"""
        self._inter_request_delay = delay
        self._logger.debug(f"Set inter-request delay to {delay}s")
 
# Global coordinator instance
_coordinator = None

def get_coordinator():
    """Get the global Modbus coordinator instance"""
    global _coordinator
    return _coordinator

def initialize_coordinator(modbus_client: ModbusClientInterface):
    """Initialize the global Modbus coordinator"""
    global _coordinator
    _coordinator = ModbusCoordinator(modbus_client)
    return _coordinator
