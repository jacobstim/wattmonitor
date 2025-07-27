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
        self._cache_timeout = 1.0  # Cache responses for 1 second
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
        
        # Buffer management settings
        self._buffer_flush_on_error = True  # Flush buffers when errors occur
        self._connection_recovery_delay = 1.0  # Delay for connection recovery
        self._flush_delay = 0.1  # Delay after buffer flush operations
        
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
                            meter_id, cst.READ_HOLDING_REGISTERS, register, size, data_format=datatype
                        )
                    else:
                        result = self.modbus_master.execute(
                            meter_id, cst.READ_HOLDING_REGISTERS, register, size
                        )
                    
                    # Validate the response - check if we got a reasonable result
                    if result is None:
                        raise Exception("Received null response")
                    
                    # Additional validation: check if result is empty or invalid
                    if hasattr(result, '__len__') and len(result) == 0:
                        raise Exception("Received empty response")
                    
                    # Only cache the result if we've successfully validated it
                    self._response_cache[cache_key] = (result, time.time())
                    self._last_request_time = time.time()
                    
                    self._logger.debug(f"Successfully read meter {meter_id}, register 0x{register:04X}: {len(result) if hasattr(result, '__len__') else 'scalar'} values")
                    return result
                    
                except Exception as e:
                    last_exception = e
                    error_msg = str(e)
                    
                    # Ensure we don't cache failed results by removing any existing cache entry
                    if cache_key in self._response_cache:
                        del self._response_cache[cache_key]
                        self._logger.debug(f"Removed potentially corrupted cache entry for meter {meter_id}, register 0x{register:04X}")
                    
                    # Check for specific error conditions that indicate communication mix-ups
                    if "Invalid unit id" in error_msg or "Exception code = 11" in error_msg:
                        self._logger.warning(f"Communication mix-up detected for meter {meter_id}, register 0x{register:04X} (attempt {attempt + 1}): {e}")
                        
                        # Aggressive recovery for communication mix-ups
                        if attempt == 0:
                            # First attempt: try socket buffer flush
                            self.flush_socket_buffers()
                        elif attempt == 1:
                            # Second attempt: full buffer flush
                            self.flush_modbus_buffers()
                        
                        # Clear any potentially corrupted cache entries for this meter
                        self._clear_cache_for_meter(meter_id)
                        
                        # Extended delay for mix-up recovery
                        time.sleep(0.3)
                        
                    elif "timeout" in error_msg.lower() or "connection" in error_msg.lower():
                        self._logger.warning(f"Connection issue detected for meter {meter_id}, register 0x{register:04X} (attempt {attempt + 1}): {e}")
                        
                        # For connection issues, try buffer management
                        if attempt < self._retry_attempts - 1:
                            if self.preventive_buffer_check():
                                # Buffer cleanup was performed
                                time.sleep(0.1)
                            else:
                                # Try TCP optimizations
                                self.configure_tcp_optimizations()
                        
                    else:
                        self._logger.warning(f"Modbus read failed for meter {meter_id}, register 0x{register:04X} (attempt {attempt + 1}): {e}")
                    
                    if attempt < self._retry_attempts - 1:
                        # Wait before retry, with exponential backoff
                        retry_delay = 0.1 * (2 ** attempt)
                        time.sleep(retry_delay)
            
            # All retry attempts failed
            self._logger.error(f"All {self._retry_attempts} attempts failed for meter {meter_id}, register 0x{register:04X}: {last_exception}")
            raise last_exception
    
    def batch_read_meters(self, meter_requests: List[Tuple[object, str]]) -> Dict[object, Dict[str, Any]]:
        """
        Perform batch reading of multiple meters in a single thread.
        
        Args:
            meter_requests: List of (meter_object, method_name) tuples
            
        Returns:
            Dictionary mapping meter objects to their measurement results
        """
        results = {}
        
        with self._lock:
            for meter, method_name in meter_requests:
                try:
                    # Call the measurement method on the meter
                    method = getattr(meter, method_name)
                    value = method()
                    
                    if meter not in results:
                        results[meter] = {}
                    results[meter][method_name] = value
                    
                except Exception as e:
                    self._logger.error(f"Failed to read {method_name} from meter {meter.modbus_id()}: {e}")
                    if meter not in results:
                        results[meter] = {}
                    results[meter][method_name] = None
        
        return results
    
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
    
    def flush_modbus_buffers(self):
        """
        Flush Modbus TCP connection buffers to clear any lingering data.
        This helps prevent communication mix-ups from stale buffer content.
        """
        try:
            # Strategy 1: TCP-specific buffer flushing before connection reset
            if hasattr(self.modbus_master, '_sock') and self.modbus_master._sock:
                self._logger.info("Flushing Modbus TCP buffers by connection reset")
                
                # First, try to flush any pending data in both directions
                sock = self.modbus_master._sock
                
                # Flush OS-level send buffers
                try:
                    import socket
                    # Use TCP_NODELAY to force immediate send of any buffered data
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    # Brief delay to allow any pending sends to complete
                    time.sleep(0.01)
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 0)
                except Exception as e:
                    self._logger.debug(f"TCP send buffer flush failed: {e}")
                
                # Drain any lingering receive buffers before closing
                self._drain_receive_buffer(sock, max_attempts=5, timeout=0.05)
                
                # Close the current connection
                self.modbus_master.close()
                
                # Extended delay to ensure complete socket cleanup and OS buffer flush
                time.sleep(max(self._flush_delay, 0.2))
                
                # Reopen the connection
                self.modbus_master.open()
                self._logger.info("Modbus TCP connection reset completed")
                
                # Clear all cache since connection state changed
                self.clear_all_cache()
                
            else:
                self._logger.warning("No active Modbus connection to flush")
                
        except Exception as e:
            self._logger.error(f"Failed to flush Modbus buffers: {e}")
            # Try to ensure connection is in a known state
            try:
                self.modbus_master.close()
                time.sleep(self._connection_recovery_delay)
                self.modbus_master.open()
            except Exception as recovery_error:
                self._logger.error(f"Connection recovery failed: {recovery_error}")
    
    def flush_socket_buffers(self):
        """
        Attempt to flush socket-level buffers without closing connection.
        Less disruptive than full connection reset but more thorough than basic flush.
        """
        try:
            if hasattr(self.modbus_master, '_sock') and self.modbus_master._sock:
                sock = self.modbus_master._sock
                
                self._logger.debug("Attempting non-disruptive socket buffer flush")
                
                # Drain receive buffers more aggressively
                flushed_bytes = self._drain_receive_buffer(sock, max_attempts=10, timeout=0.1)
                
                # Force TCP layer to process any pending data
                try:
                    import socket
                    # Brief TCP_NODELAY toggle to force buffer processing
                    original_nodelay = sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY)
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    time.sleep(0.01)  # Allow TCP stack to process
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, original_nodelay)
                except Exception as e:
                    self._logger.debug(f"TCP buffer processing failed: {e}")
                
                if flushed_bytes > 0:
                    self._logger.warning(f"Flushed {flushed_bytes} bytes of lingering data from socket")
                    # Clear cache since we removed potentially relevant data
                    self.clear_all_cache()
                else:
                    self._logger.debug("No lingering data found in socket buffers")
                    
        except Exception as e:
            self._logger.warning(f"Socket buffer flush failed: {e}")
    
    def recovery_sequence(self):
        """
        Execute a comprehensive recovery sequence for communication issues.
        Combines cache clearing, buffer flushing, and connection reset.
        """
        self._logger.info("Starting Modbus communication recovery sequence")
        
        # Step 1: Clear all cached data
        self.clear_all_cache()
        
        # Step 2: Attempt socket buffer flush (less disruptive)
        self.flush_socket_buffers()
        
        # Step 3: Brief delay for system recovery
        time.sleep(self._flush_delay)
        
        # Step 4: If problems persist, full connection reset
        if self._buffer_flush_on_error:
            self.flush_modbus_buffers()
        
        self._logger.info("Modbus communication recovery sequence completed")
        
        # Reset timing to prevent immediate requests
        self._last_request_time = time.time()
    
    def _drain_receive_buffer(self, sock, max_attempts=10, timeout=0.05):
        """
        Aggressively drain socket receive buffer to remove stale data.
        
        Args:
            sock: Socket to drain
            max_attempts: Maximum number of receive attempts
            timeout: Timeout per receive attempt
            
        Returns:
            Total bytes drained
        """
        original_timeout = sock.gettimeout()
        flushed_bytes = 0
        
        try:
            sock.settimeout(timeout)
            
            for attempt in range(max_attempts):
                try:
                    data = sock.recv(1024)
                    if not data:
                        break  # No more data
                    flushed_bytes += len(data)
                    
                    # Safety check to prevent excessive draining
                    if flushed_bytes > 8192:  # 8KB limit
                        self._logger.warning(f"Stopped draining after {flushed_bytes} bytes to prevent overflow")
                        break
                        
                except Exception:
                    # Expected when no more data available
                    break
                    
        finally:
            # Always restore original timeout
            sock.settimeout(original_timeout)
            
        return flushed_bytes
    
    def preventive_buffer_check(self):
        """
        Perform preventive check for lingering data in buffers.
        Call this periodically or before critical operations.
        """
        try:
            if hasattr(self.modbus_master, '_sock') and self.modbus_master._sock:
                sock = self.modbus_master._sock
                
                # Quick check for available data (non-blocking)
                original_timeout = sock.gettimeout()
                sock.settimeout(0.001)  # Very short timeout
                
                try:
                    import socket
                    data = sock.recv(1, socket.MSG_PEEK)  # Peek without removing
                    if data:
                        self._logger.warning("Unexpected data detected in receive buffer, performing cleanup")
                        self.flush_socket_buffers()
                        return True
                except:
                    # No data available - this is good
                    pass
                finally:
                    sock.settimeout(original_timeout)
                    
        except Exception as e:
            self._logger.debug(f"Preventive buffer check failed: {e}")
            
        return False
    
    def configure_tcp_optimizations(self):
        """
        Configure TCP socket optimizations for Modbus communication.
        Call this after connection establishment.
        """
        try:
            if hasattr(self.modbus_master, '_sock') and self.modbus_master._sock:
                sock = self.modbus_master._sock
                import socket
                
                # Enable TCP keep-alive to detect dead connections
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                
                # Reduce TCP buffer sizes to minimize buffering delays
                try:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)  # 4KB receive buffer
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)  # 4KB send buffer
                except:
                    pass  # Some systems don't allow buffer size changes
                
                # Enable TCP_NODELAY for immediate transmission (disable Nagle's algorithm)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                
                self._logger.info("Applied TCP optimizations for Modbus communication")
                
        except Exception as e:
            self._logger.warning(f"Failed to apply TCP optimizations: {e}")
    
    def get_buffer_status(self):
        """
        Get current buffer status information for diagnostics.
        
        Returns:
            Dictionary with buffer status information
        """
        status = {
            'cache_entries': len(self._response_cache),
            'last_request_time': self._last_request_time,
            'connection_active': False,
            'socket_info': {}
        }
        
        try:
            if hasattr(self.modbus_master, '_sock') and self.modbus_master._sock:
                sock = self.modbus_master._sock
                status['connection_active'] = True
                
                # Try to get socket buffer information
                try:
                    import socket
                    status['socket_info'] = {
                        'timeout': sock.gettimeout(),
                        'family': sock.family.name if hasattr(sock.family, 'name') else str(sock.family),
                        'type': sock.type.name if hasattr(sock.type, 'name') else str(sock.type),
                    }
                    
                    # Check if there's data waiting (Linux/Unix specific)
                    try:
                        import fcntl
                        import struct
                        # FIONREAD ioctl to check bytes available for reading
                        available_bytes = struct.unpack('I', fcntl.ioctl(sock.fileno(), 0x541B, b'\x00\x00\x00\x00'))[0]
                        status['socket_info']['bytes_available'] = available_bytes
                    except:
                        pass  # Not available on all platforms
                        
                except Exception as e:
                    status['socket_info']['error'] = str(e)
                    
        except Exception as e:
            status['error'] = str(e)
            
        return status


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
