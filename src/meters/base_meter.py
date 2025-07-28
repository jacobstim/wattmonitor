"""
Base meter class that uses the centralized Modbus coordinator
"""

import sys
import os

# Add the src directory to the path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from .data_types import RegisterConfig, BatchRegisterConfig


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

    def read_all_measurements(self):
        """
        Read all supported measurements for this meter using batch operations where possible.
        This method optimizes Modbus communication by grouping registers into batches.
        
        Returns:
            Dict[str, Any]: Dictionary mapping measurement names to their values
        """
        if self._coordinator is None:
            raise RuntimeError("Modbus coordinator not initialized. Call initialize_coordinator() first.")
        
        results = {}
        
        # Check if the meter defines batch register configurations
        if hasattr(self, 'BATCH_REGISTER_CONFIGS') and self.BATCH_REGISTER_CONFIGS:
            # Use batch reading for efficiency
            for batch_name, batch_config in self.BATCH_REGISTER_CONFIGS.items():
                try:
                    # Perform batch read
                    batch_result = self._coordinator.read_batch_registers(
                        self._address, 
                        batch_config, 
                        self.REGISTER_CONFIGS
                    )
                    
                    # Extract individual measurements from the batch
                    for measurement_name in batch_config.measurements:
                        try:
                            key = measurement_name.value
                            value = batch_result.get_measurement(measurement_name)
                            
                            # Validate that we got a scalar value, not a list
                            if isinstance(value, list):
                                import logging
                                logger = logging.getLogger(__name__)
                                logger.error(f"Unexpected list value for {measurement_name}: {value}")
                                logger.error("This indicates a data conversion error in BatchReadResult.get_measurement()")
                                # Skip this measurement to avoid downstream errors
                                continue
                                
                            results[key] = value
                        except Exception as e:
                            # Log but continue with other measurements
                            import logging
                            logger = logging.getLogger(__name__)
                            logger.warning(f"Failed to extract {measurement_name} from batch {batch_name}: {e}")
                
                except Exception as e:
                    # If batch read fails, fall back to individual reads for this batch
                    import logging
                    logger = logging.getLogger(__name__)
                    logger.warning(f"Batch read failed for {batch_name}, falling back to individual reads: {e}")
                    
                    # Fall back to individual reads for measurements in this batch
                    for measurement_name in batch_config.measurements:
                        key = measurement_name.value
                        
                        if measurement_name in self.REGISTER_CONFIGS:
                            try:
                                results[key] = self._read_register_config(self.REGISTER_CONFIGS[measurement_name])
                            except Exception as individual_e:
                                logger.warning(f"Individual read also failed for {key}: {individual_e}")
        
        # For any measurements not covered by batches, read individually
        if hasattr(self, 'REGISTER_CONFIGS'):
            for measurement_name, config in self.REGISTER_CONFIGS.items():
                key = measurement_name.value
                    
                if key not in results:
                    try:
                        results[key] = self._read_register_config(config)
                    except Exception as e:
                        import logging
                        logger = logging.getLogger(__name__)
                        logger.warning(f"Individual read failed for {key}: {e}")
        
        return results
