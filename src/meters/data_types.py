"""
Abstract data types for meter register reading.
These types abstract away the underlying Modbus implementation details.
"""

from enum import Enum
from typing import NamedTuple, List, Dict, Any, Union
from .measurements import MeasurementType


class DataType(Enum):
    """Abstract data types for register values"""
    
    # Integer types
    UINT16 = "uint16"           # 16-bit unsigned integer (1 register)
    UINT32 = "uint32"           # 32-bit unsigned integer (2 registers)
    UINT64 = "uint64"           # 64-bit unsigned integer (4 registers)
    INT16 = "int16"             # 16-bit signed integer (1 register)
    INT32 = "int32"             # 32-bit signed integer (2 registers)
    INT64 = "int64"             # 64-bit signed integer (4 registers)
    
    # Floating point types
    FLOAT32 = "float32"         # 32-bit IEEE 754 float (2 registers)
    FLOAT64 = "float64"         # 64-bit IEEE 754 float (4 registers)
    
    # String types
    STRING = "string"           # ASCII string (variable length)
    
    # Raw register access
    RAW_REGISTERS = "raw"       # Raw register values as list


class RegisterConfig(NamedTuple):
    """Configuration for a register read operation"""
    register: int               # Starting register address
    count: int                  # Number of registers to read
    data_type: DataType         # Abstract data type
    byte_order: str = "big"     # "big" or "little" endian
    word_order: str = "big"     # "big" or "little" endian for multi-register values


class BatchRegisterConfig(NamedTuple):
    """Configuration for a batch of register reads within a larger range"""
    start_register: int         # Starting register address of the batch
    total_count: int           # Total number of registers to read in one operation
    measurements: Dict[MeasurementType, int]  # Map measurement types to their offset within the batch
    description: str = ""       # Optional description of the batch


class BatchReadResult:
    """Result of a batch register read operation"""
    
    def __init__(self, raw_registers: List[int], batch_config: BatchRegisterConfig, register_configs: Dict[MeasurementType, RegisterConfig]):
        self.raw_registers = raw_registers
        self.batch_config = batch_config
        self.register_configs = register_configs
        self._parsed_values = {}
    
    def get_measurement(self, measurement_name: MeasurementType) -> Any:
        """Extract a specific measurement from the batch read result"""
        # Use the enum value as the cache key
        measurement_key = measurement_name.value
        
        if measurement_key in self._parsed_values:
            return self._parsed_values[measurement_key]
        
        # Check if measurement exists in batch
        if measurement_name not in self.batch_config.measurements:
            raise ValueError(f"Measurement {measurement_name} not found in batch")
        
        offset = self.batch_config.measurements[measurement_name]
        
        # Find the register configuration
        if measurement_name not in self.register_configs:
            raise ValueError(f"Register configuration for {measurement_name} not found")
        
        config = self.register_configs[measurement_name]
        
        # Extract the registers for this measurement
        measurement_registers = self.raw_registers[offset:offset + config.count]
        
        # Convert using inline conversion logic to avoid circular import
        parsed_value = self._convert_to_datatype_inline(measurement_registers, config)
        
        # Cache the parsed value using the enum value as key
        self._parsed_values[measurement_key] = parsed_value
        return parsed_value
        
        # Extract the registers for this measurement
        measurement_registers = self.raw_registers[offset:offset + config.count]
        
        # Convert using inline conversion logic to avoid circular import
        parsed_value = self._convert_to_datatype_inline(measurement_registers, config)
        
        # Cache the parsed value using the enum value as key
        self._parsed_values[measurement_key] = parsed_value
        return parsed_value

    def _convert_to_datatype_inline(self, raw_registers: List[int], config: RegisterConfig) -> Any:
        """Inline conversion logic to avoid circular import"""
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
            # Log the error instead of silently returning raw registers (which would cause type errors downstream)
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to convert registers {raw_registers} to {config.data_type}: {e}")
            logger.error(f"Register config: addr=0x{config.register:04X}, count={config.count}, word_order={config.word_order}")
            # Return 0 as a safe fallback instead of raw_registers (which is a list)
            return 0.0

