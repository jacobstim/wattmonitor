"""
Abstract data types for meter register reading.
These types abstract away the underlying Modbus implementation details.
"""

from enum import Enum
from typing import NamedTuple


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

