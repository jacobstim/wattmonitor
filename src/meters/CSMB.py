from enum import Enum
from datetime import datetime
import struct
from .measurements import MeasurementType
from .base_meter import BaseMeter
from .data_types import DataType, RegisterConfig

class CSMB(BaseMeter):
    
    """
    This class implements the Xemex CSMB meter values
    """
    
    # Register configuration constants for better maintainability
    REGISTER_CONFIGS = {
        # System registers
        'serialnumber': RegisterConfig(0x4000, 2, DataType.RAW_REGISTERS),      # DataType.UINT32 ???
        
        # Current measurement registers
        'current_l1': RegisterConfig(0x500C, 2, DataType.FLOAT32),
        'current_l2': RegisterConfig(0x500E, 2, DataType.FLOAT32),
        'current_l3': RegisterConfig(0x5010, 2, DataType.FLOAT32),
    }

    def __init__(self, modbus, address=1):
        # Construct using the base meter
        super().__init__(modbus, address)

    # Phases support
    def has_L1(self):
        return True
    def has_L2(self):
        return True
    def has_L3(self):
        return True
    def has_threephase(self):
        return True

    def supported_measurements(self):
        measurements = []
        measurements.append(MeasurementType.CURRENT)
        if self.has_L1() and self.has_threephase():
            measurements.append(MeasurementType.CURRENT_L1)
        if self.has_L2():
            measurements.append(MeasurementType.CURRENT_L2)
        if self.has_L3():
            measurements.append(MeasurementType.CURRENT_L3)
        return measurements

#################################################################################################
### SYSTEM functions
#################################################################################################

    def sys_metername(self):
        return 'CSMB'

    def sys_metermodel(self):
        return 'CSMB'

    def sys_manufacturer(self):
        return 'Xemex'

    def sys_serialnumber(self):
        return self._read_register_config(self.REGISTER_CONFIGS['serialnumber'])

#################################################################################################
### METER DATA functions
#################################################################################################

    def md_current_L1(self):
        return self._read_register_config(self.REGISTER_CONFIGS['current_l1'])

    def md_current_L2(self):
        return self._read_register_config(self.REGISTER_CONFIGS['current_l2'])

    def md_current_L3(self):
        return self._read_register_config(self.REGISTER_CONFIGS['current_l3'])

    def md_current(self):           # Average current
        current_L1 = self.md_current_L1()
        current_L2 = self.md_current_L2()
        current_L3 = self.md_current_L3()
        return (current_L1 + current_L2 + current_L3) / 3.0
