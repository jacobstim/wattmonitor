from enum import Enum
from datetime import datetime
import struct
import modbus_tk.defines as cst
from .measurements import MeasurementType

class CSMB:
    
    """
    This class implements the Xemex CSMB meter values
    """

    def __init__(self, modbus, address=1):
        # Construct 
        self._modbus = modbus
        self._address = address

#    def __del__(self):
#        self.close()

#    def open(self):
#        """open the communication with the slave"""
#        if not self._is_opened:
#            self._do_open()
#            self._is_opened = True

    # Retrieve Modbus ID
    def modbus_id(self):
        return self._address

#################################################################################################
### Module functions
#################################################################################################

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
        return (self._readregister(0x4000, 2))

#################################################################################################
### METER DATA functions
#################################################################################################

    def md_current_L1(self):
        return (self._readregister(0x500C, 2, '>f'))[0]

    def md_current_L2(self):
        return (self._readregister(0x500E, 2, '>f'))[0]

    def md_current_L3(self):
        return (self._readregister(0x5010, 2, '>f'))[0]

    def md_current(self):           # Average current
        current_L1 = self.md_current_L1()
        current_L2 = self.md_current_L2()
        current_L3 = self.md_current_L3()
        return (current_L1 + current_L2 + current_L3) / 3.0

#################################################################################################
### Internal functions
#################################################################################################

    def _readregister(self, register, size, datatype=""):
        if len(datatype)>0:
            return self._modbus.execute(self._address, cst.READ_HOLDING_REGISTERS, register, size, data_format=datatype)
        else:
            return self._modbus.execute(self._address, cst.READ_HOLDING_REGISTERS, register, size)
