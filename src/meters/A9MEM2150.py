import modbus_tk.defines as cst
from datetime import datetime
import struct

class iMEM2150:
    
    """
    This class implements the Schneider Electric iM3155 meter values
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

#################################################################################################
### Module functions
#################################################################################################

    # Phases support
    def has_L1(self):
        return True
    def has_L2(self):
        return False
    def has_L3(self):
        return False
    def has_threephase(self):
        return False

#################################################################################################
### SYSTEM functions
#################################################################################################

    def sys_metername(self):
        result = self._readregister(0x001D, 20)
        return ''.join(map(chr, struct.pack('>' + 'H' * len(result), *result)))

    def sys_metermodel(self):
        result = self._readregister(0x0031, 20)
        return ''.join(map(chr, struct.pack('>' + 'H' * len(result), *result)))

    def sys_manufacturer(self):
        result = self._readregister(0x0045, 20)
        return ''.join(map(chr, struct.pack('>' + 'H' * len(result), *result)))

    def sys_serialnumber(self):
        return self._readregister(0x0081, 2, '>L')[0]

    def sys_manufacturedate(self):
        """
        Queries the meter for its manufacturing date

        :return: Manufacturing date of the energy meter as a datetime object
        """
        mdate = self._readregister(132, 4, '>HHHH')
        return self._decodetime(mdate)

#################################################################################################
### METER DATA functions
#################################################################################################

    def md_current_L1(self):
        return (self._readregister(0x0BB7, 2, '>f'))[0]

    def md_voltage_L1_N(self):
        return (self._readregister(0x0BD3, 2, '>f'))[0]

    def md_current(self):           
        return self.md_current_L1()

    def md_voltage(self):   
        return self.md_voltage_L1_N()

    def md_power_L1(self):
        return (self._readregister(0x0BED, 2, '>f'))[0]*1000

    def md_power(self):
        return self.md_power_L1()

    def md_power_reactive(self):
        return (self._readregister(0x0BFB, 2, '>f'))[0]

    def md_power_apparent(self):
        return (self._readregister(0x0C03, 2, '>f'))[0]

    def md_powerfactor(self):
        return (self._readregister(0x0C0B, 2, '>f'))[0]

    def md_frequency(self):
        return (self._readregister(0x0C25, 2, '>f'))[0]

#################################################################################################
### ENERGY DATA functions
#################################################################################################

    def ed_total(self):
        """
        Retrieve total Active Energy import

        :return: Energy in kWh (kWatt-hour)
        """
        return (self._readregister(0xB02B, 2, '>f'))[0]

    def ed_total_export(self):
        """
        Retrieve total Active Energy export

        :return: Energy in kWh (kWatt-hour)
        """
        return (self._readregister(0xB02D, 2, '>f'))[0]

    def ed_total_reactive_import(self):
        """
        Retrieve total Reactive Energy import

        :return: Energy in kVARh (kVolt-Amper(Reactive)-hour)
        """
        return (self._readregister(0xB02F, 2, '>f'))[0]

    def ed_total_reactive_export(self):
        """
        Retrieve total Reactive Energy export

        :return: Energy in kVARh (kVolt-Amper(Reactive)-hour)
        """
        return (self._readregister(0xB031, 2, '>f'))[0]


#################################################################################################
### Internal functions
#################################################################################################

    def _readregister(self, register, size, datatype=""):
        if len(datatype)>0:
            return self._modbus.execute(self._address, cst.READ_HOLDING_REGISTERS, register, size, data_format=datatype)
        else:
            return self._modbus.execute(self._address, cst.READ_HOLDING_REGISTERS, register, size)

    def _decodetime(self, timestamp):
        """
        Decodes a Schneider Electric iEM datestamp (see manual for definition)

        :param timestamp: The four 16-bit words that describe the SE date
        :return: datetime, the converted date & timestamp to a Python datetime object
        """
        # WORD 1 - Lower 6 bits are YEAR (from 2000)
        year = 2000 + (timestamp[0] & 0b00111111)
        # WORD 2 - Lower 5 bits are DAY, lower 4 bits of upper byte are MONTH
        month = (timestamp[1] >> 8) & 0b00001111
        day = (timestamp[1] & 0b00011111)
        # WORD 3 - Upper byte is the hour (5 bits), lower byte is the minutes (6 bits)
        hour = (timestamp[2] >> 8) & 0b00011111
        minute = (timestamp[2] & 0b00111111)
        # WORD 4 - Milliseconds
        second = timestamp[2] // 1000
        microsecond = (timestamp[2] % 1000) * 1000
        return datetime(year, month, day, hour, minute, second, microsecond)
