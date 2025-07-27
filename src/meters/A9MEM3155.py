from enum import Enum
import modbus_tk.defines as cst
from datetime import datetime
import struct
from .measurements import MeasurementType
from .base_meter import BaseMeter

class iMEM3155(BaseMeter):
    
    """
    This class implements the Schneider Electric iM3155 meter values
    """

    def __init__(self, modbus, address=1):
        # Construct using the base meter
        super().__init__(modbus, address)

#    def __del__(self):
#        self.close()

#    def open(self):
#        """open the communication with the slave"""
#        if not self._is_opened:
#            self._do_open()
#            self._is_opened = True

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
        measurements.append(MeasurementType.VOLTAGE)
        measurements.append(MeasurementType.CURRENT)
        measurements.append(MeasurementType.POWER)
        measurements.append(MeasurementType.POWER_REACTIVE)        
        measurements.append(MeasurementType.POWER_APPARENT)
        measurements.append(MeasurementType.POWER_FACTOR)
        measurements.append(MeasurementType.FREQUENCY)
        measurements.append(MeasurementType.ENERGY_TOTAL)
        measurements.append(MeasurementType.ENERGY_TOTAL_EXPORT)
        measurements.append(MeasurementType.ENERGY_TOTAL_REACTIVE_IMPORT)
        measurements.append(MeasurementType.ENERGY_TOTAL_REACTIVE_EXPORT)
        if self.has_L1() and self.has_threephase():
            measurements.append(MeasurementType.VOLTAGE_L1_N)
            measurements.append(MeasurementType.POWER_L1)
            measurements.append(MeasurementType.CURRENT_L1)
        if self.has_L2():
            measurements.append(MeasurementType.VOLTAGE_L2_N)
            measurements.append(MeasurementType.POWER_L2)
            measurements.append(MeasurementType.CURRENT_L2)
        if self.has_L3():
            measurements.append(MeasurementType.VOLTAGE_L3_N)
            measurements.append(MeasurementType.POWER_L3)
            measurements.append(MeasurementType.CURRENT_L3)
        if self.has_threephase():
            measurements.append(MeasurementType.VOLTAGE_L1_L2)
            measurements.append(MeasurementType.VOLTAGE_L2_L3)
            measurements.append(MeasurementType.VOLTAGE_L3_L1)
            measurements.append(MeasurementType.VOLTAGE_L_L)
        return measurements

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
        mdate = self._readregister(0x0083, 4, '>HHHH')
        return self._decodetime(mdate)

#################################################################################################
### METER DATA functions
#################################################################################################

    def md_current_L1(self):
        return (self._readregister(0x0BB7, 2, '>f'))[0]

    def md_current_L2(self):
        return (self._readregister(0x0BB9, 2, '>f'))[0]

    def md_current_L3(self):
        return (self._readregister(0x0BBB, 2, '>f'))[0]

    def md_current(self):           # Average current
        return (self._readregister(0x0BC1, 2, '>f'))[0]

    def md_voltage_L1_L2(self):
        return (self._readregister(0x0BCB, 2, '>f'))[0]

    def md_voltage_L2_L3(self):
        return (self._readregister(0x0BCD, 2, '>f'))[0]

    def md_voltage_L3_L1(self):
        return (self._readregister(0x0BCF, 2, '>f'))[0]

    def md_voltage_L_L(self):
        return (self._readregister(0x0BD1, 2, '>f'))[0]

    def md_voltage_L1_N(self):
        return (self._readregister(0x0BD3, 2, '>f'))[0]

    def md_voltage_L2_N(self):
        return (self._readregister(0x0BD5, 2, '>f'))[0]

    def md_voltage_L3_N(self):
        return (self._readregister(0x0BD7, 2, '>f'))[0]

    def md_voltage(self):   # Average L-N voltage
        return (self._readregister(0x0BDB, 2, '>f'))[0]

    def md_power_L1(self):
        """
        Retrieve actual power usage for phase 1

        :return: Power usage in W (Watts)
        """
        return (self._readregister(0x0BED, 2, '>f'))[0]*1000

    def md_power_L2(self):
        """
        Retrieve actual power usage for phase 2

        :return: Power usage in W (Watts)
        """
        return (self._readregister(0x0BEF, 2, '>f'))[0]*1000

    def md_power_L3(self):
        """
        Retrieve actual power usage for phase 3

        :return: Power usage in W (Watts)
        """
        return (self._readregister(0x0BF1, 2, '>f'))[0]*1000

    def md_power(self):
        """
        Retrieve actual total power usage for all phases

        :return: Power usage in W (Watts)
        """
        return (self._readregister(0x0BF3, 2, '>f'))[0]*1000

    def md_power_reactive(self):    # Not applicable for iEM3150 / iEM3250 / iEM3350
        return (self._readregister(0x0BFB, 2, '>f'))[0]

    def md_power_apparent(self):    # Not applicable for iEM3150 / iEM3250 / iEM3350
        return (self._readregister(0x0C03, 2, '>f'))[0]

    def md_powerfactor(self):
        return (self._readregister(0x0C0B, 2, '>f'))[0]

    def md_frequency(self):
        """
        Retrieve current net frequency

        :return: Frequency in Hz (Hertz)
        """
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

    def ed_total_export(self):              # Not applicable for iEM3150 / iEM3250 / iEM3350
        """
        Retrieve total Active Energy export

        :return: Energy in kWh (kWatt-hour)
        """
        return (self._readregister(0xB02D, 2, '>f'))[0]

    def ed_total_reactive_import(self):     # Not applicable for iEM3150 / iEM3250 / iEM3350
        """
        Retrieve total Reactive Energy import

        :return: Energy in kVARh (kVolt-Amper(Reactive)-hour)
        """
        return (self._readregister(0xB02F, 2, '>f'))[0]

    def ed_total_reactive_export(self):     # Not applicable for iEM3150 / iEM3250 / iEM3350
        """
        Retrieve total Reactive Energy export

        :return: Energy in kVARh (kVolt-Amper(Reactive)-hour)
        """
        return (self._readregister(0xB031, 2, '>f'))[0]

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
