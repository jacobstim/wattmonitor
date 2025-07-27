from enum import Enum
from datetime import datetime
import struct
import modbus_tk.defines as cst
from .measurements import MeasurementType
from .base_meter import BaseMeter

class ECR140D(BaseMeter):
    
    """
    This class implements the Hager ECR140D meter values
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
        return False
    def has_L3(self):
        return False
    def has_threephase(self):
        return False

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
        return measurements

#################################################################################################
### SYSTEM functions
#################################################################################################

    def sys_metername(self):
        result = self._readregister(0x1032, 16)
        return ''.join(map(chr, struct.pack('>' + 'H' * len(result), *result)))

    def sys_metermodel(self):
        result = self._readregister(0x1010, 16)
        return ''.join(map(chr, struct.pack('>' + 'H' * len(result), *result)))

    def sys_manufacturer(self):
        result = self._readregister(0x1000, 16)
        return ''.join(map(chr, struct.pack('>' + 'H' * len(result), *result)))

    def sys_serialnumber(self):
        result = self._readregister(0x1064, 16)
        return ''.join(map(chr, struct.pack('>' + 'H' * len(result), *result)))

#################################################################################################
### METER DATA functions
#################################################################################################

    def md_voltage_L1_N(self):
        return (self._readregister(0xB000, 1))[0]/100.0              # Voltage is U16, returned in 0.01V units

    def md_current_L1(self):
        return (self._readregister(0xB009, 2, '>I'))[0]/1000.0       # Current is U32, returned in mA

    def md_current(self):           
        return self.md_current_L1()

    def md_voltage(self):   
        return self.md_voltage_L1_N()

    def md_power_L1(self):
        return (self._readregister(0xB019, 2, '>i'))[0]*10         # Phase power is S32, returned in 0.01kW -> convert to Watt

    def md_power(self):
        return (self._readregister(0xB011, 2, '>i'))[0]*10         # Total power is S32

    def md_power_reactive(self):
        return (self._readregister(0xB01F, 2, '>i'))[0]*10         # Reactive power is S32 in 0.01 kvar -> convert to var

    def md_power_apparent(self):
        return (self._readregister(0xB025, 2, '>I'))[0]*10         # Apparant power is U32 in 0.01 KvA -> convert to vA

    def md_powerfactor(self):
        return (self._readregister(0xB02B, 2))[0]/1000          # Total power factor is S16, in 0.001 resolution

    def md_frequency(self):
        return (self._readregister(0xB006, 2))[0]/100           # Frequency is U16, in 0.01Hz resolution

#################################################################################################
### ENERGY DATA functions
#################################################################################################

    def ed_total(self):
        """
        Retrieve total Active Energy import

        :return: Energy in kWh (kWatt-hour)
        """
        return (self._readregister(0xB060, 2, '>I'))[0]               # Total power is in U32, in kWh resolution

    def ed_total_export(self):
        """
        Retrieve total Active Energy export

        :return: Energy in kWh (kWatt-hour)
        """
        return (self._readregister(0xB064, 2, '>I'))[0]

    def ed_total_reactive_import(self):
        """
        Retrieve total Reactive Energy import

        :return: Energy in kVARh (kVolt-Amper(Reactive)-hour)
        """
        return (self._readregister(0xB062, 2, '>I'))[0]               # Total reactive power is in U32, in kvarh resolution

    def ed_total_reactive_export(self):
        """
        Retrieve total Reactive Energy export

        :return: Energy in kVARh (kVolt-Amper(Reactive)-hour)
        """
        return (self._readregister(0xB066, 2, '>I'))[0]
