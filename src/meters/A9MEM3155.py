from enum import Enum
from datetime import datetime
import struct
from .measurements import MeasurementType
from .base_meter import BaseMeter
from .data_types import DataType, RegisterConfig

class iMEM3155(BaseMeter):
    
    """
    This class implements the Schneider Electric iM3155 meter values
    """
    
    # Register configuration constants for better maintainability
    REGISTER_CONFIGS = {
        # System registers
        'metername': RegisterConfig(0x001D, 20, DataType.STRING),
        'metermodel': RegisterConfig(0x0031, 20, DataType.STRING),
        'manufacturer': RegisterConfig(0x0045, 20, DataType.STRING),
        'serialnumber': RegisterConfig(0x0081, 2, DataType.UINT32),
        'manufacturedate': RegisterConfig(0x0083, 4, DataType.STRING),
        
        # Current measurement registers
        'current_l1': RegisterConfig(0x0BB7, 2, DataType.FLOAT32),
        'current_l2': RegisterConfig(0x0BB9, 2, DataType.FLOAT32),
        'current_l3': RegisterConfig(0x0BBB, 2, DataType.FLOAT32),
        'current_avg': RegisterConfig(0x0BC1, 2, DataType.FLOAT32),
        
        # Voltage measurement registers (line-to-line)
        'voltage_l1_l2': RegisterConfig(0x0BCB, 2, DataType.FLOAT32),
        'voltage_l2_l3': RegisterConfig(0x0BCD, 2, DataType.FLOAT32),
        'voltage_l3_l1': RegisterConfig(0x0BCF, 2, DataType.FLOAT32),
        'voltage_l_l_avg': RegisterConfig(0x0BD1, 2, DataType.FLOAT32),
        
        # Voltage measurement registers (line-to-neutral)
        'voltage_l1_n': RegisterConfig(0x0BD3, 2, DataType.FLOAT32),
        'voltage_l2_n': RegisterConfig(0x0BD5, 2, DataType.FLOAT32),
        'voltage_l3_n': RegisterConfig(0x0BD7, 2, DataType.FLOAT32),
        'voltage_l_n_avg': RegisterConfig(0x0BDB, 2, DataType.FLOAT32),
        
        # Power measurement registers
        'power_l1': RegisterConfig(0x0BED, 2, DataType.FLOAT32),
        'power_l2': RegisterConfig(0x0BEF, 2, DataType.FLOAT32),
        'power_l3': RegisterConfig(0x0BF1, 2, DataType.FLOAT32),
        'power_total': RegisterConfig(0x0BF3, 2, DataType.FLOAT32),
        'power_reactive': RegisterConfig(0x0BFB, 2, DataType.FLOAT32),
        'power_apparent': RegisterConfig(0x0C03, 2, DataType.FLOAT32),
        'power_factor': RegisterConfig(0x0C0B, 2, DataType.FLOAT32),
        'frequency': RegisterConfig(0x0C25, 2, DataType.FLOAT32),
        
        # Energy registers
        'energy_total': RegisterConfig(0xB02B, 2, DataType.FLOAT32),
        'energy_export': RegisterConfig(0xB02D, 2, DataType.FLOAT32),
        'energy_reactive_import': RegisterConfig(0xB02F, 2, DataType.FLOAT32),
        'energy_reactive_export': RegisterConfig(0xB031, 2, DataType.FLOAT32),
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
        return self._read_register_config(self.REGISTER_CONFIGS['metername'])

    def sys_metermodel(self):
        return self._read_register_config(self.REGISTER_CONFIGS['metermodel'])

    def sys_manufacturer(self):
        return self._read_register_config(self.REGISTER_CONFIGS['manufacturer'])

    def sys_serialnumber(self):
        return self._read_register_config(self.REGISTER_CONFIGS['serialnumber'])

    def sys_manufacturedate(self):
        """
        Queries the meter for its manufacturing date

        :return: Manufacturing date of the energy meter as a datetime object
        """
        mdate = self._read_register_config(self.REGISTER_CONFIGS['manufacturedate'])
        return self._decodetime(mdate)

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
        return self._read_register_config(self.REGISTER_CONFIGS['current_avg'])

    def md_voltage_L1_L2(self):
        return self._read_register_config(self.REGISTER_CONFIGS['voltage_l1_l2'])

    def md_voltage_L2_L3(self):
        return self._read_register_config(self.REGISTER_CONFIGS['voltage_l2_l3'])

    def md_voltage_L3_L1(self):
        return self._read_register_config(self.REGISTER_CONFIGS['voltage_l3_l1'])

    def md_voltage_L_L(self):
        return self._read_register_config(self.REGISTER_CONFIGS['voltage_l_l_avg'])

    def md_voltage_L1_N(self):
        return self._read_register_config(self.REGISTER_CONFIGS['voltage_l1_n'])

    def md_voltage_L2_N(self):
        return self._read_register_config(self.REGISTER_CONFIGS['voltage_l2_n'])

    def md_voltage_L3_N(self):
        return self._read_register_config(self.REGISTER_CONFIGS['voltage_l3_n'])

    def md_voltage(self):   # Average L-N voltage
        return self._read_register_config(self.REGISTER_CONFIGS['voltage_l_n_avg'])

    def md_power_L1(self):
        """
        Retrieve actual power usage for phase 1

        :return: Power usage in W (Watts)
        """
        return self._read_register_config(self.REGISTER_CONFIGS['power_l1']) * 1000

    def md_power_L2(self):
        """
        Retrieve actual power usage for phase 2

        :return: Power usage in W (Watts)
        """
        return self._read_register_config(self.REGISTER_CONFIGS['power_l2']) * 1000

    def md_power_L3(self):
        """
        Retrieve actual power usage for phase 3

        :return: Power usage in W (Watts)
        """
        return self._read_register_config(self.REGISTER_CONFIGS['power_l3']) * 1000

    def md_power(self):
        """
        Retrieve actual total power usage for all phases

        :return: Power usage in W (Watts)
        """
        return self._read_register_config(self.REGISTER_CONFIGS['power_total']) * 1000

    def md_power_reactive(self):    # Not applicable for iEM3150 / iEM3250 / iEM3350
        return self._read_register_config(self.REGISTER_CONFIGS['power_reactive'])

    def md_power_apparent(self):    # Not applicable for iEM3150 / iEM3250 / iEM3350
        return self._read_register_config(self.REGISTER_CONFIGS['power_apparent'])

    def md_powerfactor(self):
        return self._read_register_config(self.REGISTER_CONFIGS['power_factor'])

    def md_frequency(self):
        """
        Retrieve current net frequency

        :return: Frequency in Hz (Hertz)
        """
        return self._read_register_config(self.REGISTER_CONFIGS['frequency'])

#################################################################################################
### ENERGY DATA functions
#################################################################################################

    def ed_total(self):
        """
        Retrieve total Active Energy import

        :return: Energy in kWh (kWatt-hour)
        """
        return self._read_register_config(self.REGISTER_CONFIGS['energy_total'])

    def ed_total_export(self):              # Not applicable for iEM3150 / iEM3250 / iEM3350
        """
        Retrieve total Active Energy export

        :return: Energy in kWh (kWatt-hour)
        """
        return self._read_register_config(self.REGISTER_CONFIGS['energy_export'])

    def ed_total_reactive_import(self):     # Not applicable for iEM3150 / iEM3250 / iEM3350
        """
        Retrieve total Reactive Energy import

        :return: Energy in kVARh (kVolt-Amper(Reactive)-hour)
        """
        return self._read_register_config(self.REGISTER_CONFIGS['energy_reactive_import'])

    def ed_total_reactive_export(self):     # Not applicable for iEM3150 / iEM3250 / iEM3350
        """
        Retrieve total Reactive Energy export

        :return: Energy in kVARh (kVolt-Amper(Reactive)-hour)
        """
        return self._read_register_config(self.REGISTER_CONFIGS['energy_reactive_export'])

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
