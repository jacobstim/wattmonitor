from enum import Enum
from datetime import datetime
import struct
from .measurements import MeasurementType
from .base_meter import BaseMeter
from .data_types import DataType, RegisterConfig

class ECR140D(BaseMeter):
    
    """
    This class implements the Hager ECR140D meter values
    """
    
    # Register configuration constants for better maintainability
    REGISTER_CONFIGS = {
        # System registers
        'metername': RegisterConfig(0x1032, 16, DataType.STRING),
        'metermodel': RegisterConfig(0x1010, 16, DataType.STRING),
        'manufacturer': RegisterConfig(0x1000, 16, DataType.STRING),
        'serialnumber': RegisterConfig(0x1064, 16, DataType.STRING),
        
        # Measurement registers
        'voltage_l1_n': RegisterConfig(0xB000, 1, DataType.UINT16),
        'current_l1': RegisterConfig(0xB009, 2, DataType.UINT32),
        'power_l1': RegisterConfig(0xB019, 2, DataType.INT32),
        'power_total': RegisterConfig(0xB011, 2, DataType.INT32),
        'power_reactive': RegisterConfig(0xB01F, 2, DataType.INT32),
        'power_apparent': RegisterConfig(0xB025, 2, DataType.UINT32),
        'power_factor': RegisterConfig(0xB02B, 1, DataType.UINT16),
        'frequency': RegisterConfig(0xB006, 1, DataType.UINT16),
        
        # Energy registers
        'energy_total': RegisterConfig(0xB060, 2, DataType.UINT32),
        'energy_export': RegisterConfig(0xB064, 2, DataType.UINT32),
        'energy_reactive_import': RegisterConfig(0xB062, 2, DataType.UINT32),
        'energy_reactive_export': RegisterConfig(0xB066, 2, DataType.UINT32),
    }

    def __init__(self, modbus, address=1):
        # Construct using the base meter
        super().__init__(modbus, address)

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
        return self._read_register_config(self.REGISTER_CONFIGS['metername'])

    def sys_metermodel(self):
        return self._read_register_config(self.REGISTER_CONFIGS['metermodel'])

    def sys_manufacturer(self):
        return self._read_register_config(self.REGISTER_CONFIGS['manufacturer'])

    def sys_serialnumber(self):
        return self._read_register_config(self.REGISTER_CONFIGS['serialnumber'])

#################################################################################################
### METER DATA functions
#################################################################################################

    def md_voltage_L1_N(self):
        return self._read_register_config(self.REGISTER_CONFIGS['voltage_l1_n']) / 100.0  # Convert from 0.01V units

    def md_current_L1(self):
        return self._read_register_config(self.REGISTER_CONFIGS['current_l1']) / 1000.0  # Convert from mA to A

    def md_current(self):           
        return self.md_current_L1()

    def md_voltage(self):   
        return self.md_voltage_L1_N()

    def md_power_L1(self):
        return self._read_register_config(self.REGISTER_CONFIGS['power_l1']) * 10  # Convert from 0.01kW to W

    def md_power(self):
        return self._read_register_config(self.REGISTER_CONFIGS['power_total']) * 10  # Convert from 0.01kW to W

    def md_power_reactive(self):
        return self._read_register_config(self.REGISTER_CONFIGS['power_reactive']) * 10  # Convert from 0.01kvar to var

    def md_power_apparent(self):
        return self._read_register_config(self.REGISTER_CONFIGS['power_apparent']) * 10  # Convert from 0.01kVA to VA

    def md_powerfactor(self):
        return self._read_register_config(self.REGISTER_CONFIGS['power_factor']) / 1000.0  # Convert from 0.001 units

    def md_frequency(self):
        return self._read_register_config(self.REGISTER_CONFIGS['frequency']) / 100.0  # Convert from 0.01Hz units

#################################################################################################
### ENERGY DATA functions
#################################################################################################

    def ed_total(self):
        """
        Retrieve total Active Energy import

        :return: Energy in kWh (kWatt-hour)
        """
        return self._read_register_config(self.REGISTER_CONFIGS['energy_total'])  # Already in kWh

    def ed_total_export(self):
        """
        Retrieve total Active Energy export

        :return: Energy in kWh (kWatt-hour)
        """
        return self._read_register_config(self.REGISTER_CONFIGS['energy_export'])

    def ed_total_reactive_import(self):
        """
        Retrieve total Reactive Energy import

        :return: Energy in kVARh (kVolt-Amper(Reactive)-hour)
        """
        return self._read_register_config(self.REGISTER_CONFIGS['energy_reactive_import'])  # Already in kVARh

    def ed_total_reactive_export(self):
        """
        Retrieve total Reactive Energy export

        :return: Energy in kVARh (kVolt-Amper(Reactive)-hour)
        """
        return self._read_register_config(self.REGISTER_CONFIGS['energy_reactive_export'])
