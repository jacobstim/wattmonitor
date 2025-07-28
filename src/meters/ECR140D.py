from enum import Enum
from datetime import datetime
import struct
from .measurements import MeasurementType
from .base_meter import BaseMeter
from .data_types import DataType, RegisterConfig, BatchRegisterConfig

class ECR140D(BaseMeter):
    
    """
    This class implements the Hager ECR140D meter values
    """
    
    # Register configuration constants using MeasurementType enum
    REGISTER_CONFIGS = {
        # Primary electrical measurements
        MeasurementType.VOLTAGE: RegisterConfig(0xB000, 1, DataType.UINT16),       # voltage_l1_n
        MeasurementType.CURRENT: RegisterConfig(0xB009, 2, DataType.UINT32),       # current_l1
        MeasurementType.POWER: RegisterConfig(0xB019, 2, DataType.INT32),          # power_l1
        MeasurementType.POWER_REACTIVE: RegisterConfig(0xB01F, 2, DataType.INT32),
        MeasurementType.POWER_APPARENT: RegisterConfig(0xB025, 2, DataType.UINT32),
        MeasurementType.POWER_FACTOR: RegisterConfig(0xB02B, 1, DataType.UINT16),
        MeasurementType.FREQUENCY: RegisterConfig(0xB006, 1, DataType.UINT16),
        MeasurementType.ENERGY_TOTAL: RegisterConfig(0xB060, 2, DataType.UINT32),
        MeasurementType.ENERGY_TOTAL_EXPORT: RegisterConfig(0xB064, 2, DataType.UINT32),
        MeasurementType.ENERGY_TOTAL_REACTIVE_IMPORT: RegisterConfig(0xB062, 2, DataType.UINT32),
        MeasurementType.ENERGY_TOTAL_REACTIVE_EXPORT: RegisterConfig(0xB066, 2, DataType.UINT32),
        
        # System information
        MeasurementType.METER_NAME: RegisterConfig(0x1032, 16, DataType.STRING),
        MeasurementType.METER_MODEL: RegisterConfig(0x1010, 16, DataType.STRING),
        MeasurementType.MANUFACTURER: RegisterConfig(0x1000, 16, DataType.STRING),
        MeasurementType.SERIAL_NUMBER: RegisterConfig(0x1064, 16, DataType.STRING),
        
        # Total power register
        MeasurementType.POWER_TOTAL: RegisterConfig(0xB011, 2, DataType.INT32),
    }

    # Batch register configurations for maximum efficiency with smaller, targeted blocks
    BATCH_REGISTER_CONFIGS = {
        # Split into two batches to avoid illegal address ranges
        'live_measurements': BatchRegisterConfig(
            start_register=0xB000,
            total_count=44,  # From 0xB000 to 0xB02B - covers all live measurements
            measurements={
                # Live measurements (contiguous block)
                MeasurementType.VOLTAGE: 0,          # Offset 0: 0xB000 (1 register)
                MeasurementType.FREQUENCY: 6,        # Offset 6: 0xB006 (1 register)
                MeasurementType.CURRENT: 9,          # Offset 9: 0xB009 (2 registers)
                MeasurementType.POWER_TOTAL: 17,     # Offset 17: 0xB011 (2 registers)
                MeasurementType.POWER: 25,           # Offset 25: 0xB019 (2 registers) - power_l1
                MeasurementType.POWER_REACTIVE: 31,  # Offset 31: 0xB01F (2 registers)
                MeasurementType.POWER_APPARENT: 37,  # Offset 37: 0xB025 (2 registers)
                MeasurementType.POWER_FACTOR: 43,    # Offset 43: 0xB02B (1 register)
            },
            description="Live electrical measurements (voltage, current, power, frequency, etc.)"
        ),
        'energy_measurements': BatchRegisterConfig(
            start_register=0xB060,
            total_count=8,   # From 0xB060 to 0xB067 - covers all energy registers
            measurements={
                # Energy registers (contiguous block)
                MeasurementType.ENERGY_TOTAL: 0,                      # Offset 0: 0xB060 (2 registers)
                MeasurementType.ENERGY_TOTAL_REACTIVE_IMPORT: 2,      # Offset 2: 0xB062 (2 registers)
                MeasurementType.ENERGY_TOTAL_EXPORT: 4,               # Offset 4: 0xB064 (2 registers)
                MeasurementType.ENERGY_TOTAL_REACTIVE_EXPORT: 6,      # Offset 6: 0xB066 (2 registers)
            },
            description="Energy totals (active and reactive, import and export)"
        ),
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
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.METER_NAME])

    def sys_metermodel(self):
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.METER_MODEL])

    def sys_manufacturer(self):
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.MANUFACTURER])

    def sys_serialnumber(self):
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.SERIAL_NUMBER])

#################################################################################################
### METER DATA functions
#################################################################################################

    def md_voltage_L1_N(self):
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.VOLTAGE]) / 100.0  # Convert from 0.01V units

    def md_current_L1(self):
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.CURRENT]) / 1000.0  # Convert from mA to A

    def md_current(self):           
        return self.md_current_L1()

    def md_voltage(self):   
        return self.md_voltage_L1_N()

    def md_power_L1(self):
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.POWER]) * 10  # Convert from 0.01kW to W

    def md_power(self):
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.POWER_TOTAL]) * 10  # Convert from 0.01kW to W

    def md_power_reactive(self):
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.POWER_REACTIVE]) * 10  # Convert from 0.01kvar to var

    def md_power_apparent(self):
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.POWER_APPARENT]) * 10  # Convert from 0.01kVA to VA

    def md_powerfactor(self):
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.POWER_FACTOR]) / 1000.0  # Convert from 0.001 units

    def md_frequency(self):
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.FREQUENCY]) / 100.0  # Convert from 0.01Hz units

#################################################################################################
### ENERGY DATA functions
#################################################################################################

    def ed_total(self):
        """
        Retrieve total Active Energy import

        :return: Energy in kWh (kWatt-hour)
        """
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.ENERGY_TOTAL])  # Already in kWh

    def ed_total_export(self):
        """
        Retrieve total Active Energy export

        :return: Energy in kWh (kWatt-hour)
        """
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.ENERGY_TOTAL_EXPORT])

    def ed_total_reactive_import(self):
        """
        Retrieve total Reactive Energy import

        :return: Energy in kVARh (kVolt-Amper(Reactive)-hour)
        """
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.ENERGY_TOTAL_REACTIVE_IMPORT])  # Already in kVARh

    def ed_total_reactive_export(self):
        """
        Retrieve total Reactive Energy export

        :return: Energy in kVARh (kVolt-Amper(Reactive)-hour)
        """
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.ENERGY_TOTAL_REACTIVE_EXPORT])
