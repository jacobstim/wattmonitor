from enum import Enum
from datetime import datetime
import struct
from .measurements import MeasurementType
from .base_meter import BaseMeter
from .data_types import DataType, RegisterConfig, BatchRegisterConfig

class iMEM2150(BaseMeter):
    
    """
    This class implements the Schneider Electric iM2150 meter values
    """
    
    # Register configuration constants for better maintainability
    REGISTER_CONFIGS = {
        # System registers
        MeasurementType.VOLTAGE: RegisterConfig(0x0BD3, 2, DataType.FLOAT32),  # voltage_l1_n
        MeasurementType.CURRENT: RegisterConfig(0x0BB7, 2, DataType.FLOAT32),  # current_l1
        MeasurementType.POWER: RegisterConfig(0x0BED, 2, DataType.FLOAT32),    # power_l1
        MeasurementType.POWER_REACTIVE: RegisterConfig(0x0BFB, 2, DataType.FLOAT32),
        MeasurementType.POWER_APPARENT: RegisterConfig(0x0C03, 2, DataType.FLOAT32),
        MeasurementType.POWER_FACTOR: RegisterConfig(0x0C0B, 2, DataType.FLOAT32),
        MeasurementType.FREQUENCY: RegisterConfig(0x0C25, 2, DataType.FLOAT32),
        MeasurementType.ENERGY_TOTAL: RegisterConfig(0xB02B, 2, DataType.FLOAT32),
        MeasurementType.ENERGY_TOTAL_EXPORT: RegisterConfig(0xB02D, 2, DataType.FLOAT32),
        MeasurementType.ENERGY_TOTAL_REACTIVE_IMPORT: RegisterConfig(0xB02F, 2, DataType.FLOAT32),
        MeasurementType.ENERGY_TOTAL_REACTIVE_EXPORT: RegisterConfig(0xB031, 2, DataType.FLOAT32),
        
        # System information
        MeasurementType.METER_NAME: RegisterConfig(0x001D, 20, DataType.STRING),
        MeasurementType.METER_MODEL: RegisterConfig(0x0031, 20, DataType.STRING),
        MeasurementType.MANUFACTURER: RegisterConfig(0x0045, 20, DataType.STRING),
        MeasurementType.SERIAL_NUMBER: RegisterConfig(0x0081, 2, DataType.UINT32),
        MeasurementType.MANUFACTURE_DATE: RegisterConfig(0x0083, 4, DataType.STRING),
    }

    # Batch register configurations for optimized Modbus communication
    # These configurations create larger batches that may skip unused registers for maximum efficiency
    BATCH_REGISTER_CONFIGS = {
#        'system_info': BatchRegisterConfig(
#            start_register=0x001D,
#            total_count=106,  # From 0x001D to 0x0086 - covers all system information in one large read
#            measurements={
#                MeasurementType.METER_NAME: 0,           # Offset 0: 0x001D (20 registers)
#                MeasurementType.METER_MODEL: 20,         # Offset 20: 0x0031 (20 registers)
#                MeasurementType.MANUFACTURER: 40,       # Offset 40: 0x0045 (20 registers)
#                MeasurementType.SERIAL_NUMBER: 100,      # Offset 100: 0x0081 (2 registers) - (0x0081 - 0x001D) = 100
#                MeasurementType.MANUFACTURE_DATE: 102,   # Offset 102: 0x0083 (4 registers) - (0x0083 - 0x001D) = 102
#                # Note: This reads 106 registers but only uses specific offsets
#                # Registers in between (e.g., 0x0059-0x0080) are read but ignored
#            },
#            description="Complete system information block (includes unused registers)"
#        ),
        'live_measurements': BatchRegisterConfig(
            start_register=0x0BB7,
            total_count=112,  # From 0x0BB7 to 0x0C26 - covers ALL measurement registers in one massive read (fixed: was 111, now 112 to include both frequency registers)
            measurements={
                MeasurementType.CURRENT: 0,           # Offset 0: 0x0BB7 (2 registers)
                MeasurementType.VOLTAGE: 28,          # Offset 28: 0x0BD3 (2 registers) - (0x0BD3 - 0x0BB7) = 28
                MeasurementType.POWER: 54,            # Offset 54: 0x0BED (2 registers) - (0x0BED - 0x0BB7) = 54
                MeasurementType.POWER_REACTIVE: 68,   # Offset 68: 0x0BFB (2 registers) - (0x0BFB - 0x0BB7) = 68
                MeasurementType.POWER_APPARENT: 76,   # Offset 76: 0x0C03 (2 registers) - (0x0C03 - 0x0BB7) = 76
                MeasurementType.POWER_FACTOR: 84,     # Offset 84: 0x0C0B (2 registers) - (0x0C0B - 0x0BB7) = 84
                MeasurementType.FREQUENCY: 110,       # Offset 110: 0x0C25 (2 registers) - (0x0C25 - 0x0BB7) = 110
                # Note: This reads 111 registers but only extracts 7 specific measurements
                # Many registers in between are read but unused (e.g., 0x0BB9-0x0BD2, 0x0BD5-0x0BEC, etc.)
            },
            description="Complete live measurements block (optimized single read with gaps)"
        ),
        'energy_counters': BatchRegisterConfig(
            start_register=0xB02B,
            total_count=8,   # From 0xB02B to 0xB032 - covers all energy registers (still contiguous)
            measurements={
                MeasurementType.ENERGY_TOTAL: 0,                      # Offset 0: 0xB02B (2 registers)
                MeasurementType.ENERGY_TOTAL_EXPORT: 2,               # Offset 2: 0xB02D (2 registers)
                MeasurementType.ENERGY_TOTAL_REACTIVE_IMPORT: 4,      # Offset 4: 0xB02F (2 registers)
                MeasurementType.ENERGY_TOTAL_REACTIVE_EXPORT: 6,      # Offset 6: 0xB031 (2 registers)
            },
            description="Energy counters (contiguous block)"
        ),
    }

    def __init__(self, modbus, address=1):
        # Construct using the base meter
        super().__init__(modbus, address)

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

    def sys_manufacturedate(self):
        """
        Queries the meter for its manufacturing date

        :return: Manufacturing date of the energy meter as a datetime object
        """
        mdate = self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.MANUFACTURE_DATE])
        return self._decodetime(mdate)

#################################################################################################
### METER DATA functions
#################################################################################################

    def md_current_L1(self):
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.CURRENT])

    def md_voltage_L1_N(self):
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.VOLTAGE])

    def md_current(self):           
        return self.md_current_L1()

    def md_voltage(self):   
        return self.md_voltage_L1_N()

    def md_power_L1(self):
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.POWER]) * 1000

    def md_power(self):
        return self.md_power_L1()

    def md_power_reactive(self):
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.POWER_REACTIVE])

    def md_power_apparent(self):
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.POWER_APPARENT])

    def md_powerfactor(self):
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.POWER_FACTOR])

    def md_frequency(self):
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.FREQUENCY])

#################################################################################################
### ENERGY DATA functions
#################################################################################################

    def ed_total(self):
        """
        Retrieve total Active Energy import

        :return: Energy in kWh (kWatt-hour) -- It returns in Wh, not kWh!!!
        """
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.ENERGY_TOTAL]) / 1000.0

    def ed_total_export(self):
        """
        Retrieve total Active Energy export

        :return: Energy in kWh (kWatt-hour)
        """
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.ENERGY_TOTAL_EXPORT]) / 1000.0

    def ed_total_reactive_import(self):
        """
        Retrieve total Reactive Energy import

        :return: Energy in kVARh (kVolt-Amper(Reactive)-hour)
        """
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.ENERGY_TOTAL_REACTIVE_IMPORT]) / 1000.0

    def ed_total_reactive_export(self):
        """
        Retrieve total Reactive Energy export

        :return: Energy in kVARh (kVolt-Amper(Reactive)-hour)
        """
        return self._read_register_config(self.REGISTER_CONFIGS[MeasurementType.ENERGY_TOTAL_REACTIVE_EXPORT]) / 1000.0

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
