from enum import Enum

class MeasurementType(Enum):
    VOLTAGE = "voltage"
    POWER = "power"
    CURRENT = "current"
    VOLTAGE_L1_N = "voltage_L1_N"
    VOLTAGE_L2_N = "voltage_L2_N"
    VOLTAGE_L3_N = "voltage_L3_N"
    VOLTAGE_L1_L2 = "voltage_L1_L2"
    VOLTAGE_L2_L3 = "voltage_L2_L3"
    VOLTAGE_L3_L1 = "voltage_L3_L1"
    VOLTAGE_L_L = "voltage_L_L"
    POWER_L1 = "power_L1"   
    POWER_L2 = "power_L2"
    POWER_L3 = "power_L3"
    CURRENT_L1 = "current_L1"
    CURRENT_L2 = "current_L2"
    CURRENT_L3 = "current_L3"
    POWER_REACTIVE = "power_reactive"
    POWER_APPARENT = "power_apparent"
    POWER_FACTOR = "powerfactor"
    FREQUENCY = "frequency"
    ENERGY_TOTAL = "total_active_in"
    ENERGY_TOTAL_EXPORT = "total_active_out"
    ENERGY_TOTAL_REACTIVE_IMPORT = "total_reactive_in"
    ENERGY_TOTAL_REACTIVE_EXPORT = "total_reactive_out"

    @property
    def unit(self):
        return {
            MeasurementType.VOLTAGE: "V",
            MeasurementType.CURRENT: "A",
            MeasurementType.POWER: "W",
            MeasurementType.POWER_REACTIVE: "VAR",
            MeasurementType.POWER_APPARENT: "VA",
            MeasurementType.POWER_FACTOR: "%",
            MeasurementType.FREQUENCY: "Hz",
            MeasurementType.ENERGY_TOTAL: "kWh",
            MeasurementType.ENERGY_TOTAL_EXPORT: "kWh",
            MeasurementType.ENERGY_TOTAL_REACTIVE_IMPORT: "kVARh",
            MeasurementType.ENERGY_TOTAL_REACTIVE_EXPORT: "kVARh",
            MeasurementType.VOLTAGE_L1_N: "V",
            MeasurementType.VOLTAGE_L2_N: "V",
            MeasurementType.VOLTAGE_L3_N: "V",
            MeasurementType.VOLTAGE_L_L: "V",
            MeasurementType.VOLTAGE_L1_L2: "V",
            MeasurementType.VOLTAGE_L2_L3: "V",
            MeasurementType.VOLTAGE_L3_L1: "V",
            MeasurementType.POWER_L1: "W",
            MeasurementType.POWER_L2: "W",
            MeasurementType.POWER_L3: "W",
            MeasurementType.CURRENT_L1: "A",
            MeasurementType.CURRENT_L2: "A",
            MeasurementType.CURRENT_L3: "A",
        }[self]

    @property
    def valuename(self):
        return {
            MeasurementType.VOLTAGE: "voltage",
            MeasurementType.CURRENT: "current",
            MeasurementType.POWER: "power",
            MeasurementType.POWER_REACTIVE: "power_reactive",
            MeasurementType.POWER_APPARENT: "power_apparent",
            MeasurementType.POWER_FACTOR: "powerfactor",
            MeasurementType.FREQUENCY: "frequency",
            MeasurementType.ENERGY_TOTAL: "total_active_in",
            MeasurementType.ENERGY_TOTAL_EXPORT: "total_active_out",
            MeasurementType.ENERGY_TOTAL_REACTIVE_IMPORT: "total_reactive_in",
            MeasurementType.ENERGY_TOTAL_REACTIVE_EXPORT: "total_reactive_out",
            MeasurementType.VOLTAGE_L1_N: "voltage_L1_N",
            MeasurementType.VOLTAGE_L2_N: "voltage_L2_N",
            MeasurementType.VOLTAGE_L3_N: "voltage_L3_N",
            MeasurementType.VOLTAGE_L_L: "voltage_L_L",
            MeasurementType.VOLTAGE_L1_L2: "voltage_L1_L2",
            MeasurementType.VOLTAGE_L2_L3: "voltage_L2_L3",
            MeasurementType.VOLTAGE_L3_L1: "voltage_L3_L1",
            MeasurementType.POWER_L1: "power_L1",
            MeasurementType.POWER_L2: "power_L2",
            MeasurementType.POWER_L3: "power_L3",
            MeasurementType.CURRENT_L1: "current_L1",
            MeasurementType.CURRENT_L2: "current_L2",
            MeasurementType.CURRENT_L3: "current_L3",
        }[self]