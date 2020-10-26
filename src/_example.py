import modbus_tk
from modbus_tk import modbus_tcp
import logging

# Meters to use
from meters import A9MEM3155

########################################################################################
### NETWORK CONFIGURATION
########################################################################################

MODBUS_SERVER = "172.16.0.60"
MODBUS_PORT = 502

########################################################################################
### ACTUAL MAIN
########################################################################################

logging.basicConfig(level=logging.DEBUG)

try:
    # Configure Modbus TCP server
    master = modbus_tcp.TcpMaster(host=MODBUS_SERVER, port=MODBUS_PORT)
    master.set_timeout(5.0)

except modbus_tk.modbus.ModbusError as exc:
    logging.error("%s - Code=%d", exc, exc.get_exception_code())

# Initialize meters (Modbus Slave ID 10)
meter1 = A9MEM3155.iMEM3155(master, 10)

### READ METER CHARACTERISTICS
logging.info("\tQuerying Meter Information")
logging.info("\tMeter name           : '" + meter1.sys_metername() + "'")
logging.info("\tMeter model          : '" + meter1.sys_metermodel() + "'")
logging.info("\tManufacturer         : '" + meter1.sys_manufacturer() + "'")
logging.info("\tSerial Number        : " + str(meter1.sys_serialnumber()))
logging.info("\tManufacture Date     : " + meter1.sys_manufacturedate().isoformat())

logging.info("\tQuerying Current Voltages")
logging.info("\tVoltage L-L Avg      : " + str(meter1.md_voltage_L_L()))
logging.info("\t * Voltage L1-L2     : " + str(meter1.md_voltage_L1_L2()))
logging.info("\t * Voltage L2-L3     : " + str(meter1.md_voltage_L2_L3()))
logging.info("\t * Voltage L3-L1     : " + str(meter1.md_voltage_L3_L1()))
logging.info("\tVoltage L-N Avg      : " + str(meter1.md_voltage()))
logging.info("\t * Voltage L1-N      : " + str(meter1.md_voltage_L1_N()))
logging.info("\t * Voltage L2-N      : " + str(meter1.md_voltage_L2_N()))
logging.info("\t * Voltage L3-N      : " + str(meter1.md_voltage_L3_N()))

logging.info("\tQuerying Current Currents")
logging.info("\tCurrent Total        : " + str(meter1.md_current()))
logging.info("\t * Current L1        : " + str(meter1.md_current_L1()))
logging.info("\t * Current L2        : " + str(meter1.md_current_L2()))
logging.info("\t * Current L3        : " + str(meter1.md_current_L3()))

logging.info("\tQuerying Current Powers")
logging.info("\tPower Total          : " + str(meter1.md_power()))
logging.info("\t * Power L1          : " + str(meter1.md_power_L1()))
logging.info("\t * Power L2          : " + str(meter1.md_power_L2()))
logging.info("\t * Power L3          : " + str(meter1.md_power_L3()))

logging.info("\tQuerying Other Statistics")
logging.info("\tPower Factor         : " + str(meter1.md_powerfactor()))
logging.info("\tFrequency            : " + str(meter1.md_frequency()))

logging.info("\tQuerying Cumulative Energy Statistics")
logging.info("\tTotal (Active IN)    : " + str(meter1.ed_total()))
logging.info("\tTotal (Active OUT)   : " + str(meter1.ed_total_export()))
logging.info("\tTotal (Reactive IN)  : " + str(meter1.ed_total_reactive_import()))
logging.info("\tTotal (Reactive OUT) : " + str(meter1.ed_total_reactive_export()))
