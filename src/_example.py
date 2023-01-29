import modbus_tk
from modbus_tk import modbus_tcp
import logging

# Meters to use
from meters import A9MEM2150

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
meter1 = A9MEM2150.iMEM2150(master, 21)

### READ METER CHARACTERISTICS
logging.info("\tQuerying Meter Information")
logging.info("\tMeter name           : '" + meter1.sys_metername() + "'")
logging.info("\tMeter model          : '" + meter1.sys_metermodel() + "'")
logging.info("\tManufacturer         : '" + meter1.sys_manufacturer() + "'")
logging.info("\tSerial Number        : " + str(meter1.sys_serialnumber()))
#logging.info("\tManufacture Date     : " + meter1.sys_manufacturedate().isoformat())

logging.info("\tQuerying Current Voltages")
logging.info("\tVoltage L-N Avg      : " + str(meter1.md_voltage()))

logging.info("\tQuerying Current Currents")
logging.info("\tCurrent Total        : " + str(meter1.md_current()))
logging.info("\t * Current L1        : " + str(meter1.md_current_L1()))

logging.info("\tQuerying Current Powers")
logging.info("\tPower Total          : " + str(meter1.md_power()))

logging.info("\tQuerying Other Statistics")
logging.info("\tPower Factor         : " + str(meter1.md_powerfactor()))
logging.info("\tFrequency            : " + str(meter1.md_frequency()))

logging.info("\tQuerying Cumulative Energy Statistics")
logging.info("\tTotal (Active IN)    : " + str(meter1.ed_total()))
logging.info("\tTotal (Active OUT)   : " + str(meter1.ed_total_export()))
logging.info("\tTotal (Reactive IN)  : " + str(meter1.ed_total_reactive_import()))
logging.info("\tTotal (Reactive OUT) : " + str(meter1.ed_total_reactive_export()))
