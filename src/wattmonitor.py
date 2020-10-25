import modbus_tk
import modbus_tk.defines as cst
from modbus_tk import modbus_tcp, hooks
import logging
import struct

########################################################################################
### METER CONFIGURATION
########################################################################################

from meters import A9MEM3155

MODBUS_SERVER = "172.16.0.60"
MODBUS_PORT = 502

########################################################################################
### MAIN
########################################################################################

def main():
    # Event handlers
    def on_after_recv(data):
        master, bytes_data = data
        logger.info(bytes_data)

    def on_before_connect(args):
        master = args[0]
        logger.debug("on_before_connect {0} {1}".format(master._host, master._port))

    def on_after_recv(args):
        response = args[1]
        logger.debug("on_after_recv {0} bytes received".format(len(response)))

    # Logger from modbus_tk
    logger = modbus_tk.utils.create_logger("console", level=logging.DEBUG)
    #hooks.install_hook('modbus.Master.after_recv', on_after_recv)
    hooks.install_hook("modbus_tcp.TcpMaster.before_connect", on_before_connect)
    #hooks.install_hook("modbus_tcp.TcpMaster.after_recv", on_after_recv)

    # Connect to server
    try:
        master = modbus_tcp.TcpMaster(host=MODBUS_SERVER, port=MODBUS_PORT)
        master.set_timeout(5.0)
        logger.info("connected")
        
        # Initialize meters
        meter1 = A9MEM3155.iMEM3155(master, 10)

        ### READ METER CHARACTERISTICS
        logger.info("Querying Meter Information")
        logger.info("Meter name           : '" + meter1.sys_metername() + "'")
        logger.info("Meter model          : '" + meter1.sys_metermodel() + "'")
        logger.info("Manufacturer         : '" + meter1.sys_manufacturer() + "'")
        logger.info("Serial Number        : " + str(meter1.sys_serialnumber()))
        logger.info("Manufacture Date     : " + meter1.sys_manufacturedate().isoformat() )

        logger.info("Querying Current Voltages")
        logger.info("Voltage L-L Avg      : " + str(meter1.md_voltage_L_L()))
        logger.info(" * Voltage L1-L2     : " + str(meter1.md_voltage_L1_L2()))
        logger.info(" * Voltage L2-L3     : " + str(meter1.md_voltage_L2_L3()))
        logger.info(" * Voltage L3-L1     : " + str(meter1.md_voltage_L3_L1()))
        logger.info("Voltage L-N Avg      : " + str(meter1.md_voltage()))
        logger.info(" * Voltage L1-N      : " + str(meter1.md_voltage_L1_N()))
        logger.info(" * Voltage L2-N      : " + str(meter1.md_voltage_L2_N()))
        logger.info(" * Voltage L3-N      : " + str(meter1.md_voltage_L3_N()))

        logger.info("Querying Current Currents")
        logger.info("Current Total        : " + str(meter1.md_current()))
        logger.info(" * Current L1        : " + str(meter1.md_current_L1()))
        logger.info(" * Current L2        : " + str(meter1.md_current_L2()))
        logger.info(" * Current L3        : " + str(meter1.md_current_L3()))

        logger.info("Querying Current Powers")
        logger.info("Power Total          : " + str(meter1.md_power()))
        logger.info(" * Power L1          : " + str(meter1.md_power_L1()))
        logger.info(" * Power L2          : " + str(meter1.md_power_L2()))
        logger.info(" * Power L3          : " + str(meter1.md_power_L3()))

        logger.info("Querying Other Statistics")
        logger.info("Power Factor         : " + str(meter1.md_powerfactor()))
        logger.info("Frequency            : " + str(meter1.md_frequency()))

        logger.info("Querying Cumulative Energy Statistics")
        logger.info("Total (Active IN)    : " + str(meter1.ed_total()))
        logger.info("Total (Active OUT)   : " + str(meter1.ed_total_export()))
        logger.info("Total (Reactive IN)  : " + str(meter1.ed_total_reactive_import()))
        logger.info("Total (Reactive OUT) : " + str(meter1.ed_total_reactive_export()))

        ### DUMMY RELAIS SWITCHING

        ### execute(SLAVE ID, FUNCTION, STARTING ADDRESS, quantity_of_x)
        #logger.info("Querying")
        #result = master.execute(1, cst.READ_COILS, 0, 10)
        #logger.info(result)
        #newvalue = not(result[0])
        #logger.info("setting to: " + str(newvalue))
        ###logger.info(master.execute(1, cst.WRITE_SINGLE_COIL, 0, quantity_of_x=1, output_value=newvalue))
        #logger.info(master.execute(1, cst.WRITE_MULTIPLE_COILS, 0, output_value=[newvalue, not(newvalue), newvalue, newvalue, 1, 0, 1, 1]))
        
    except modbus_tk.modbus.ModbusError as exc:
        logger.error("%s - Code=%d", exc, exc.get_exception_code())
 

########################################################################################
### ACTUAL MAIN
########################################################################################

if __name__ == '__main__':
	main()
