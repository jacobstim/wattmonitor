import paho.mqtt.client as mqtt
import modbus_tk
import modbus_tk.defines as cst
from modbus_tk import modbus_tcp, hooks
from time import sleep
from repeatedtimer import repeatedtimer
from datetime import datetime
import logging
import json

# Meters to use
from meters import A9MEM3155

########################################################################################
### NETWORK CONFIGURATION
########################################################################################

MODBUS_SERVER = "172.16.0.60"
MODBUS_PORT = 502

MQTT_SERVER = "mqtt.home.local"
MQTT_PORT = 1883
PUBTOPIC1="smarthome/energy/iem3155/data"

########################################################################################
### LOOP
########################################################################################

def loop(meter, mqttclient):
    measurements = {}
    measurements["timestamp"] = datetime.now().isoformat()

    # Voltages
    measurements["voltage_L_L"] = meter.md_voltage_L_L()
    measurements["voltage_L1_L2"] = meter.md_voltage_L1_L2()
    measurements["voltage_L2_L3"] = meter.md_voltage_L2_L3()
    measurements["voltage_L3_L1"] = meter.md_voltage_L3_L1()
    measurements["voltage_L1_N"] = meter.md_voltage_L1_N()
    measurements["voltage_L2_N"] = meter.md_voltage_L2_N()
    measurements["voltage_L3_N"] = meter.md_voltage_L3_N()

    # Currents
    measurements["current"] = meter.md_current()
    measurements["current_L1"] = meter.md_current_L1()
    measurements["current_L2"] = meter.md_current_L2()
    measurements["current_L3"] = meter.md_current_L3()

    # Power
    measurements["power"] = meter.md_power()
    measurements["power_L1"] = meter.md_power_L1()
    measurements["power_L2"] = meter.md_power_L2()
    measurements["power_L3"] = meter.md_power_L3()

    # Other
    measurements["powerfactor"] = meter.md_powerfactor()
    measurements["frequency"] = meter.md_frequency()

    # Totals
    measurements["total_active_in"] = meter.ed_total()
    measurements["total_active_out"] = meter.ed_total_export()
    measurements["total_reactive_in"] = meter.ed_total_reactive_import()
    measurements["total_reactive_out"] = meter.ed_total_reactive_export()

    # Convert to JSON
    jsondata = json.dumps(measurements)
    logging.debug("JSON Data: " + jsondata)

    # Post to MQTT server
    mqttclient.publish(PUBTOPIC1, payload = jsondata, qos=1)

########################################################################################
### CALLBACKS
########################################################################################

#def modbus_on_before_connect(args):
#    master = args[0]
#    logging.debug("on_before_connect {0} {1}".format(master._host, master._port))

#def modbus_on_after_recv(data):
#    master, bytes_data = data
#    logging.info(bytes_data)

#def modbus_on_after_recv(args):
#    response = args[1]
#   logging.debug("on_after_recv {0} bytes received".format(len(response)))

# The callback for when the client receives a CONNACK response from the server.
def mqtt_on_connect(client, userdata, flags, rc):
    logging.info("Connected to MQTT server (result code " + str(rc) + ")")

########################################################################################
### MAIN
########################################################################################

def main():
    # Configure Modbus
    logger = modbus_tk.utils.create_logger("console", level=logging.DEBUG)
    # hooks.install_hook('modbus.Master.after_recv', modbus_on_after_recv)
    # hooks.install_hook("modbus_tcp.TcpMaster.before_connect", modbus_on_before_connect)
    # hooks.install_hook("modbus_tcp.TcpMaster.after_recv", modbus_on_after_recv)

    try:
        # Configure Modbus TCP server
        master = modbus_tcp.TcpMaster(host=MODBUS_SERVER, port=MODBUS_PORT)
        master.set_timeout(5.0)

    except modbus_tk.modbus.ModbusError as exc:
        logger.error("%s - Code=%d", exc, exc.get_exception_code())

    # Initialize meters
    meter1 = A9MEM3155.iMEM3155(master, 10)
    # TODO - create meter reader for all meters

    # Initialize MQTT
    mqttclient = mqtt.Client()
    mqttclient.on_connect = mqtt_on_connect     # On connect handler

    mqttclient.connect(MQTT_SERVER, MQTT_PORT, 60)
    mqttclient.loop_start()     # Launch seperate thread for checking for messages, keep connection alive, ...

    # Initialize recurring task, our 'loop' function
    rt = repeatedtimer.RepeatedTimer(1, 1, loop, meter1, mqttclient)
    rt.first_start()

    try:
        while(True):
            sleep(5)

    except KeyboardInterrupt:
        logging.info('Stopping program!')

    finally:
        rt.stop()  # stop reading data
        mqttclient.loop_stop()  # stop the mqtt loop

########################################################################################
### ACTUAL MAIN
########################################################################################

logging.basicConfig(level=logging.DEBUG)

if __name__ == '__main__':
	main()
