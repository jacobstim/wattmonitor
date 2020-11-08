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
PUBTOPIC1="smarthome/energy/iem3155/data/sec"               # Publish here every second
PUBTOPIC_AVG="smarthome/energy/iem3155/data/min"            # Publish here every minute

########################################################################################
### MEASUREMENT STORAGE
########################################################################################

class PowerMeasurements():
    # We store in the dictionary:
    # * key = measurement name
    # * value = tuple (count, totalvalue)
    valuestore = {}

    def __init__(self):
        self.valuestore = {}

    def clear(self):
        self.valuestore.clear()

    def add(self, index, value):
        if ~(index in self.valuestore.keys()):
            self.valuestore[index] = [0, 0]
        self.valuestore[index][0] += 1
        self.valuestore[index][1] += value

    def set(self, index, value):
        self.valuestore[index] = [1, value]

    def average(self, index):
        if index in self.valuestore.keys():
            if self.valuestore[index][0] > 0:
                return self.valuestore[index][1] / self.valuestore[index][0]
        return 0

    def to_json(self):
        # Create a new dictionary that we will serialize into JSON
        measurements = dict.fromkeys(self.valuestore.keys())
        for index in measurements.keys():
            if self.valuestore[index][0] > 0:
                measurements[index] = self.valuestore[index][1] / self.valuestore[index][0]

        return json.dumps(measurements)

########################################################################################
### LOOP
########################################################################################

# These get updated every second and pushed every minute
minute_data = PowerMeasurements()

# This pushes the data every second for analytical purposes
def loop_1s(meter, mqttclient):
    measurements = {}
    measurements["timestamp"] = datetime.now().isoformat()

    # Voltages
    value = meter.md_voltage_L_L()
    minute_data.add("voltage_L_L", value)
    measurements["voltage_L_L"] = value

    value = meter.md_voltage_L1_L2()
    minute_data.add("voltage_L1_L2", value)
    measurements["voltage_L1_L2"] = value

    value = meter.md_voltage_L2_L3()
    minute_data.add("voltage_L2_L3", value)
    measurements["voltage_L2_L3"] = value

    value = meter.md_voltage_L3_L1()
    minute_data.add("voltage_L3_L1", value)
    measurements["voltage_L3_L1"] = value

    value = meter.md_voltage_L1_N()
    minute_data.add("voltage_L1_N", value)
    measurements["voltage_L1_N"] = value

    value = meter.md_voltage_L2_N()
    minute_data.add("voltage_L2_N", value)
    measurements["voltage_L2_N"] = value

    value = meter.md_voltage_L3_N()
    minute_data.add("voltage_L3_N", value)
    measurements["voltage_L3_N"] = value

    # Power
    value = meter.md_power()
    minute_data.add("power", value)
    measurements["power"] = value

    value = meter.md_power_L1()
    minute_data.add("power_L1", value)
    measurements["power_L1"] = value

    value = meter.md_power_L2()
    minute_data.add("power_L2", value)
    measurements["power_L2"] = value

    value = meter.md_power_L3()
    minute_data.add("power_L3", value)
    measurements["power_L3"] = value

    # Currents
    value = meter.md_current()
    minute_data.add("current", value)
    measurements["current"] = value

    value = meter.md_current_L1()
    minute_data.add("current_L1", value)
    measurements["current_L1"] = value

    value = meter.md_current_L2()
    minute_data.add("current_L2", value)
    measurements["current_L2"] = value

    value = meter.md_current_L3()
    minute_data.add("current_L3", value)
    measurements["current_L3"] = value

    # Other
    value = meter.md_powerfactor()
    minute_data.add("powerfactor", value)
    measurements["powerfactor"] = value

    value = meter.md_frequency()
    minute_data.add("frequency", value)
    measurements["frequency"] = value

    # Totals
    value = meter.ed_total()
    minute_data.set("total_active_in", value)
    measurements["total_active_in"] = value

    value = meter.ed_total_export()
    minute_data.set("total_active_out", value)
    measurements["total_active_out"] = value

    value = meter.ed_total_reactive_import()
    minute_data.set("total_reactive_in", value)
    measurements["total_reactive_in"] = value

    value = meter.ed_total_reactive_export()
    minute_data.set("total_reactive_out", value)
    measurements["total_reactive_out"] = value

    # Convert to JSON
    jsondata = json.dumps(measurements)
    logging.debug("JSON Data: " + jsondata)

    # Post to MQTT server
    mqttclient.publish(PUBTOPIC1, payload = jsondata, qos=1)


# This publishes average data every 60 seconds for dashboarding purposes
def loop_60s(meter, mqttclient):
    # Retrieve averages of past 60 minutes
    jsondata = minute_data.to_json()
    logging.debug("Per minute data: " + jsondata)
    # Post to MQTT server
    mqttclient.publish(PUBTOPIC_AVG, payload = jsondata, qos=1)

    # Clear and restart
    minute_data.clear()

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
    rt = repeatedtimer.RepeatedTimer(1, 1, loop_1s, meter1, mqttclient)
    rt.first_start()

    rt2 = repeatedtimer.RepeatedTimer(60, 60, loop_60s, meter1, mqttclient)
    rt2.first_start()

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
