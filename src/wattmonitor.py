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
from meters import A9MEM2150
from meters import ECR140D

########################################################################################
### NETWORK CONFIGURATION
########################################################################################

MODBUS_SERVER = "172.16.0.60"
MODBUS_PORT = 502

MQTT_SERVER = "mqtt.home.local"
MQTT_PORT = 1883
PUBTOPIC1="smarthome/energy/iem3155/data/sec"                # Publish here every second
PUBTOPIC1_AVG="smarthome/energy/iem3155/data/min"            # Publish here every minute
PUBTOPIC2="smarthome/energy/iem2150-airco1/data/sec"         # Publish here every second
PUBTOPIC2_AVG="smarthome/energy/iem2150-airco1/data/min"     # Publish here every minute
PUBTOPIC3="smarthome/energy/iem2150-airco2/data/sec"         # Publish here every second
PUBTOPIC3_AVG="smarthome/energy/iem2150-airco2/data/min"     # Publish here every minute
PUBTOPIC4="smarthome/energy/ecr140d-unit1/data/sec"          # Publish here every second
PUBTOPIC4_AVG="smarthome/energy/ecr140d-unit1/data/min"      # Publish here every minute
PUBTOPIC5="smarthome/energy/ecr140d-unit2/data/sec"          # Publish here every second
PUBTOPIC5_AVG="smarthome/energy/ecr140d-unit2/data/min"      # Publish here every minute
 
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
### METER DATA HANDLER
########################################################################################

class MeterDataHandler():
    def __init__(self, meter, mqttclient, topic, topic_avg):
        self.meter = meter
        self.mqttclient = mqttclient
        self.topic = topic
        self.topic_avg = topic_avg
        self.minute_data = PowerMeasurements()

    def pushMeasurements(self):
        measurements = {}
        measurements["timestamp"] = datetime.now().isoformat()

        ###################################################################
        # Voltages
        ###################################################################
        # First phase is always supported
        value = self.meter.md_voltage_L1_N()
        self.minute_data.add("voltage_L1_N", value)
        measurements["voltage_L1_N"] = value

        # Add other metrics only for three-phase meters
        if self.meter.has_threephase():
            value = self.meter.md_voltage_L_L()
            self.minute_data.add("voltage_L_L", value)
            measurements["voltage_L_L"] = value

            value = self.meter.md_voltage_L1_L2()
            self.minute_data.add("voltage_L1_L2", value)
            measurements["voltage_L1_L2"] = value

            value = self.meter.md_voltage_L2_L3()
            self.minute_data.add("voltage_L2_L3", value)
            measurements["voltage_L2_L3"] = value

            value = self.meter.md_voltage_L3_L1()
            self.minute_data.add("voltage_L3_L1", value)
            measurements["voltage_L3_L1"] = value

            value = self.meter.md_voltage_L2_N()
            self.minute_data.add("voltage_L2_N", value)
            measurements["voltage_L2_N"] = value

            value = self.meter.md_voltage_L3_N()
            self.minute_data.add("voltage_L3_N", value)
            measurements["voltage_L3_N"] = value

        ###################################################################
        # Power
        ###################################################################
        value = self.meter.md_power()
        self.minute_data.add("power", value)
        measurements["power"] = value

        if self.meter.has_threephase():
            value = self.meter.md_power_L1()
            self.minute_data.add("power_L1", value)
            measurements["power_L1"] = value

            value = self.meter.md_power_L2()
            self.minute_data.add("power_L2", value)
            measurements["power_L2"] = value

            value = self.meter.md_power_L3()
            self.minute_data.add("power_L3", value)
            measurements["power_L3"] = value

        ###################################################################
        # Currents
        ###################################################################
        value = self.meter.md_current()
        self.minute_data.add("current", value)
        measurements["current"] = value

        if self.meter.has_threephase():
            value = self.meter.md_current_L1()
            self.minute_data.add("current_L1", value)
            measurements["current_L1"] = value

            value = self.meter.md_current_L2()
            self.minute_data.add("current_L2", value)
            measurements["current_L2"] = value

            value = self.meter.md_current_L3()
            self.minute_data.add("current_L3", value)
            measurements["current_L3"] = value

        ###################################################################
        # Other
        ###################################################################
        value = self.meter.md_powerfactor()
        self.minute_data.add("powerfactor", value)
        measurements["powerfactor"] = value

        value = self.meter.md_frequency()
        self.minute_data.add("frequency", value)
        measurements["frequency"] = value

        ###################################################################
        # Totals
        ###################################################################

        value = self.meter.ed_total()
        self.minute_data.set("total_active_in", value)
        measurements["total_active_in"] = value

        value = self.meter.ed_total_export()
        self.minute_data.set("total_active_out", value)
        measurements["total_active_out"] = value

        value = self.meter.ed_total_reactive_import()
        self.minute_data.set("total_reactive_in", value)
        measurements["total_reactive_in"] = value

        value = self.meter.ed_total_reactive_export()
        self.minute_data.set("total_reactive_out", value)
        measurements["total_reactive_out"] = value

        # Convert to JSON
        jsondata = json.dumps(measurements)
        logging.debug("---- JSON Data (topic: " + self.topic + ") ----------------------------------------\n" + jsondata)

        # Post to MQTT server
        self.mqttclient.publish(self.topic, payload = jsondata, qos=1)
        
    def pushAverageMeasurements(self):
         # Retrieve averages of past 60 minutes
        jsondata = self.minute_data.to_json()
        logging.debug("---- Per minute data (topic: " + self.topic_avg + ") ---------------------------------\n" + jsondata)
        # Post to MQTT server
        self.mqttclient.publish(self.topic_avg, payload = jsondata, qos=1)
        # Clear and restart
        self.minute_data.clear()   


########################################################################################
### LOOP
########################################################################################


# This pushes the data every second for analytical purposes
def loop_1s(meters):
    # Read the secondly data for every meter and send it
    for meterhandler in meters:
        meterhandler.pushMeasurements()


# This publishes average data every 60 seconds for dashboarding purposes
def loop_60s(meters):
    # Send the minute average data
    for meterhandler in meters:
        meterhandler.pushAverageMeasurements()

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

meters = []

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


    # Initialize MQTT
    mqttclient = mqtt.Client()
    mqttclient.on_connect = mqtt_on_connect     # On connect handler

    mqttclient.connect(MQTT_SERVER, MQTT_PORT, 60)
    mqttclient.loop_start()     # Launch seperate thread for checking for messages, keep connection alive, ...

    # Initialize meters
    meter1 = A9MEM3155.iMEM3155(master, 10)             # MODBUS ID = 10
    meter2 = A9MEM2150.iMEM2150(master, 20)             # MODBUS ID = 20
    meter3 = A9MEM2150.iMEM2150(master, 21)             # MODBUS ID = 21
    meter4 = ECR140D.ECR140D(master, 25)                # MODBUS ID = 25
    meter5 = ECR140D.ECR140D(master, 26)                # MODBUS ID = 26
 
    # Create meter data handlers
    meterhandler1 = MeterDataHandler(meter1,mqttclient,PUBTOPIC1,PUBTOPIC1_AVG)
    meters.append(meterhandler1)

    meterhandler2 = MeterDataHandler(meter2,mqttclient,PUBTOPIC2,PUBTOPIC2_AVG)
    meters.append(meterhandler2)

    meterhandler3 = MeterDataHandler(meter3,mqttclient,PUBTOPIC3,PUBTOPIC3_AVG)
    meters.append(meterhandler3)

    meterhandler4 = MeterDataHandler(meter4,mqttclient,PUBTOPIC4,PUBTOPIC4_AVG)
    meters.append(meterhandler4)

    meterhandler5 = MeterDataHandler(meter5,mqttclient,PUBTOPIC5,PUBTOPIC5_AVG)
    meters.append(meterhandler5)

    # Initialize recurring task, our 'loop' function
    rt = repeatedtimer.RepeatedTimer(1, 1, loop_1s, meters)
    rt.first_start()

    rt2 = repeatedtimer.RepeatedTimer(60, 60, loop_60s, meters)
    rt2.first_start()

    try:
        while(True):
            sleep(5)

    except KeyboardInterrupt:
        logging.info('Stopping program!')

    finally:
        rt.stop()   # stop reading data
        rt2.stop()  # stop reading data
        mqttclient.loop_stop()  # stop the mqtt loop

########################################################################################
### ACTUAL MAIN
########################################################################################

logging.basicConfig(level=logging.DEBUG)

if __name__ == '__main__':
	main()
