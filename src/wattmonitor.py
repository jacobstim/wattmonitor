import paho.mqtt.client as mqtt
import modbus_tk
import modbus_tk.defines as cst
from modbus_tk import modbus_tcp, hooks
from time import sleep
from repeatedtimer import repeatedtimer
from datetime import datetime
import logging
import json
import itertools
from enum import Enum
from meters.measurements import MeasurementType

# Meters to use
from meters import A9MEM3155
from meters import A9MEM2150
from meters import ECR140D

# Mapping between meter types and corresponding classes
meter_classes = {
    "A9MEM3155": A9MEM3155.iMEM3155,
    "A9MEM2150": A9MEM2150.iMEM2150,
    "ECR140D": ECR140D.ECR140D
}

########################################################################################
### NETWORK CONFIGURATION
########################################################################################

MODBUS_SERVER = "172.16.0.60"
MODBUS_PORT = 502

MQTT_SERVER = "mqtt.home.local"
MQTT_PORT = 1883

METER_CONFIG = [
    {"type": "A9MEM3155", "modbus_id": 10, "name": "iem3155", "homeassistant": "true", "custom_pub_topic": "smarthome/energy/iem3155/data/sec", "custom_pub_topic_avg": "smarthome/energy/iem3155/data/min"},
    {"type": "A9MEM2150", "modbus_id": 20, "name": "iem2150-airco1", "homeassistant": "true", "custom_pub_topic": "smarthome/energy/iem2150-airco1/data/sec", "custom_pub_topic_avg": "smarthome/energy/iem2150-airco1/data/min"},
    {"type": "A9MEM2150", "modbus_id": 21, "name": "iem2150-airco2", "homeassistant": "true", "custom_pub_topic": "smarthome/energy/iem2150-airco2/data/sec", "custom_pub_topic_avg": "smarthome/energy/iem2150-airco2/data/min"},
    {"type": "ECR140D", "modbus_id": 25, "name": "ecr140d-unit1", "homeassistant": "true", "custom_pub_topic": "smarthome/energy/ecr140d-unit1/data/sec", "custom_pub_topic_avg": "smarthome/energy/ecr140d-unit1/data/min"},
    {"type": "ECR140D", "modbus_id": 26, "name": "ecr140d-unit2", "homeassistant": "true", "custom_pub_topic": "smarthome/energy/ecr140d-unit2/data/sec", "custom_pub_topic_avg": "smarthome/energy/ecr140d-unit2/data/min"}
]

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
        if not(index in self.valuestore.keys()):
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
# By default operates in Home Assistant MQTT Auto-Discovery mode. This can be disabled
# by setting the 'ha' parameter to False.
# If you also want to send the meter data to custom topics, provide the custom topics.
#  - topic is the "frequently updated" data (every 5 seconds)
#  - topic_avg is the "average per minute" data	
# It is possible to use either HA or custom topics, or both.

class MeterDataHandler():
    def __init__(self, meter, name, mqttclient, ha=True, topic=None, topic_avg=None):
        self.meter = meter
        self.name = name
        self.mqttclient = mqttclient
        self.topic = topic
        self.topic_avg = topic_avg
        self.ha = ha
        self.minute_data = PowerMeasurements()

        if self.ha:
            self.ha_id = f"{self.name}_{self.meter.modbus_id()}"
            self.ha_configtopic = f"homeassistant/sensor/{self.ha_id}/config"
            self.ha_statetopic = f"homeassistant/sensor/{self.ha_id}/state"
            self.publish_discovery()

    def publish_discovery(self):
        # Publish Home Assistant MQTT discovery messages

        # First message contains all the details for the device, we send POWER data
        discovery_payload = {
            "state_topic": self.ha_statetopic,
            "unit_of_measurement": "W",
            "value_template": "{{ value_json.power }}",
            "device": {
            "identifiers": [f"{self.ha_id}"],
            "name": self.meter.sys_metername().strip(),
            "model": self.meter.sys_metermodel().strip(),
            "manufacturer": self.meter.sys_manufacturer().strip()
            }
        }
        self.mqttclient.publish(self.ha_configtopic, json.dumps(discovery_payload), qos=1, retain=True)

    def pushMeasurements(self):
        measurements = {}
        measurements["timestamp"] = datetime.now().isoformat()

        ###################################################################
        # Voltages
        ###################################################################
        # First phase is always supported
        value = self.meter.md_voltage_L1_N()
        self.minute_data.add(MeasurementType.VOLTAGE_L1_N.valuename, value)
        measurements[MeasurementType.VOLTAGE_L1_N.valuename] = value

        # Add other metrics only for three-phase meters
        if self.meter.has_threephase():
            value = self.meter.md_voltage_L_L()
            self.minute_data.add(MeasurementType.VOLTAGE_L_L.valuename, value)
            measurements[MeasurementType.VOLTAGE_L_L.valuename] = value

            value = self.meter.md_voltage_L1_L2()
            self.minute_data.add(MeasurementType.VOLTAGE_L1_L2.valuename, value)
            measurements[MeasurementType.VOLTAGE_L1_L2.valuename] = value

            value = self.meter.md_voltage_L2_L3()
            self.minute_data.add(MeasurementType.VOLTAGE_L2_L3.valuename, value)
            measurements[MeasurementType.VOLTAGE_L2_L3.valuename] = value

            value = self.meter.md_voltage_L3_L1()
            self.minute_data.add(MeasurementType.VOLTAGE_L3_L1.valuename, value)
            measurements[MeasurementType.VOLTAGE_L3_L1.valuename] = value

            value = self.meter.md_voltage_L2_N()
            self.minute_data.add(MeasurementType.VOLTAGE_L2_N.valuename, value)
            measurements[MeasurementType.VOLTAGE_L2_N.valuename] = value

            value = self.meter.md_voltage_L3_N()
            self.minute_data.add(MeasurementType.VOLTAGE_L3_N.valuename, value)
            measurements[MeasurementType.VOLTAGE_L3_N.valuename] = value

        ###################################################################
        # Power
        ###################################################################
        value = self.meter.md_power()
        self.minute_data.add(MeasurementType.POWER.valuename, value)
        measurements[MeasurementType.POWER.valuename] = value

        if self.meter.has_threephase():
            value = self.meter.md_power_L1()
            self.minute_data.add(MeasurementType.POWER_L1.valuename, value)
            measurements[MeasurementType.POWER_L1.valuename] = value

            value = self.meter.md_power_L2()
            self.minute_data.add(MeasurementType.POWER_L2.valuename, value)
            measurements[MeasurementType.POWER_L2.valuename] = value

            value = self.meter.md_power_L3()
            self.minute_data.add(MeasurementType.POWER_L3.valuename, value)
            measurements[MeasurementType.POWER_L3.valuename] = value

        ###################################################################
        # Currents
        ###################################################################
        value = self.meter.md_current()
        self.minute_data.add(MeasurementType.CURRENT.valuename, value)
        measurements[MeasurementType.CURRENT.valuename] = value

        if self.meter.has_threephase():
            value = self.meter.md_current_L1()
            self.minute_data.add(MeasurementType.CURRENT_L1.valuename, value)
            measurements[MeasurementType.CURRENT_L1.valuename] = value

            value = self.meter.md_current_L2()
            self.minute_data.add(MeasurementType.CURRENT_L2.valuename, value)
            measurements[MeasurementType.CURRENT_L2.valuename] = value

            value = self.meter.md_current_L3()
            self.minute_data.add(MeasurementType.CURRENT_L3.valuename, value)
            measurements[MeasurementType.CURRENT_L3.valuename] = value

        ###################################################################
        # Other
        ###################################################################
        value = self.meter.md_powerfactor()
        self.minute_data.add(MeasurementType.POWER_FACTOR.valuename, value)
        measurements[MeasurementType.POWER_FACTOR.valuename] = value

        value = self.meter.md_frequency()
        self.minute_data.add(MeasurementType.FREQUENCY.valuename, value)
        measurements[MeasurementType.FREQUENCY.valuename] = value

        ###################################################################
        # Totals
        ###################################################################

        value = self.meter.ed_total()
        self.minute_data.set(MeasurementType.ENERGY_TOTAL.valuename, value)
        measurements[MeasurementType.ENERGY_TOTAL.valuename] = value

        value = self.meter.ed_total_export()
        self.minute_data.set(MeasurementType.ENERGY_TOTAL_EXPORT.valuename, value)
        measurements[MeasurementType.ENERGY_TOTAL_EXPORT.valuename] = value

        value = self.meter.ed_total_reactive_import()
        self.minute_data.set(MeasurementType.ENERGY_TOTAL_REACTIVE_IMPORT.valuename, value)
        measurements[MeasurementType.ENERGY_TOTAL_REACTIVE_IMPORT.valuename] = value

        value = self.meter.ed_total_reactive_export()
        self.minute_data.set(MeasurementType.ENERGY_TOTAL_REACTIVE_EXPORT.valuename, value)
        measurements[MeasurementType.ENERGY_TOTAL_REACTIVE_EXPORT.valuename] = value

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


# This pushes the data every 5 seconds for analytical purposes
def loop_5s(meters):
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

# The callback for when the client receives a CONNACK response from the server.
def mqtt_on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected to MQTT server with result code {reason_code}")

########################################################################################
### MAIN
########################################################################################

meters = []

def connect_modbus(master, logger):
    backoff = itertools.chain((1, 2, 4, 8, 16, 32, 64, 128, 256, 300), itertools.repeat(300))
    while True:
        try:
            master.open()
            logger.info("Connected to Modbus server")
            return
        except modbus_tk.modbus.ModbusError as exc:
            delay = next(backoff)
            logger.error("%s - Code=%d. Retrying in %d seconds...", exc, exc.get_exception_code(), delay)
            sleep(delay)

def connect_mqtt(mqttclient, logger):
    backoff = itertools.chain((1, 2, 4, 8, 16, 32, 64, 128, 256, 300), itertools.repeat(300))
    while True:
        try:
            mqttclient.connect(MQTT_SERVER, MQTT_PORT, 60)
            mqttclient.loop_start()
            logger.info("Connected to MQTT server")
            return
        except Exception as exc:
            delay = next(backoff)
            logger.error("MQTT connection failed: %s. Retrying in %d seconds...", exc, delay)
            sleep(delay)

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
        connect_modbus(master, logger)

    except modbus_tk.modbus.ModbusError as exc:
        logger.error("%s - Code=%d", exc, exc.get_exception_code())

    # Initialize MQTT
    mqttclient = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttclient.on_connect = mqtt_on_connect     # On connect handler
    connect_mqtt(mqttclient, logger)

    # Initialize meters - parse the METER_CONFIG, instantiate per found meter the correct class and meterhandler
    for meter_conf in METER_CONFIG:
        meter_class = meter_classes[meter_conf["type"]]
        meter = meter_class(master, meter_conf["modbus_id"])
        ha = meter_conf.get("homeassistant", "false").lower() == "true"
        meterhandler = MeterDataHandler(meter, meter_conf["name"], mqttclient, ha, meter_conf["custom_pub_topic"], meter_conf["custom_pub_topic_avg"])
        meters.append(meterhandler)

    # Initialize recurring task, our 'loop' function
    rt = repeatedtimer.RepeatedTimer(0, 5, loop_5s, meters)
    rt.first_start()

    rt2 = repeatedtimer.RepeatedTimer(60, 60, loop_60s, meters)
    rt2.first_start()

    try:
        while(True):
            sleep(5)
            if not master._is_opened:
                logger.warning("Modbus connection lost. Attempting to reconnect...")
                connect_modbus(master, logger)
            if not mqttclient.is_connected():
                logger.warning("MQTT connection lost. Attempting to reconnect...")
                connect_mqtt(mqttclient, logger)

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
