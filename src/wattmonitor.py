import paho.mqtt.client as mqtt
# Use the new modbus abstraction layer
from modbus.modbus_wrapper import create_modbus_master, connect_modbus, setup_modbus_logger
from time import sleep
from datetime import datetime
import logging
import json
import itertools
from enum import Enum
from meters.measurements import MeasurementType
from modbus.modbus_coordinator import initialize_coordinator
from single_thread_scheduler import SingleThreadScheduler

# Meters to use
from meters import iMEM3155, iMEM2150, ECR140D, CSMB

########################################################################################
### CONFIGURATION
########################################################################################

# Modbus TCP server to connect to
MODBUS_SERVER = "172.16.0.60"
MODBUS_PORT = 502

# Configuration: choose between "modbus_tk" and "pymodbus"
MODBUS_CLIENT_TYPE = "pymodbus" # "modbus_tk"  

# MQTT server to publish collected data to
MQTT_SERVER = "mqtt.home.local"
MQTT_PORT = 1883

# Meter configuration
# METER_CONFIG is a list of dictionaries, each dictionary contains the following keys:
#  - type: the meter type (A9MEM3155, A9MEM2150, ECR140D, CSMB)
#  - modbus_id: the modbus ID of the meter
#  - name: the friendly name of the meter (used mainly for Home Assistant)
#  - homeassistant: whether to use Home Assistant MQTT Auto-Discovery (true/false)
#       * This will publish the meter configuration to Home Assistant upon startup
#       * The meter data is also published every minute to Home Assistant
#       * Frequently updated meter data is never published to Home Assistant
#  - custom_pub_topic: the custom topic to publish the "frequently updated" data
#  - custom_pub_topic_avg: the custom topic to publish the "average per minute" data
#  - modbus_delay: delay in seconds after reading this meter (to prevent communication mix-ups)
#       * Default: 0.05 (50ms) for most meters
#       * CSMB: 0.15 (150ms) due to slower response times
# It is possible to use either HA or custom topics, or both.
# The custom topics are optional, if not provided, the meter data will not be published to custom topics.

METER_CONFIG = [
    {"type": "CSMB", "modbus_id": 30, "name": "csmb", "homeassistant": "true", "custom_pub_topic": "smarthome/energy/csmb/data/sec", "custom_pub_topic_avg": "smarthome/energy/csmb/data/min", "modbus_delay": 0.05},
    {"type": "A9MEM3155", "modbus_id": 10, "name": "iem3155", "homeassistant": "true", "custom_pub_topic": "smarthome/energy/iem3155/data/sec", "custom_pub_topic_avg": "smarthome/energy/iem3155/data/min", "modbus_delay": 0.05},
    {"type": "A9MEM2150", "modbus_id": 20, "name": "iem2150-airco1", "homeassistant": "true", "custom_pub_topic": "smarthome/energy/iem2150-airco1/data/sec", "custom_pub_topic_avg": "smarthome/energy/iem2150-airco1/data/min", "modbus_delay": 0.05},
    {"type": "A9MEM2150", "modbus_id": 21, "name": "iem2150-airco2", "homeassistant": "true", "custom_pub_topic": "smarthome/energy/iem2150-airco2/data/sec", "custom_pub_topic_avg": "smarthome/energy/iem2150-airco2/data/min", "modbus_delay": 0.05},
    {"type": "ECR140D", "modbus_id": 25, "name": "ecr140d-unit1", "homeassistant": "true", "custom_pub_topic": "smarthome/energy/ecr140d-unit1/data/sec", "custom_pub_topic_avg": "smarthome/energy/ecr140d-unit1/data/min", "modbus_delay": 0.05},
    {"type": "ECR140D", "modbus_id": 26, "name": "ecr140d-unit2", "homeassistant": "true", "custom_pub_topic": "smarthome/energy/ecr140d-unit2/data/sec", "custom_pub_topic_avg": "smarthome/energy/ecr140d-unit2/data/min", "modbus_delay": 0.05},
]

########################################################################################
### MAPPINGS
########################################################################################

# Mapping between meter types and corresponding classes
meter_classes = {
    "A9MEM3155": iMEM3155,
    "A9MEM2150": iMEM2150,
    "ECR140D": ECR140D,
    "CSMB": CSMB
}

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
            self.ha_statetopic = f"homeassistant/sensor/{self.ha_id}/state"
            self.publish_discovery()

    def publish_discovery(self):
        # Publish Home Assistant MQTT discovery messages for each supported measurement
        metername = self.meter.sys_metername().strip()
        metermodel = self.meter.sys_metermodel().strip()
        metermanufacturer = self.meter.sys_manufacturer().strip()

        for measurement in self.meter.supported_measurements():
            discovery_payload = {
                "state_topic": self.ha_statetopic,
                "unit_of_measurement": measurement.unit,
                "value_template": f"{{{{ value_json.{measurement.valuename} }}}}",
                "device": {
                    "identifiers": [f"{self.ha_id}"],
                    "name": metername,
                    "model": metermodel,
                    "manufacturer": metermanufacturer
                },
                "name": f"{self.name} {measurement.valuename}",
                "unique_id": f"{self.ha_id}_{measurement.valuename}"
            }
            config_topic = f"homeassistant/sensor/{self.ha_id}/{measurement.valuename}/config"
            logging.info(f"Posting Home Assistant MQTT Self-Discovery to: {config_topic}")
            self.mqttclient.publish(config_topic, json.dumps(discovery_payload), qos=1, retain=True)

    def pushMeasurements(self):
        measurements = {}
        measurements["timestamp"] = datetime.now().isoformat()

        supported_measurements = self.meter.supported_measurements()

        ###################################################################
        # Voltages
        ###################################################################
        # First phase is always supported
        if MeasurementType.VOLTAGE in supported_measurements:
            value = self.meter.md_voltage()
            self.minute_data.add(MeasurementType.VOLTAGE.valuename, value)
            measurements[MeasurementType.VOLTAGE.valuename] = value

        # Add other metrics only for three-phase meters
        if self.meter.has_threephase():
            if MeasurementType.VOLTAGE_L1_N in supported_measurements:
                value = self.meter.md_voltage_L1_N()
                self.minute_data.add(MeasurementType.VOLTAGE_L1_N.valuename, value)
                measurements[MeasurementType.VOLTAGE_L1_N.valuename] = value

            if MeasurementType.VOLTAGE_L_L in supported_measurements:
                value = self.meter.md_voltage_L_L()
                self.minute_data.add(MeasurementType.VOLTAGE_L_L.valuename, value)
                measurements[MeasurementType.VOLTAGE_L_L.valuename] = value

            if MeasurementType.VOLTAGE_L1_L2 in supported_measurements:
                value = self.meter.md_voltage_L1_L2()
                self.minute_data.add(MeasurementType.VOLTAGE_L1_L2.valuename, value)
                measurements[MeasurementType.VOLTAGE_L1_L2.valuename] = value

            if MeasurementType.VOLTAGE_L2_L3 in supported_measurements:
                value = self.meter.md_voltage_L2_L3()
                self.minute_data.add(MeasurementType.VOLTAGE_L2_L3.valuename, value)
                measurements[MeasurementType.VOLTAGE_L2_L3.valuename] = value

            if MeasurementType.VOLTAGE_L3_L1 in supported_measurements:
                value = self.meter.md_voltage_L3_L1()
                self.minute_data.add(MeasurementType.VOLTAGE_L3_L1.valuename, value)
                measurements[MeasurementType.VOLTAGE_L3_L1.valuename] = value

            if MeasurementType.VOLTAGE_L2_N in supported_measurements:
                value = self.meter.md_voltage_L2_N()
                self.minute_data.add(MeasurementType.VOLTAGE_L2_N.valuename, value)
                measurements[MeasurementType.VOLTAGE_L2_N.valuename] = value

            if MeasurementType.VOLTAGE_L3_N in supported_measurements:
                value = self.meter.md_voltage_L3_N()
                self.minute_data.add(MeasurementType.VOLTAGE_L3_N.valuename, value)
                measurements[MeasurementType.VOLTAGE_L3_N.valuename] = value

        ###################################################################
        # Power
        ###################################################################
        if MeasurementType.POWER in supported_measurements:
            value = self.meter.md_power()
            self.minute_data.add(MeasurementType.POWER.valuename, value)
            measurements[MeasurementType.POWER.valuename] = value

        if self.meter.has_threephase():
            if MeasurementType.POWER_L1 in supported_measurements:
                value = self.meter.md_power_L1()
                self.minute_data.add(MeasurementType.POWER_L1.valuename, value)
                measurements[MeasurementType.POWER_L1.valuename] = value

            if MeasurementType.POWER_L2 in supported_measurements:
                value = self.meter.md_power_L2()
                self.minute_data.add(MeasurementType.POWER_L2.valuename, value)
                measurements[MeasurementType.POWER_L2.valuename] = value

            if MeasurementType.POWER_L3 in supported_measurements:
                value = self.meter.md_power_L3()
                self.minute_data.add(MeasurementType.POWER_L3.valuename, value)
                measurements[MeasurementType.POWER_L3.valuename] = value

        if MeasurementType.POWER_REACTIVE in supported_measurements:
            value = self.meter.md_power_reactive()
            self.minute_data.add(MeasurementType.POWER_REACTIVE.valuename, value)
            measurements[MeasurementType.POWER_REACTIVE.valuename] = value

        if MeasurementType.POWER_APPARENT in supported_measurements:
            value = self.meter.md_power_apparent()
            self.minute_data.add(MeasurementType.POWER_APPARENT.valuename, value)
            measurements[MeasurementType.POWER_APPARENT.valuename] = value

        ###################################################################
        # Currents
        ###################################################################
        if MeasurementType.CURRENT in supported_measurements:
            value = self.meter.md_current()
            self.minute_data.add(MeasurementType.CURRENT.valuename, value)
            measurements[MeasurementType.CURRENT.valuename] = value

        if self.meter.has_threephase():
            if MeasurementType.CURRENT_L1 in supported_measurements:
                value = self.meter.md_current_L1()
                self.minute_data.add(MeasurementType.CURRENT_L1.valuename, value)
                measurements[MeasurementType.CURRENT_L1.valuename] = value

            if MeasurementType.CURRENT_L2 in supported_measurements:
                value = self.meter.md_current_L2()
                self.minute_data.add(MeasurementType.CURRENT_L2.valuename, value)
                measurements[MeasurementType.CURRENT_L2.valuename] = value

            if MeasurementType.CURRENT_L3 in supported_measurements:
                value = self.meter.md_current_L3()
                self.minute_data.add(MeasurementType.CURRENT_L3.valuename, value)
                measurements[MeasurementType.CURRENT_L3.valuename] = value

        ###################################################################
        # Other
        ###################################################################
        if MeasurementType.POWER_FACTOR in supported_measurements:
            value = self.meter.md_powerfactor()
            self.minute_data.add(MeasurementType.POWER_FACTOR.valuename, value)
            measurements[MeasurementType.POWER_FACTOR.valuename] = value

        if MeasurementType.FREQUENCY in supported_measurements:
            value = self.meter.md_frequency()
            self.minute_data.add(MeasurementType.FREQUENCY.valuename, value)
            measurements[MeasurementType.FREQUENCY.valuename] = value

        ###################################################################
        # Totals
        ###################################################################

        if MeasurementType.ENERGY_TOTAL in supported_measurements:
            value = self.meter.ed_total()
            self.minute_data.set(MeasurementType.ENERGY_TOTAL.valuename, value)
            measurements[MeasurementType.ENERGY_TOTAL.valuename] = value

        if MeasurementType.ENERGY_TOTAL_EXPORT in supported_measurements:
            value = self.meter.ed_total_export()
            self.minute_data.set(MeasurementType.ENERGY_TOTAL_EXPORT.valuename, value)
            measurements[MeasurementType.ENERGY_TOTAL_EXPORT.valuename] = value

        if MeasurementType.ENERGY_TOTAL_REACTIVE_IMPORT in supported_measurements:
            value = self.meter.ed_total_reactive_import()
            self.minute_data.set(MeasurementType.ENERGY_TOTAL_REACTIVE_IMPORT.valuename, value)
            measurements[MeasurementType.ENERGY_TOTAL_REACTIVE_IMPORT.valuename] = value

        if MeasurementType.ENERGY_TOTAL_REACTIVE_EXPORT in supported_measurements:
            value = self.meter.ed_total_reactive_export()
            self.minute_data.set(MeasurementType.ENERGY_TOTAL_REACTIVE_EXPORT.valuename, value)
            measurements[MeasurementType.ENERGY_TOTAL_REACTIVE_EXPORT.valuename] = value

        # If we are expected to publish to our separate topic, do so...
        if self.topic:
            # Convert to JSON
            jsondata = json.dumps(measurements)
            logging.info("---- JSON Data (topic: " + self.topic + ") ----------------------------------------\n" + jsondata)

            # Post to MQTT server
            self.mqttclient.publish(self.topic, payload = jsondata, qos=1)

    def pushAverageMeasurements(self):
         # Retrieve averages of past 60 minutes
        jsondata = self.minute_data.to_json()
        logging.info("Publishing minute averages...")
        # Post to MQTT server
        if self.topic_avg:
            logging.info("-> Published to: " + self.topic_avg)
            self.mqttclient.publish(self.topic_avg, payload = jsondata, qos=1)
        if self.ha:
            # In case we need to publish to HA, also do that
            logging.info("-> Published to: " + self.ha_statetopic)
            self.mqttclient.publish(self.ha_statetopic, payload = jsondata, qos=1)
        if self.topic_avg or self.ha:
            logging.info("---- Per minute data (topic: " + self.topic_avg + ") ---------------------------------\n" + jsondata)
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

# Monitor connections and reconnect if needed
def monitor_connections(master, mqttclient, logger):
    """Monitor Modbus and MQTT connections and reconnect if needed"""
    if not master.is_connected:
        logger.warning("Modbus connection lost. Attempting to reconnect...")
        connect_modbus(master, logger)
    if not mqttclient.is_connected():
        logger.warning("MQTT connection lost. Attempting to reconnect...")
        connect_mqtt(mqttclient, logger)

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
    logger = setup_modbus_logger(logging.INFO)
    
    try:
        # Configure Modbus TCP server using the new abstraction
        master = create_modbus_master(MODBUS_SERVER, MODBUS_PORT, MODBUS_CLIENT_TYPE)
        master.set_timeout(5.0)
        connect_modbus(master, logger)
        
        # Initialize the Modbus coordinator for thread-safe communication
        coordinator = initialize_coordinator(master._client)  # Pass the abstracted client
        
        # Set default inter-request delay
        coordinator.set_inter_request_delay(0.05)  # 50ms default between requests
        
    except Exception as exc:
        logger.error("Modbus initialization failed: %s", exc)
        return

    # Initialize MQTT
    mqttclient = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttclient.on_connect = mqtt_on_connect     # On connect handler
    connect_mqtt(mqttclient, logger)

    # Initialize meters - parse the METER_CONFIG, instantiate per found meter the correct class and meterhandler
    device_delays = {}  # Collect device-specific delays from configuration
    
    for meter_conf in METER_CONFIG:
        meter_class = meter_classes[meter_conf["type"]]
        meter = meter_class(master, meter_conf["modbus_id"])
        ha = meter_conf.get("homeassistant", "false").lower() == "true"
        meterhandler = MeterDataHandler(meter, meter_conf["name"], mqttclient, ha, meter_conf["custom_pub_topic"], meter_conf["custom_pub_topic_avg"])
        meters.append(meterhandler)
        
        # Collect modbus delay configuration for this meter
        modbus_delay = meter_conf.get("modbus_delay", 0.05)  # Default to 50ms if not specified
        device_delays[meter_conf["modbus_id"]] = modbus_delay
        logger.info(f"Configured meter {meter_conf['name']} (ID {meter_conf['modbus_id']}) with {modbus_delay*1000:.0f}ms delay")
    
    # Apply collected device delays to the coordinator
    coordinator.configure_device_delays(device_delays)
    #logger.info(f"Applied device-specific delays: {device_delays}")

    # Initialize single-threaded scheduler instead of multiple timers
    scheduler = SingleThreadScheduler()
    
    # Add tasks to scheduler
    scheduler.add_task("5_second_readings", 5.0, loop_5s, meters)
    scheduler.add_task("60_second_averages", 60.0, loop_60s, meters)
    scheduler.add_task("connection_monitor", 30.0, monitor_connections, master, mqttclient, logger)

    try:
        # Start the single-threaded scheduler - this blocks until stopped
        scheduler.start()

    except KeyboardInterrupt:
        logging.info('Stopping program!')

    finally:
        scheduler.stop()  # stop the scheduler
        mqttclient.loop_stop()  # stop the mqtt loop

########################################################################################
### ACTUAL MAIN
########################################################################################

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
	main()
