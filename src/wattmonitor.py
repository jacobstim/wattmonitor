import paho.mqtt.client as mqtt
# Use the new modbus abstraction layer
from modbus.modbus_wrapper import create_modbus_master, connect_modbus, setup_modbus_logger
from time import sleep
from datetime import datetime
import logging
import json
import itertools
from enum import Enum
from meters.measurements import MeasurementType, MeasurementFrequency
from modbus.modbus_coordinator import initialize_coordinator, ModbusTCPConnectionError
from single_thread_scheduler import SingleThreadScheduler

# Meters to use
from meters import iMEM3155, iMEM2150, ECR140D, CSMB

########################################################################################
### MEASUREMENT CONFIGURATION
########################################################################################
# List of measurements to perform for every meter (if the meter supports it)
# Also defines the publishing frequency of the measurement:
# - FAST: every 5 seconds published 
# - SLOW: every 60 seconds published 
# This allows to send critical measurements (e.g. current, voltage, power) frequently,
# while less critical measurements (e.g. energy totals) can be sent less frequently.
# The mqtt topic publishes to the MQTT server, while the "ha" topic is used for Home Assistant MQTT
# publishing. 
#
# This allows to publish a lot of data to a generic MQTT topic (for whatever purpose), 
# and avoids Home Assistant of being flooded with data every few seconds for every measurement.
# In our case, we want to publish CURRENT measurements fast to HA (because this controls EV charging),
# but the other data can just come in every minute for dashboarding purposes.

MEASUREMENTS_TO_PERFORM = {
    MeasurementType.VOLTAGE: {"mqtt": MeasurementFrequency.FAST, "ha": MeasurementFrequency.SLOW},
    MeasurementType.VOLTAGE_L1_N: {"mqtt": MeasurementFrequency.FAST, "ha": MeasurementFrequency.SLOW},
    MeasurementType.VOLTAGE_L_L: {"mqtt": MeasurementFrequency.FAST, "ha": MeasurementFrequency.SLOW},
    MeasurementType.VOLTAGE_L1_L2: {"mqtt": MeasurementFrequency.FAST, "ha": MeasurementFrequency.SLOW},
    MeasurementType.VOLTAGE_L2_L3: {"mqtt": MeasurementFrequency.FAST, "ha": MeasurementFrequency.SLOW},
    MeasurementType.VOLTAGE_L3_L1: {"mqtt": MeasurementFrequency.FAST, "ha": MeasurementFrequency.SLOW},
    MeasurementType.VOLTAGE_L2_N: {"mqtt": MeasurementFrequency.FAST, "ha": MeasurementFrequency.SLOW},
    MeasurementType.VOLTAGE_L3_N: {"mqtt": MeasurementFrequency.FAST, "ha": MeasurementFrequency.SLOW},
    MeasurementType.POWER: {"mqtt": MeasurementFrequency.FAST, "ha": MeasurementFrequency.SLOW},
    MeasurementType.POWER_L1: {"mqtt": MeasurementFrequency.FAST, "ha": MeasurementFrequency.SLOW},
    MeasurementType.POWER_L2: {"mqtt": MeasurementFrequency.FAST, "ha": MeasurementFrequency.SLOW},
    MeasurementType.POWER_L3: {"mqtt": MeasurementFrequency.FAST, "ha": MeasurementFrequency.SLOW},
    MeasurementType.POWER_REACTIVE: {"mqtt": MeasurementFrequency.FAST, "ha": MeasurementFrequency.SLOW},
    MeasurementType.POWER_APPARENT: {"mqtt": MeasurementFrequency.FAST, "ha": MeasurementFrequency.SLOW},
    MeasurementType.CURRENT: {"mqtt": MeasurementFrequency.FAST, "ha": MeasurementFrequency.FAST},
    MeasurementType.CURRENT_L1: {"mqtt": MeasurementFrequency.FAST, "ha": MeasurementFrequency.FAST},
    MeasurementType.CURRENT_L2: {"mqtt": MeasurementFrequency.FAST, "ha": MeasurementFrequency.FAST},
    MeasurementType.CURRENT_L3: {"mqtt": MeasurementFrequency.FAST, "ha": MeasurementFrequency.FAST},
    MeasurementType.POWER_FACTOR: {"mqtt": MeasurementFrequency.SLOW, "ha": MeasurementFrequency.SLOW},
    MeasurementType.FREQUENCY: {"mqtt": MeasurementFrequency.FAST, "ha": MeasurementFrequency.SLOW},
    MeasurementType.ENERGY_TOTAL: {"mqtt": MeasurementFrequency.FAST, "ha": MeasurementFrequency.SLOW},
    MeasurementType.ENERGY_TOTAL_EXPORT: {"mqtt": MeasurementFrequency.FAST, "ha": MeasurementFrequency.SLOW},
    MeasurementType.ENERGY_TOTAL_REACTIVE_IMPORT: {"mqtt": MeasurementFrequency.FAST, "ha": MeasurementFrequency.SLOW},
    MeasurementType.ENERGY_TOTAL_REACTIVE_EXPORT: {"mqtt": MeasurementFrequency.FAST, "ha": MeasurementFrequency.SLOW},
}

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
#       * The meter data is also published every 5 seconds to Home Assistant
#       * Frequently updated meter data is never published to Home Assistant
#  - custom_pub_topic: the custom topic to publish the "frequently updated" data
#  - custom_pub_topic_avg: the custom topic to publish the "average per minute" data
#  - modbus_delay: delay in seconds after reading this meter (to prevent communication mix-ups)
#       * Default: 0.05 (50ms) for most meters
# It is possible to use either HA or custom topics, or both.
# The custom topics are optional, if not provided, the meter data will not be published to custom topics.

METER_CONFIG = [
    {"type": "CSMB", "modbus_id": 30, "name": "csmb", "homeassistant": "true", "custom_pub_topic": "smarthome/energy/csmb/data/sec", "custom_pub_topic_avg": "smarthome/energy/csmb/data/min", "modbus_delay": 0},
    {"type": "A9MEM3155", "modbus_id": 10, "name": "iem3155", "homeassistant": "true", "custom_pub_topic": "smarthome/energy/iem3155/data/sec", "custom_pub_topic_avg": "smarthome/energy/iem3155/data/min", "modbus_delay": 0},
    {"type": "A9MEM2150", "modbus_id": 20, "name": "iem2150-airco1", "homeassistant": "true", "custom_pub_topic": "smarthome/energy/iem2150-airco1/data/sec", "custom_pub_topic_avg": "smarthome/energy/iem2150-airco1/data/min", "modbus_delay": 0},
    {"type": "A9MEM2150", "modbus_id": 21, "name": "iem2150-airco2", "homeassistant": "true", "custom_pub_topic": "smarthome/energy/iem2150-airco2/data/sec", "custom_pub_topic_avg": "smarthome/energy/iem2150-airco2/data/min", "modbus_delay": 0},
    {"type": "ECR140D", "modbus_id": 25, "name": "ecr140d-unit1", "homeassistant": "true", "custom_pub_topic": "smarthome/energy/ecr140d-unit1/data/sec", "custom_pub_topic_avg": "smarthome/energy/ecr140d-unit1/data/min", "modbus_delay": 0},
    {"type": "ECR140D", "modbus_id": 26, "name": "ecr140d-unit2", "homeassistant": "true", "custom_pub_topic": "smarthome/energy/ecr140d-unit2/data/sec", "custom_pub_topic_avg": "smarthome/energy/ecr140d-unit2/data/min", "modbus_delay": 0},
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
    def __init__(self):
        # We store in the dictionary:
        # * key = measurement name
        # * value = tuple (count, totalvalue)
        self.valuestore = {}

    def clear(self):
        self.valuestore.clear()

    def add(self, index, value):
        if not(index in self.valuestore.keys()):
            self.valuestore[index] = [0, 0]
        self.valuestore[index][0] += 1
        try:
            self.valuestore[index][1] += value
        except TypeError as e:
            # Add debugging information to help track down the source of the error
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"TypeError in PowerMeasurements.add(): index='{index}', value={value} (type: {type(value)}), error: {e}")
            logger.error(f"Current valuestore[{index}] = {self.valuestore[index]}")
            logger.error(f"Trying to add {type(value)} to {type(self.valuestore[index][1])}")
            raise

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

    def doMeasurements(self):
        """Perform all configured measurements and return a dictionary of results"""
        measurements = {}
        measurements["timestamp"] = datetime.now().isoformat()
        
        # Perform batch reads first to populate cache for all subsequent individual reads
        # This dramatically reduces Modbus communication overhead
        batch_measurements = self.meter.read_all_measurements()
        logging.debug(f"Batch read completed for {self.name}: {len(batch_measurements)} measurements cached")

        ###################################################################
        # Process all configured measurements
        ###################################################################
        supported_measurements = self.meter.supported_measurements()

        for measurement_type in MEASUREMENTS_TO_PERFORM:
            # Skip unsupported measurements
            if measurement_type not in supported_measurements:
                continue
            
            # Skip three-phase specific measurements for single-phase meters
            if measurement_type in MeasurementType.get_three_phase_measurements() and not self.meter.has_threephase():
                continue

            # Read the measurement value based on type
            if measurement_type == MeasurementType.VOLTAGE:
                value = self.meter.md_voltage()
            elif measurement_type == MeasurementType.VOLTAGE_L1_N:
                value = self.meter.md_voltage_L1_N()
            elif measurement_type == MeasurementType.VOLTAGE_L_L:
                value = self.meter.md_voltage_L_L()
            elif measurement_type == MeasurementType.VOLTAGE_L1_L2:
                value = self.meter.md_voltage_L1_L2()
            elif measurement_type == MeasurementType.VOLTAGE_L2_L3:
                value = self.meter.md_voltage_L2_L3()
            elif measurement_type == MeasurementType.VOLTAGE_L3_L1:
                value = self.meter.md_voltage_L3_L1()
            elif measurement_type == MeasurementType.VOLTAGE_L2_N:
                value = self.meter.md_voltage_L2_N()
            elif measurement_type == MeasurementType.VOLTAGE_L3_N:
                value = self.meter.md_voltage_L3_N()
            elif measurement_type == MeasurementType.POWER:
                value = self.meter.md_power()
            elif measurement_type == MeasurementType.POWER_L1:
                value = self.meter.md_power_L1()
            elif measurement_type == MeasurementType.POWER_L2:
                value = self.meter.md_power_L2()
            elif measurement_type == MeasurementType.POWER_L3:
                value = self.meter.md_power_L3()
            elif measurement_type == MeasurementType.POWER_REACTIVE:
                value = self.meter.md_power_reactive()
            elif measurement_type == MeasurementType.POWER_APPARENT:
                value = self.meter.md_power_apparent()
            elif measurement_type == MeasurementType.CURRENT:
                value = self.meter.md_current()
            elif measurement_type == MeasurementType.CURRENT_L1:
                value = self.meter.md_current_L1()
            elif measurement_type == MeasurementType.CURRENT_L2:
                value = self.meter.md_current_L2()
            elif measurement_type == MeasurementType.CURRENT_L3:
                value = self.meter.md_current_L3()
            elif measurement_type == MeasurementType.POWER_FACTOR:
                value = self.meter.md_powerfactor()
            elif measurement_type == MeasurementType.FREQUENCY:
                value = self.meter.md_frequency()
            elif measurement_type == MeasurementType.ENERGY_TOTAL:
                value = self.meter.ed_total()
            elif measurement_type == MeasurementType.ENERGY_TOTAL_EXPORT:
                value = self.meter.ed_total_export()
            elif measurement_type == MeasurementType.ENERGY_TOTAL_REACTIVE_IMPORT:
                value = self.meter.ed_total_reactive_import()
            elif measurement_type == MeasurementType.ENERGY_TOTAL_REACTIVE_EXPORT:
                value = self.meter.ed_total_reactive_export()
            else:
                continue

            # Store the measurement for later average computation in slow loop
            if measurement_type in {MeasurementType.ENERGY_TOTAL, MeasurementType.ENERGY_TOTAL_EXPORT,
                                  MeasurementType.ENERGY_TOTAL_REACTIVE_IMPORT, MeasurementType.ENERGY_TOTAL_REACTIVE_EXPORT}:
                self.minute_data.set(measurement_type.valuename, value)
            else:
                self.minute_data.add(measurement_type.valuename, value)
            measurements[measurement_type.valuename] = value
        
        return measurements

    def pushMeasurements(self):
        """Perform all configured measurements and publish them to MQTT topics"""
        try:
            # Perform all configured measurements
            measurements = self.doMeasurements()
                
        except ModbusTCPConnectionError as e:
            logging.error(f"TCP connection error during measurements for {self.name}: {e}")
            # Mark the modbus connection as disconnected
            if hasattr(self.meter, '_modbus') and hasattr(self.meter._modbus, 'mark_disconnected'):
                self.meter._modbus.mark_disconnected()
            return  # Skip publishing when TCP connection fails

        except Exception as e:
            logging.error(f"Error reading measurements for {self.name}: {e}")
            return

        # Filter measurements based on frequency for each topic
        mqtt_measurements = {"timestamp": measurements["timestamp"]}
        ha_measurements = {"timestamp": measurements["timestamp"]}
        
        for measurement_type in MEASUREMENTS_TO_PERFORM:
            measurement_name = measurement_type.valuename
            if measurement_name in measurements:
                # Check frequency for self.topic
                if MEASUREMENTS_TO_PERFORM[measurement_type]["mqtt"] == MeasurementFrequency.FAST:
                    mqtt_measurements[measurement_name] = measurements[measurement_name]
                
                # Check frequency for self.ha_statetopic
                if MEASUREMENTS_TO_PERFORM[measurement_type]["ha"] == MeasurementFrequency.FAST:
                    ha_measurements[measurement_name] = measurements[measurement_name]

        # If we are expected to publish to our separate topic, do so...
        if self.topic and len(mqtt_measurements) > 1:
            # Convert to JSON
            jsondata = json.dumps(mqtt_measurements)
            logging.info("---- JSON Data ----------------------------------------------------------------------------\n" + jsondata)
            # Post to MQTT server
            logging.info("-> Published to: " + self.topic)
            self.mqttclient.publish(self.topic, payload = jsondata, qos=1)

        if self.ha and len(ha_measurements) > 1:
            # Convert to JSON
            jsondata = json.dumps(ha_measurements)
            logging.info("---- JSON Data ----------------------------------------------------------------------------\n" + jsondata)
            # In case we need to publish to HA, also do that
            logging.info("-> Published to: " + self.ha_statetopic)
            self.mqttclient.publish(self.ha_statetopic, payload = jsondata, qos=1)

    def pushAverageMeasurements(self):
         # Retrieve averages of past 60 minutes
        jsondata = self.minute_data.to_json()
        if self.topic_avg or self.ha:
            logging.info("---- Per minute data (topic: " + self.topic_avg + ") --------------------------------------\n" + jsondata)

        # Post to MQTT server
        if self.topic_avg:
            logging.info("-> Published to: " + self.topic_avg)
            self.mqttclient.publish(self.topic_avg, payload = jsondata, qos=1)

        # Clear and restart
        self.minute_data.clear()   

########################################################################################
### LOOP
########################################################################################


# This pushes the data every 5 seconds for analytical purposes (FAST loop)
def loop_fast(meters):
    # Skip if no connection
    if not meters or not meters[0].meter._modbus.is_connected:
        logging.debug("Skipping fast loop - Modbus connection not available")
        return
    
    # Read the secondly data for every meter and send it
    for meterhandler in meters:
        try:
            meterhandler.pushMeasurements()
        except Exception as e:
            logging.error(f"Error reading meter {meterhandler.name}: {e}")


# This publishes average data every 60 seconds for dashboarding purposes
def loop_slow(meters):
    # Skip if no connection
    if not meters or not meters[0].meter._modbus.is_connected:
        logging.debug("Skipping slow loop - Modbus connection not available")
        return
    
    # Send the minute average data
    for meterhandler in meters:
        try:
            meterhandler.pushAverageMeasurements()
        except Exception as e:
            logging.error(f"Error publishing averages for meter {meterhandler.name}: {e}")

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
        coordinator.set_inter_request_delay(0)          # No delay between requests by default
        
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
    scheduler.add_task("fast_readings", 2, loop_fast, meters)
    scheduler.add_task("compute_averages", 60.0, loop_slow, meters)
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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

if __name__ == '__main__':
	main()
