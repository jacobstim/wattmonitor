# WattMonitor
Docker container which reads electricy usage data over Modbus TCP and then posts the results to an MQTT server of choice.
 * Full support for Home Assistant MQTT Auto discovery
 * Supports the following devices:
     * Schneider Electric iEM3155
     * Schneider Electric iEM2150
     * Hager ECR 140D

Other devices can be almost trivially added if you know the Modbus registers. Pull Requests with new devices are highly appreciated!

## Usage
Currently, you still need to modify the following config parameters at the top of *wattmonitor.py*:

| Parameter | Description | Example |
| --------- | ----------- | ------- |
| MODBUS_SERVER | FQDN or IPv4 address of ModBus TCP server to connect to | 172.16.0.60 |
| MODBUS_PORT | TCP port to connect to, default for Modbus TCP is 502 | 502 |
| MQTT_SERVER | FQDN or IPv4 address of the MQTT server to connect to | mqtt.network.local |
| MQTT_PORT | TCP port to connect to, default for MQTT is 1883 | 1883 |
| METER_CONFIG | Array of meters to communicate with. Make sure you can read the data within 5 seconds or you will see horrible crashes :) | {"type": "A9MEM2150", "modbus_id": 20, "name": "iem2150-airco1", "homeassistant": "true", "custom_pub_topic": "smarthome/energy/iem2150-airco1/data/sec", "custom_pub_topic_avg": "smarthome/energy/iem2150-airco1/data/min"} |
