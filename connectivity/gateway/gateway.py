# [START my_gateway]
import argparse
import datetime
import logging
import os
import ssl
import sys
import time
import json

import jwt
import paho.mqtt.client as mqtt

# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# create console handler and set level to debug
ch = logging.StreamHandler()
# ch.setLevel(logging.DEBUG)

# CRITICAL > ERROR > WARNING > INFO > DEBUG

# create formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)


# The initial backoff time after a disconnection occurs, in seconds.
minimum_backoff_time = 1

# The maximum backoff time before giving up, in seconds.
MAXIMUM_BACKOFF_TIME = 32


import socket

HOST = ''
PORT = 10000
BUFSIZE = 2048
ADDR = (HOST, PORT)

udpSerSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
udpSerSock.setblocking(False)
udpSerSock.bind(ADDR)


class GatewayState:
    # Gateway ID
    gateway_id = None

    # for all PUBLISH messages which are waiting for PUBACK. The key is 'mid'
    # returned by publish().
    pending_responses = {}

    # SUBSCRIBE messages waiting for SUBACK. The key is 'mid' from Paho.
    pending_subscribes = {}

    # for all SUBSCRIPTIONS. The key is subscription topic.
    subscriptions = {}

    # Indicates if MQTT client is connected or not
    connected = False

gateway_state = GatewayState()


# [START iot_mqtt_jwt]
def create_jwt(project_id, private_key_file, algorithm, jwt_expires_minutes):
    """Creates a JWT (https://jwt.io) to establish an MQTT connection.
    Args:
       project_id: The cloud project ID this device belongs to
       private_key_file: A path to a file containing either an RSA256 or
                       ES256 private key.
       algorithm: The encryption algorithm to use. Either 'RS256' or 'ES256'
       jwt_expires_minutes: The time in minutes before the JWT expires.
    Returns:
        An MQTT generated from the given project_id and private key, which
        expires in 20 minutes. After 20 minutes, your client will be
        disconnected, and a new JWT will have to be generated.
    Raises:
        ValueError: If the private_key_file does not contain a known key.
    """

    token = {
            # The time that the token was issued at
            'iat': datetime.datetime.utcnow(),
            # The time the token expires.
            'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=jwt_expires_minutes),
            # The audience field should always be set to the GCP project id.
            'aud': project_id
    }

    # Read the private key file.
    with open(private_key_file, 'r') as f:
        private_key = f.read()

    logger.info('Creating JWT using {} from private key file {}'.format(
            algorithm, private_key_file))

    return jwt.encode(token, private_key, algorithm=algorithm)
# [END iot_mqtt_jwt]


# [START iot_mqtt_config]
def error_str(rc):
    """Convert a Paho error to a human readable string."""
    return '{}: {}'.format(rc, mqtt.error_string(rc))


def on_connect(unused_client, unused_userdata, unused_flags, rc):
    """Callback for when a device connects."""
    logger.info('on_connect {}'.format(mqtt.connack_string(rc)))

    # After a successful connect, reset backoff time and stop backing off.
    global minimum_backoff_time
    minimum_backoff_time = 1

    gateway_state.connected = True

def on_disconnect(unused_client, unused_userdata, rc):
    """Paho callback for when a device disconnects."""
    logger.info('on_disconnect {}'.format(error_str(rc)))

    gateway_state.connected = False

    # Since a disconnect occurred, the next loop iteration will wait with
    # exponential backoff.
    global minimum_backoff_time
    while True:
        # If backoff time is too large, give up.
        if minimum_backoff_time > MAXIMUM_BACKOFF_TIME:
            logger.info('Exceeded maximum backoff time. Giving up.')
            break

        delay = minimum_backoff_time + random.randint(0, 1000) / 1000.0
        time.sleep(delay)
        minimum_backoff_time *= 2
        client.connect(mqtt_bridge_hostname, mqtt_bridge_port)


def on_publish(unused_client, unused_userdata, unused_mid):
    """Paho callback when a message is sent to the broker."""
    logger.debug('on_publish \'{}\''.format(unused_mid))


def on_message(unused_client, unused_userdata, message):
    """Callback when the device receives a message on a subscription."""
    payload = str(message.payload.decode('utf-8'))
    logger.info('Received message \'{}\' on topic \'{}\' with Qos {}'.format(
        payload, message.topic, str(message.qos)))

    
    try:
        # Relaying to the device
        client_addr = gateway_state.subscriptions[message.topic]

        if client_addr != gateway_state.gateway_id:
            # Having fun with 'DISP: <Text>' command
            if payload.startswith('DISP'):
                payload = "d_{}".format(payload)
            
            logger.info('Relaying config[{}] to {}'.format(payload, client_addr))
            udpSerSock.sendto(payload.encode('utf8'), client_addr)
        

    except KeyError:
        logger.info('Nobody subscribes to topic {}'.format(message.topic))

def on_subscribe(unused_client, unused_userdata, mid, granted_qos):
    logger.debug('on_subscribe success: mid {}, qos {}'.format(mid, granted_qos))

# This client is the gateway
def get_client(
        project_id, cloud_region, registry_id, device_id, private_key_file,
        algorithm, ca_certs, mqtt_bridge_hostname, mqtt_bridge_port, 
        jwt_expires_minutes):
    """Create our MQTT client. The client_id is a unique string that identifies
    this device. For Google Cloud IoT Core, it must be in the format below."""
    client_id=('projects/{}/locations/{}/registries/{}/devices/{}'.format(
            project_id,
            cloud_region,
            registry_id,
            device_id))
    logger.info('Gateway client_id is \'{}\''.format(client_id))
    client = mqtt.Client(client_id=client_id)

    # With Google Cloud IoT Core, the username field is ignored, and the
    # password field is used to transmit a JWT to authorize the device.
    client.username_pw_set(
            username='unused',
            password=create_jwt(
                    project_id, private_key_file, algorithm, jwt_expires_minutes))

    # Enable SSL/TLS support.
    client.tls_set(ca_certs=ca_certs, tls_version=ssl.PROTOCOL_TLSv1_2)

    # Register message callbacks. https://eclipse.org/paho/clients/python/docs/
    # describes additional callbacks that Paho supports. In this example, the
    # callbacks just print to standard out.
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.on_subscribe = on_subscribe

    # Connect to the Google MQTT bridge.
    client.connect(mqtt_bridge_hostname, mqtt_bridge_port)

    # Subscribe to the config topic.
    # This is the topic that the device will receive configuration updates on.
    mqtt_config_topic = '/devices/{}/config'.format(device_id)
    try:
        result, mid = client.subscribe(mqtt_config_topic, qos=1)
        gateway_state.subscriptions[mqtt_config_topic] = device_id
        logger.info('[Subscription] Subscribing to {} - qos {}, with mid {}'.format(mqtt_config_topic, 1, mid))
    except:   #ValueError
        logger.info("Error with Subscription mid {}".format(mid))

    # Subscribe to the commands topic
    # The topic that the device will receive commands on.
    mqtt_command_topic = '/devices/{}/commands/#'.format(device_id)    
    try:
        result, mid = client.subscribe(mqtt_command_topic, qos=0)
        gateway_state.subscriptions[mqtt_config_topic[:-2]] = device_id
        logger.info('[Subscription] Subscribing to {} - qos {}, with mid {}'.format(mqtt_command_topic, 0, mid))
    except:   #ValueError
        logger.info("Error with Subscription mid {}".format(mid))
    

    return client
# [END iot_mqtt_config]

# [START iot_attach_device]
def attach_device(client, device_id, auth):
    """Attach the device to the gateway."""
    attach_topic = '/devices/{}/attach'.format(device_id)
    attach_payload = '{{"authorization" : "{}"}}'.format(auth)
    try:
        result, mid = client.publish(attach_topic, attach_payload, qos=1)
        logger.info('[Attachment] Publishing to {} - payload {} - qos {}, with mid {}'.format(attach_topic, attach_payload, 1, mid))
    except:   #ValueError
        logger.info("Error with Attachment - mid {}".format(mid))
# [END iot_attach_device]

# [START iot_detach_device]
def detach_device(client, device_id):
    """Detach the device from the gateway."""
    detach_topic = '/devices/{}/detach'.format(device_id)
    try:
        result, mid = client.publish(detach_topic, "{}", qos=1)
        logger.info('[Detachment] Publishing to {} - qos {}, with mid {}'.format(detach_topic, 1, mid))
    except:   #ValueError
        logger.info("Error with Detachment - mid {}".format(mid))
# [END iot_detach_device]

# [START subscribe_device]
def subscribe_device(client, device_id, client_addr):
    # Subscribe to the config topic.
    # This is the topic that the device will receive configuration updates on.
    mqtt_config_topic = '/devices/{}/config'.format(device_id)
    try:
        result, mid = client.subscribe(mqtt_config_topic, qos=1)
        gateway_state.subscriptions[mqtt_config_topic] = client_addr        # Remember the corresponding device
        logger.info('[Subscription] Subscribing to {} - qos {}, with mid {}'.format(mqtt_config_topic, 1, mid))
    except:   #ValueError
        logger.info("Error with Subscription mid {}".format(mid))


    # Subscribe to the commands topic
    # The topic that the device will receive commands on.
    mqtt_command_topic = '/devices/{}/commands/#'.format(device_id)    
    try:
        result, mid = client.subscribe(mqtt_command_topic, qos=0)
        gateway_state.subscriptions[mqtt_command_topic[:-2]] = client_addr        # Remember the corresponding device
        logger.info('[Subscription] Subscribing to {} - qos {}, with mid {}'.format(mqtt_command_topic, 0, mid))
    except:   #ValueError
        logger.info("Error with Subscription mid {}".format(mid))
# [END subscribe_device]

# [START sendevent_device]
def sendevent_device(client, device_id, payload):
    # This is the topic that the device will send events to
    mqtt_topic = '/devices/{}/events'.format(device_id)
    mid = -1
    try:
        result, mid = client.publish(mqtt_topic, payload, qos=0)
        logger.info('[Publishing Event] Publishing to {} - payload {} - qos {}, with mid {}'.format(mqtt_topic, payload, 0, mid))
    except:   #ValueError
        logger.info("Error with Publishing Event to {} - mid {}".format(mqtt_topic, mid))
# [END sendevent_device]



# [START parse_command_line_args]
def parse_command_line_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description=(
        'Example Google Cloud IoT Core MQTT device connection code.'))
    parser.add_argument(
        '--project_id',
        default=os.environ.get('GOOGLE_CLOUD_PROJECT'),
        help='GCP cloud project name')
    parser.add_argument(
        '--registry_id', required=True,
        help='Cloud IoT Core registry id')
    parser.add_argument(
        '--gateway_id', required=True, help='Cloud IoT Core gateway id')
    parser.add_argument(
        '--private_key_file',
        required=True, help='Path to private key file.')
    parser.add_argument(
        '--algorithm',
        choices=('RS256', 'ES256'),
        required=True,
        help='Which encryption algorithm to use to generate the JWT.')
    parser.add_argument(
        '--cloud_region', default='us-central1',
        help='GCP cloud region')
    parser.add_argument(
        '--ca_certs',
        default='roots.pem',
        help=('CA root from https://pki.google.com/roots.pem'))
    parser.add_argument(
        '--mqtt_bridge_hostname',
        default='mqtt.googleapis.com',
        help='MQTT bridge hostname.')
    parser.add_argument(
        '--mqtt_bridge_port',
        choices=(8883, 443),
        default=8883,
        type=int,
        help='MQTT bridge port.')
    parser.add_argument(
        '--jwt_expires_minutes',
        default=1200,
        type=int,
        help=('Expiration time, in minutes, for JWT tokens.'))

    return parser.parse_args()
# [END parse_command_line_args]

# [START iot_mqtt_run]
def main():
    global gateway_state

    args = parse_command_line_args()

    client = get_client(
        args.project_id, args.cloud_region, args.registry_id, args.gateway_id,
        args.private_key_file, args.algorithm, args.ca_certs,
        args.mqtt_bridge_hostname, args.mqtt_bridge_port,
        args.jwt_expires_minutes)

    gateway_state.gateway_id = args.gateway_id

    jwt_iat = datetime.datetime.utcnow()
    jwt_exp_mins = args.jwt_expires_minutes

    while True:
        client.loop()        

        # Loop until gateway is connected
        if gateway_state.connected is False:
            logger.info('connect status {}'.format(gateway_state.connected))
            time.sleep(1)
            continue

        # Gateway connected!
        # Any data receiving from the device?
        try:
            data, client_addr = udpSerSock.recvfrom(BUFSIZE)
        except socket.error:
            continue
        logger.info('From Address {}:{} receive data: {}'.format(
                client_addr[0], client_addr[1], data.decode("utf-8")))

        # Yes, receive something from device
        command = json.loads(data.decode('utf-8'))
        if not command:
            logger.info('invalid json command {}'.format(data))
            continue

        action = command["action"]
        device_id = command["device"]
        template = '{{ "device": "{}", "command": "{}", "status" : "ok" }}'

        if action == 'attach':
            auth = ''  # TODO:    auth = command["jwt"]
            attach_device(client, device_id, auth)

            # Reply to the device
            message = template.format(device_id, action)
            logger.debug('Sending data over UDP {} {}'.format(client_addr, message))
            udpSerSock.sendto(message.encode('utf8'), client_addr)
        elif action == 'detach':
            detach_device(client, device_id)

            # Reply to the device
            message = template.format(device_id, action)
            logger.debug('Sending data over UDP {} {}'.format(client_addr, message))
            udpSerSock.sendto(message.encode('utf8'), client_addr)
        elif action == 'subscribe':
            subscribe_device(client, device_id, client_addr)

            # Reply to the device
            message = template.format(device_id, action)
            logger.debug('Sending data over UDP {} {}'.format(client_addr, message))
            udpSerSock.sendto(message.encode('utf8'), client_addr)
        elif action == 'event':
            payload = "{}".format(json.dumps(command["data"]))      # To get double_quote in json output
            sendevent_device(client, device_id, payload)

            # Reply to the device
            message = template.format(device_id, action)
            logger.debug('Sending data over UDP {} {}'.format(client_addr, message))
            udpSerSock.sendto(message.encode('utf8'), client_addr)
        else:
            logger.info('undefined action: {}'.format(action))


        # Refresh token if needed
        seconds_since_issue = (datetime.datetime.utcnow() - jwt_iat).seconds
        if seconds_since_issue > 60 * jwt_exp_mins:
            logger.info('Refreshing token after {}s'.format(seconds_since_issue))
            jwt_iat = datetime.datetime.utcnow()
            client.loop()
            client.disconnect()
            client = get_client(
                    args.project_id, args.cloud_region, args.registry_id, args.gateway_id,
                    args.private_key_file, args.algorithm, args.ca_certs,
                    args.mqtt_bridge_hostname, args.mqtt_bridge_port,
                    args.jwt_expires_minutes)

        time.sleep(1)


    logger.info('Finished.')
    # [END iot_listen_for_messages]


if __name__ == '__main__':
    main()

# [END my_gateway]