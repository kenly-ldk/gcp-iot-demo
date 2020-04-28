from __future__ import print_function

import socket
import fcntl, os
import errno

import sys
import time

from sense_hat import SenseHat
sh = SenseHat()

ADDR = ''          # Edit the gateway address here
PORT = 10000

# Create a UDP socket
client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
server_address = (ADDR, PORT)
fcntl.fcntl(client_sock, fcntl.F_SETFL, os.O_NONBLOCK)


import logging

# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# CRITICAL > ERROR > WARNING > INFO > DEBUG

# create formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)


# Input Parameters from command line
device_id = sys.argv[1]
num_messages = int(sys.argv[2])
enable_accelerometer = sys.argv[3]

if not device_id:
    sys.exit('The device id must be specified.')

logger.info('Bringing up device {}'.format(device_id))


def SendCommand(sock, message):
    logger.debug('Sending "{}"'.format(message))
    sock.sendto(message.encode(), server_address)

    # Receive response
    logger.debug('waiting for response')
    while True:
        try:
            response = sock.recv(4096)
        except socket.error as e:
            err = e.args[0]
            if err == errno.EAGAIN or err == errno.EWOULDBLOCK:
                continue
            else:
                # a "real" error occurred
                logger.info(e)
                sys.exit(1)
        else:
            break

    logger.debug('Received: "{}"'.format(response))

    return response


def MakeMessage(device_id, action, data=''):
    if data:
        return '{{ "device" : "{}", "action":"{}", "data" : {} }}'.format(
            device_id, action, data)
    else:
        return '{{ "device" : "{}", "action":"{}" }}'.format(
            device_id, action)


def RunAction(action, data=''):
    global client_sock
    message = MakeMessage(device_id, action, data)
    if not message:
        return
    logger.info('Send data: {} '.format(message))
    event_response = SendCommand(client_sock, message)
    logger.debug('Response: {}'.format(event_response))


try:
    RunAction('attach')
    
    time.sleep(3)
    RunAction('subscribe')

    time.sleep(3)
    for i in range(0, num_messages):
        RunAction('event', '"Sending message #{}"'.format(i))
        time.sleep(1)

    response = None
    while True:
        try:
            response = client_sock.recv(4096).decode('utf8')
        except socket.error as e:
            err = e.args[0]
            if err == errno.EAGAIN or err == errno.EWOULDBLOCK:
                ### Non-blocking operations
                if enable_accelerometer == "true":
                    # accelerometer
                    acceleration = sh.get_accelerometer_raw()
                    x = acceleration['x']
                    y = acceleration['y']
                    z = acceleration['z']

                    x=round(x, 0)
                    y=round(y, 0)
                    z=round(z, 0)

                    logger.debug("x={0}, y={1}, z={2}".format(x, y, z))
                    RunAction('event', '{{ "device_id": "{}", "event_time": "{}", "raw_accelerometer_data": "x={}, y={}, z={}" }}'.format(device_id, time.ctime(), x, y, z))
            else:
                # a "real" error occurred
                logger.info(e)
                sys.exit(1)
        else:
            logger.info('Client received {{{}}}'.format(response))
            
            # Display from command
            if response.startswith("d_DISP"):
                display_content = response[8:]
                sh.show_message(display_content) #text_colour=yellow, back_colour=blue, scroll_speed=0.05
                sh.clear()

finally:
    RunAction('detach')

    logger.info('closing socket', file=sys.stderr)
    client_sock.close()
