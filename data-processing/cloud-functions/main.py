def detect_x_abnormal(event, context):
    import base64
    import json

    if 'data' in event:
        d_data = base64.b64decode(event['data']).decode('utf-8')
        j_data = json.loads(d_data)
        x_value = float(j_data['raw_accelerometer_data'].split(',')[0].strip().split('=')[1])
        if x_value == -1.0:
            print("""X value is -1, triggered by messageID {} published at {}""".format(context.event_id, context.timestamp))