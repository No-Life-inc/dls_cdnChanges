import os
import base64
import pika
import json
from dotenv import load_dotenv

load_dotenv()

RABBITMQ_URL = f"amqp://{os.getenv('RABBITUSER')}:{os.getenv('RABBITPW')}@{os.getenv('RABBITURL')}/%2F"
RABBITMQ_NEW_IMAGE_QUEUE = 'new_images'
CDN_URL = os.getenv('CDNURL')

def on_message(channel, method_frame, header_frame, body):
    try:
        message = json.loads(body)
    except json.JSONDecodeError:
        print(f'Failed to parse message body as JSON: {body}')
        return

    if not isinstance(message, dict) or 'image' not in message:
        print(f'Message is not a dictionary or does not contain an "image" key: {message}')
        return
    
    data_url = message['image']
    base64_image_data = data_url.split(',')[1]
    image_data = base64.b64decode(base64_image_data)
    filename = message['filename'].split('.')[0]  # Remove the file extension from the filename
    fileType = message['fileType'].split('/')[1]  # Extract the correct file type

    print(f'Uploading image {filename}.{fileType} to CDN')

    # Save the image to a file in the shared volume
    with open(f'/usr/src/app/images/images/{filename}.{fileType}', 'wb') as f:
        f.write(image_data)

    channel.basic_ack(delivery_tag=method_frame.delivery_tag)

connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
channel = connection.channel()

# Declare the queue to ensure it exists
channel.queue_declare(queue=RABBITMQ_NEW_IMAGE_QUEUE, durable=True)

channel.basic_consume(RABBITMQ_NEW_IMAGE_QUEUE, on_message)
print(f'Starting to consume messages from the "{RABBITMQ_NEW_IMAGE_QUEUE}" queue')
channel.start_consuming()