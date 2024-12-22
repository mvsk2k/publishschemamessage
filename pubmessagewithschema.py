from google.cloud import pubsub_v1

# Path to your service account key file
service_account_path = "key.json"

# Define your project ID and topic ID
project_id = "myownproject241124"
topic_id = "mytopicmessagewithschema"

# Create a publisher client using the service account key
publisher = pubsub_v1.PublisherClient.from_service_account_file(service_account_path)

# Build the topic name
topic_path = publisher.topic_path(project_id, topic_id)

# Define the message data (JSON format, must conform to the schema)
message_data = {
     "Name": "M V SIVAKUMAR",
     "Age": 50,
     "Email": "skmar@gmail.com"
     #"Email": 25
   }

# Convert the message to a JSON string
import json
message_json = json.dumps(message_data)

# Publish the message

"""
# This also worked, Publish the message
future = publisher.publish(
    topic_path,
    data=message_json.encode("utf-8"),
    # Add optional attributes (if needed)
    origin="python-script",
    user="test-user"
)

print(f"Message published with ID: {future.result()}")

"""

# This also worked, Publish the message
try:
    future = publisher.publish(topic_path, data=message_json.encode("utf-8"))
    print(f"Message published with ID: {future.result()}")
except Exception as e:
    print(f"Failed to publish message: {e}")
