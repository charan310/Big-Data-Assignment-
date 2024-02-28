from confluent_kafka import Producer
import csv

p = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

# Open the CSV file for reading
csv_file_path = "/home/sai/EVENT.csv"

with open(csv_file_path, 'r') as file:
    # Create a CSV reader object
    csv_reader = csv.DictReader(file)
    
    #for row in csv_reader:
        # Convert row values to appropriate types as needed
       # row["EventValue"] = int(row["EventValue"])
        #row["ComponentID"] = int(row["ComponentID"])
        #row["UserID"] = int(row["UserID"])

        #print(row)

        # Trigger any available delivery report callbacks from previous produce() calls
        #p.poll(0)

        # Asynchronously produce a message
p.produce('new_topic', str().encode('utf-8'), callback=delivery_report)

# Wait for any outstanding messages to be delivered and delivery report callbacks to be triggered
p.flush()
