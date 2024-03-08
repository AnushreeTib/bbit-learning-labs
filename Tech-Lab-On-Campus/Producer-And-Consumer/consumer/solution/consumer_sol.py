import os
import pika
import json

from consumer_interface import mqConsumerInterface

class mqConsumer(mqConsumerInterface):
    def __init__(self, exchange_name, queue_name, binding_key):
        # body of constructor
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.binding_key = binding_key
        
        # Call the setupRMQConnection function
        self.setupRMQConnection()
    
    def setupRMQConnection(self) -> None:
        #  Establish connection to the RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        
        # declare a queue and exchange
        self.channel = self.connection.channel()
        
        self.channel.queue_declare(self.queue_name)
        self.channel.exchange_declare(self.exchange_name)
        
        self.channel.queue_bind(
            queue=self.queue_name,
            routing_key=self.binding_key, 
            exchange=self.exchange_name,
        )
        
        self.channel.basic_consume(
            self.queue_name, self.on_message_callback, auto_ack=False
        )
            
    def on_message_callback(
        self, channel, method_frame, header_frame, body
    ) -> None:
        # Acknowledge message
        channel.basic_ack(method_frame.delivery_tag, False)

        #Print message (The message is contained in the body parameter variable)
        message = json.loads(body)
        print(message)


    def startConsuming(self) -> None:
        # Print " [*] Waiting for messages. To exit press CTRL+C"

        # Start consuming messages
        self.channel.start_consuming()
    
    def __del__(self) -> None:
        # Print "Closing RMQ connection on destruction"
        
        # Close Channel
        # Close Connection
        self.channel.close()
        self.connection.close()
        
