import pika
import os
from producer_interface import mqProducerInterface 
class mqProducer(mqProducerInterface):
    def __init__(self, routing_key: str, exchange_name: str):
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.setupRMQConnection()

    def setupRMQConnection(self):
        # Set-up Connection to RabbitMQ Service
        con_params = pika.ConnectionParameters(os.environ['AMQP_URL'])
        self.connection = pika.BlockingConnection(parameters=con_params)

        # Establish Channel
        self.channel = connection.channel()

        # Create the exchange if not already present
        self.exchange = channel.exchange_declare(exchange=self.exchange_name)

        pass

    def publishOrder(self, message: str):
        # Publish to Exchange
        self.channel.basic_publish(
            exchange = self.exchange_name,
            routing_key = self.routing_key,
            body = message
        )
        # Close Channel
        self.channel.close()
        self.connection.close()

        pass
