import json
from loguru import logger
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException

import io
import struct
from avro.io import BinaryDecoder, DatumReader
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer import SerializerError


class KafkaProcessor(object):
    def __init__(self, kafka_conf):
        self.topic = kafka_conf['group.id']
        self.consumer_conf = {
            'auto.offset.reset': 'earliest',
            'enable.partition.eof': True,
        }
        self.consumer_conf.update(kafka_conf)
        self.producer_conf = kafka_conf
        self.producer = None
        self.consumer = None

    def init_producer(self):
        self.producer = Producer(self.producer_conf)

    def init_consumer(self):
        self.consumer = Consumer(self.consumer_conf)
        self.consumer.subscribe(self.topic.split(','))

    @staticmethod
    def delivery_report(err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            logger.error('Message delivery failed: {}'.format(err))
        else:
            logger.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    def produce(self, messages):
        if not self.producer:
            self.init_producer()
        for message in messages:
            self.producer.produce(self.topic, json.dumps(message).encode('utf-8'),
                                  callback=self.delivery_report)
            self.producer.poll(0.5)

    def consume(self):
        if not self.consumer:
            self.init_consumer()
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        logger.info('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                        break
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    logger.info(msg.value().decode('utf-8'))
        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()


class KafkaAvroProcessor(object):
    def __init__(self, kafka_conf):
        self.topic = kafka_conf['group.id']
        self.register_client = None
        self.consumer_conf = {
            'auto.offset.reset': 'earliest',
            'enable.partition.eof': True,
        }
        self.consumer_conf.update(kafka_conf)
        # self.producer_conf = kafka_conf
        # self.producer = None
        self.consumer = None
        self.MAGIC_BYTES = 0

    def init_producer(self):
        pass

    def init_consumer(self, schema_registry_url, topics):
        self.consumer = Consumer(self.consumer_conf)
        self.register_client = CachedSchemaRegistryClient(url=schema_registry_url)
        self.consumer.subscribe(topics)

    @staticmethod
    def delivery_report(err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            logger.error('Message delivery failed: {}'.format(err))
        else:
            logger.\
                info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    def produce(self, messages):
        pass

    def consume(self):
        while True:
            try:
                msg = self.consumer.poll(1)
            except SerializerError as e:
                print("Message deserialization failed for {}: {}".format(msg, e))
                raise SerializerError

            if msg is None:
                continue

            if msg.error():
                print("AvroConsumer error: {}".format(msg.error()))
                return

            key, value = self._unpack(msg.key()), self._unpack(msg.value())
            print(key, value)

    def _unpack(self, payload):
        magic, schema_id = struct.unpack('>bi', payload[:5])

        # Get Schema registry
        # Avro value format
        if magic == self.MAGIC_BYTES:
            schema = self.register_client.get_by_id(schema_id)
            reader = DatumReader(schema)
            output = BinaryDecoder(io.BytesIO(payload[5:]))
            abc = reader.read(output)
            return abc
        # String key
        else:
            # If KSQL payload, exclude timestamp which is inside the key.
            # payload[:-8].decode()
            return payload.decode()