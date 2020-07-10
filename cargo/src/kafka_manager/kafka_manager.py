import io
import struct

from avro.io import BinaryDecoder, DatumReader
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer import SerializerError
from loguru import logger


class KafkaAvroProcessor(object):
    def __init__(self, kafka_conf):
        self.topics = None
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
        logger.info("Initializing avro consumer")
        self.consumer = Consumer(self.consumer_conf)
        logger.info(f"Schema registry url: {schema_registry_url}")
        self.register_client = CachedSchemaRegistryClient(url=schema_registry_url)
        logger.info(f"Subscribing to topics: {topics}")
        self.topics = topics
        self.consumer.subscribe(self.topics)

    @staticmethod
    def delivery_report(err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            logger.error('Message delivery failed: {}'.format(err))
        else:
            logger. \
                info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    def produce(self, messages):
        pass

    def consume(self, db_manager=None):
        logger.info("Consuming")
        while True:
            try:
                msg = self.consumer.poll(timeout=1)
            except SerializerError as e:
                logger.error("Message deserialization failed for {}: {}".format(msg, e))
                raise SerializerError

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info('%% %s [%d] reached end at offset %d' %
                                (msg.topic(), msg.partition(), msg.offset()))
                    continue
                logger.error("AvroConsumer error: {}".format(msg.error()))
                return

            key, value = self._unpack(msg.key()), self._unpack(msg.value())
            logger.info(f"Message: {key}, {value}")
            if db_manager:
                sql = f"""INSERT INTO public.fdw_kafka(key, BEFORE, AFTER, FLIGHT_URL, LEG_URL, AFTER_RAW_DATA)
                            values ({key}, {value['BEFORE']}, {value['AFTER']}, value['FLIGHT_URL'], value['LEG_URL'], value['AFTER_RAW_DATA']);"""
            db_manager.execute(sql)

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
