from cargo.src.kafka_manager.kafka_manager import KafkaAvroProcessor
from cargo.utils.config_utils import read_config
from cargo.src.postgres_manager.postgres_manager import PostgresProcessor
import os

CONFIG_PATH = os.environ['CARGO_CONFIG_PATH']
config_sections = read_config(CONFIG_PATH)
kafka_conf = config_sections['FDW_KAFKA']
postgres_conf = config_sections['POSTGRES']

schema_registry_url = config_sections['FDW_SCHEMA_REGISTRY']['schema_registry_url']
topics = config_sections['FDW_SCHEMA_REGISTRY']['topics'].split(',')

postgres = PostgresProcessor().with_config(postgres_conf).establish_connection()
c = KafkaAvroProcessor(kafka_conf)
c.init_consumer(schema_registry_url=schema_registry_url, topics=topics)
c.consume(db_manager=postgres)
