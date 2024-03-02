from pathlib import Path

from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Types
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer, FlinkKafkaConsumer

from src.plugins.streamprocessor import StreamProcessor
#from src.plugins.datastream.connector.redis_connector import redis_sink
from src.job.test_alram_gx.gx_alram import validate

import json

class gx_job(StreamProcessor):
    def __init__(
        self,
        job_name: str,
        **kwargs
    ):
        super().__init__(
            job_name=job_name,
            **kwargs
        )

    # user define source
    def add_source(self):
        kafka_consumer = FlinkKafkaConsumer(
            topics='source',
            deserialization_schema=SimpleStringSchema(),
            properties={'bootstrap.servers': 'kafka:9092',
                        'group.id': 'test_group_1'}
        )
        kafka_consumer.set_start_from_latest()
        self.stream = self.env.add_source(kafka_consumer).set_parallelism(2)

    # user define process
    def process(self):
        self.stream.key_by(
            lambda x: eval(json.loads(x))['Symbol'],
            key_type=Types.STRING(),
        ).map(validate, output_type= Types.STRING()).set_parallelism(2)

    # user define sink
    def add_sink(self):
        kafka_producer = FlinkKafkaProducer(
            topic="target",
            serialization_schema=SimpleStringSchema(),
            producer_config={
                "bootstrap.servers": "kafka:9092",
                "group.id": "self._consumer_group_id",
            },
        )
        self.stream.add_sink(kafka_producer) \
            .name("kafka_sink") \
            .set_parallelism(2)


if __name__ == "__main__":
    stream = gx_job(job_name=Path(__file__).stem)
    stream.execute()
