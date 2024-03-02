from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend


class StreamProcessor():
    def __init__(
        self,
        job_name: str,
    ):
        self.job_name = job_name
        self.config = Configuration()
        self.config.set_string("python.executable", "/opt/flink/venv/bin/python3") # config 
        self.env = StreamExecutionEnvironment.get_execution_environment(self.config)
        self.env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)
        self.env.set_state_backend(EmbeddedRocksDBStateBackend())
        
    def add_source(self):
        pass
    
    def process(self):
        pass
    
    def add_sink(self):
        pass
    
    def execute(self):
        self.add_source()
        self.process()
        self.add_sink()
        self.env.execute(self.job_name)