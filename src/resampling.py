from typing import Any, Dict, List
import numpy as np 
import pandas as pd 

from output import OutputAbstract, KafkaOutput


import numpy.ma as ma
import time

class Resample():

    def __init__(self, conf: Dict[Any, Any] = None) -> None:
        super().__init__()
        if(conf is not None):
            self.configure(conf)

    def configure(self, conf: Dict[Any, Any] = None,
                  configuration_location: str = None,
                  algorithm_indx: int = None) -> None:

        self.interval =  conf["publishing_interval"]
        self.start_timestamp = conf["start_timestamp"]

        self.running_timestamp = self.start_timestamp
        
        self.outputs = [eval(o) for o in conf["output"]]
        output_configurations = conf["output_conf"]
        for o in range(len(self.outputs)):
            self.outputs[o].configure(output_configurations[o])


    def message_insert(self, message: Dict[Any, Any]) -> Any:
        print('message: ' + str(message))
        timestamp = message['time']
        self.buffer_message = message

        if(timestamp > self.running_timestamp):
            diff = timestamp - self.running_timestamp
            ticks = int(diff/self.interval)
            self.running_timestamp += self.interval*ticks
            self.buffer_message['time'] = self.running_timestamp

            self.running_timestamp += self.interval*1

            print(self.buffer_message)

            for output in self.outputs:
                output.send_out(timestamp=timestamp,
                            value=self.buffer_message,
                            suggested_value = None, 
                            algorithm= 'Resample')

    