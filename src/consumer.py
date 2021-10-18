from abc import ABC, abstractmethod
import csv
import json
import sys

from typing import Any, Dict, List
sys.path.insert(0,'./src')
# Algorithm imports
from kafka import KafkaConsumer, TopicPartition
from json import loads
import matplotlib.pyplot as plt
from time import sleep
import numpy as np
import pandas as pd
import datetime
from resampling import Resample


class ConsumerAbstract(ABC):
    configuration_location: str

    def __init__(self, configuration_location: str = None) -> None:
        self.configuration_location = configuration_location

    @abstractmethod
    def configure(self, con: Dict[Any, Any],
                  configuration_location: str) -> None:
        self.configuration_location = configuration_location

    @abstractmethod
    def read(self) -> None:
        pass

    # rewrites anomaly detection configuration
    def rewrite_configuration(self, anomaly_detection_conf: Dict[str, Any]
                              ) -> None:
        with open(self.configuration_location) as c:
            conf = json.load(c)
            conf["anomaly_detection_conf"] = anomaly_detection_conf

        with open(self.configuration_location, "w") as c:
            json.dump(conf, c)




class ConsumerKafka(ConsumerAbstract):
    consumer: KafkaConsumer

    def __init__(self, conf: Dict[Any, Any] = None,
                 configuration_location: str = None) -> None:
        super().__init__(configuration_location=configuration_location)
        if(conf is not None):
            self.configure(con=conf)
        elif(configuration_location is not None):
            # Read config file
            with open("configuration/" + configuration_location) as data_file:
                conf = json.load(data_file)
            self.configure(con=conf)
        else:
            print("No configuration was given")

    def configure(self, con: Dict[Any, Any] = None) -> None:
        if(con is None):
            print("No configuration was given")
            return 

        self.topics = con['topics']
        self.consumer = KafkaConsumer(
                        bootstrap_servers=con['bootstrap_servers'],
                        auto_offset_reset=con['auto_offset_reset'],
                        enable_auto_commit=con['enable_auto_commit'],
                        group_id=con['group_id'],
                        value_deserializer=eval(con['value_deserializer']))
        self.consumer.subscribe(self.topics)

        # Initialize a list of anomaly detection algorithms, each for a
        # seperate topic
        self.resampling_names = con["resampling_alg"]
        self.resampling_configurations = con["resampling_conf"]
        # check if the lengths of configurations, algorithms and topics are
        # the same
        assert (len(self.resampling_names) == len(self.topics) and
                len(self.topics) == len(self.resampling_configurations)),\
                "Number of algorithms, configurations and topics does not match"
        self.resample = []
        algorithm_indx = 0
        for resampling_name in self.resampling_names:
            Resampling = eval(resampling_name)
            Resampling.configure(self.resampling_configurations[algorithm_indx],
                              configuration_location=self.configuration_location,
                              algorithm_indx=algorithm_indx)
            self.resample.append(Resampling)
            algorithm_indx += 1
            
    def read(self) -> None:
        for message in self.consumer:
            # Get topic and insert into correct algorithm
            #print(message)
            topic = message.topic
            print('topic: ' + str(topic), flush=True)
            algorithm_indx = self.topics.index(topic)
            

            if message is not None:
                value = message.value
                self.resample[algorithm_indx].message_insert(value)




