import argparse
import json
import sys
import requests
import threading
import time
import logging

from src.consumer import ConsumerKafka
from src.resampling import Resample


def start_consumer(args):
    consumer = ConsumerKafka(configuration_location=args.config)
    
    print("=== Service starting ===", flush=True)
    consumer.read()

def main():
    logging.basicConfig(filename="event_log.log", format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    parser = argparse.ArgumentParser(description="consumer")

    parser.add_argument(
        "-c",
        "--config",
        dest="config",
        default="config1.json",
        help=u"Config file located in ./config/ directory."
    )

    # Display help if no arguments are defined
    if (len(sys.argv) == 1):
        parser.print_help()
        sys.exit(1)

    # Parse input arguments
    args = parser.parse_args()

 
    start_consumer(args)

    


if (__name__ == '__main__'):
    main()