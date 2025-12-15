from multiprocessing import Process, Queue, Value
import time
from stream import CTlogsStream
import argparse
import sys

def producer_process(ct_log_url, buffer, total_entries):
    producer = CTlogsStream(ct_log_url=ct_log_url, buffer=buffer, total_entries=total_entries)
    producer.start_stream()


def main():
    parser = argparse.ArgumentParser(description="CTLog Kafka producer")
    parser.add_argument("ctlog_url", type=str, help="CTLog source URL")
    parser.add_argument("first_index", type=int, help="First entry index")
    parser.add_argument("last_index", type=int, help="Last entry index")
    parser.add_argument("broker", type=str, help="Kafka broker (host:port)")
    parser.add_argument("topic", type=str, help="Kafka topic to produce to")
    args = parser.parse_args()

    # --- Assign to variables ---
    ctlog_url = args.ctlog_url
    first_index = args.first_index
    last_index = args.last_index
    broker = args.broker
    topic = args.topic

if __name__ == "__main__":
    main()