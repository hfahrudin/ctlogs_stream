from multiprocessing import Process, Queue, Value
import time
from stream import CTlogsStream
from decrypt import CTLogsProcs
import argparse


def producer_process(ct_log_url, buffer, total_entries, start_index=0, end_index=None):
    producer = CTlogsStream(ct_log_url=ct_log_url, buffer=buffer, total_entries=total_entries, start_index=start_index, end_index=end_index)
    producer.start_stream()


def consumer_process(buffer, total_procs):
    consumer = CTLogsProcs(buffer, total_procs)
    consumer.start_processing()


def run():
    parser = argparse.ArgumentParser(description="CTLog Kafka producer")
    
    parser.add_argument("--ctlog_url", type=str,
                        default="https://ct.cloudflare.com/logs/nimbus2025/",
                        help="CTLog source URL")
    parser.add_argument("--start_index", type=int, default=0, help="First entry index")
    parser.add_argument("--end_index", type=int, default=1000, help="Last entry index")
    parser.add_argument("--broker", type=str, default="localhost:9092", help="Kafka broker (host:port)")
    parser.add_argument("--topic", type=str, default="ctlogs", help="Kafka topic to produce to")
    parser.add_argument("--num_consumers", type=int, default=3, help="Number of consumer processes")
    
    args = parser.parse_args()

    buffer_queue = Queue()
    total_procs = Value('i', 0)
    total_entries = Value('i', 0)

    # Start stream
    producer = Process(
        target=producer_process,
        args=(args.ctlog_url, buffer_queue, total_entries, args.start_index, args.end_index),
        daemon=True
    )
    producer.start()

    # Start Decode
    consumers = []
    for _ in range(args.num_consumers):
        p = Process(target=consumer_process, args=(buffer_queue, total_procs), daemon=True)
        p.start()
        consumers.append(p)

    # Monitoring loop
    start_time = time.time()
    try:
        while True:
            time.sleep(10)
            elapsed = time.time() - start_time
            print(f"Elapsed time: {elapsed:.2f}s")
            print(f"Total Entries: {total_entries.value}")
            print(f"Buffer size: {buffer_queue.qsize()}")
            print(f"Total processed: {total_procs.value}")
            print("-----")
    except KeyboardInterrupt:
        print("Shutting down...")
        producer.terminate()
        producer.join()
        for c in consumers:
            c.terminate()
            c.join()


# In Jupyter, call run() explicitly
if __name__ == "__main__":
    run()