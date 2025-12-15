import threading
import uuid
import base64
import time
import requests
import queue

class CTlogsStream:
    def __init__(self, ct_log_url: str, buffer: queue.Queue, total_entries, start_index=0, end_index=None, max_retries=10, retry_delay=1):
        self.ct_log_url = ct_log_url
        self.start_index = start_index
        self.end_index = end_index or self.get_sth(ct_log_url)['tree_size']
        self.current_index = self.start_index
        self.workers = {}
        self.lock = threading.Lock()  # still needed for current_index & workers
        self.start_time = time.time()
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.buffer = buffer  # queue.Queue
        self.stop_event = threading.Event()  # <--- stop flag
        self.start_time = time.time()
        self.total_entries = total_entries


    def get_sth(self, log_url, timeout=10):
        url = log_url + "ct/v1/get-sth"
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            print(f"Error fetching STH: {e}")
            return {'tree_size': 0}

    def get_entries(self, start_index, end_index, timeout=60):
        url = f"{self.ct_log_url}ct/v1/get-entries?start={start_index}&end={end_index}"
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            print(f"Error fetching entries {start_index}-{end_index}: {e}")
            return None

    def stream_task(self, batch_size=100):
        u = uuid.uuid4()
        short_id = base64.urlsafe_b64encode(u.bytes)[:8].decode('utf-8')

        with self.lock:
            if self.current_index >= self.end_index:
                return
            start_index = self.current_index
            end_index = min(self.current_index + batch_size - 1, self.end_index - 1)
            self.current_index = end_index + 1
            self.workers[short_id] = {"index": {"start": start_index, "end": end_index}, "job_status": "process"}

        attempt = 0
        while True:
            try:
                entries = self.get_entries(start_index, end_index)
                # elapsed = time.time() - self.start_time
                # print(f"[{elapsed:.2f}s] Worker {u}: fetched {len(entries['entries'])} entries "
                #     f"({start_index} → {end_index})")
                if entries is None or 'entries' not in entries:
                    raise Exception("Failed to get entries")

                # put each entry individually into the queue
                batched_entries = entries['entries']
                self.buffer.put(batched_entries)
                with self.total_entries.get_lock():
                    self.total_entries.value += len(batched_entries)

                with self.lock:
                    self.workers.pop(short_id, None)
                break

            except Exception as e:
                attempt += 1
                if self.max_retries and attempt >= self.max_retries:
                    print(f"Worker {short_id}: giving up after {attempt} retries. Last error: {e}")
                    with self.lock:
                        self.workers.pop(short_id, None)
                    break
                time.sleep(self.retry_delay)

    def start_stream(self, stagger_coef=0.01, batch_size=512):
        # last_yield_time = time.time()
        while not self.stop_event.is_set():
            # stop if all work done
            with self.lock:
                if self.current_index >= self.end_index and not self.workers:
                    break

            t = threading.Thread(target=self.stream_task, args=(batch_size,))
            t.daemon = True
            t.start()

            with self.lock:
                num_workers = len(self.workers)
            stagger = 0.12 + num_workers * stagger_coef

            # self.total_entries_shared.value = self.total_entries
            time.sleep(stagger)
        print("Stream finished.")

            # # periodic yield: can yield nothing or just indicate buffer size
            # if time.time() - last_yield_time >= push_time:
            #     yield self.buffer.qsize()  # or just yield a signal; queue contents are thread-safe
            #     last_yield_time = time.time()