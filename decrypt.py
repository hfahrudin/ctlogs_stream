import certlib
import base64
from OpenSSL import crypto
from cryptography import x509
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
import threading
import time
import uuid
import clickhouse_connect

init_client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='default',
    password='mysecretpassword',
    database='default'
)

# --- Step 1: Create database if it doesn't exist ---
db_name = 'certs_db'
init_client.command(f'CREATE DATABASE IF NOT EXISTS {db_name}')

# Step 2: Use the database
init_client.command(f'USE {db_name}')

# Step 3: Drop the table if it exists
init_client.command('DROP TABLE IF EXISTS certs')

# Step 4: Create a fresh table
init_client.command('''
CREATE TABLE certs (
    cert_index UInt32,
    sha1_fingerprint String,
    issuer_country String,
    issuer_organization String,
    issuer_common_name String,
    subject_country String,
    subject_state String,
    subject_organization String,
    subject_serial_number String,
    subject_common_name String,
    san String,
    subject_business_category String,
    subject_jurisdiction_state String,
    subject_jurisdiction_country String,
    not_after DateTime
) ENGINE = MergeTree()
ORDER BY cert_index
''')
init_client.close()  # closes connections

print("Fresh certs table created successfully!")

class CTLogsProcs:
    def __init__(self, entries_queue: queue, total_procs):
        self.entries_queue = entries_queue
        # self.max_workers = max_workers
        self.stop_event = threading.Event()  # <--- stop flag
        self.pull_time = 2  # seconds
        self.start_time = time.time()
        self.total_procs = total_procs
        self.db_client = clickhouse_connect.get_client(
            host='localhost',
            port=8123,
            username='default',
            password='mysecretpassword',
            database='default'
        )




    def start_processing(self):
        while not self.stop_event.is_set():
            batched_entries = self.entries_queue.get()  # blocking get
            rows_to_insert = []
            for entry in batched_entries:
                cert_data = process_ctlog_entry(entry)
                rows_to_insert.append((
                    cert_data['cert_index'],
                    cert_data['sha1_fingerprint'],
                    cert_data['issuer_country'],
                    cert_data['issuer_organization'],
                    cert_data['issuer_common_name'],
                    cert_data['subject_country'],
                    cert_data['subject_state'],
                    cert_data['subject_organization'],
                    cert_data['subject_serial_number'],
                    cert_data['subject_common_name'],
                    cert_data['san'],
                    cert_data['subject_business_category'],
                    cert_data['subject_jurisdiction_state'],
                    cert_data['subject_jurisdiction_country'],
                    cert_data['not_after']
                ))

            # Insert all rows at once
            self.db_client.insert(
                'certs',
                rows_to_insert,
                column_names=[
                    'cert_index', 'sha1_fingerprint', 'issuer_country', 'issuer_organization',
                    'issuer_common_name', 'subject_country', 'subject_state', 'subject_organization',
                    'subject_serial_number', 'subject_common_name', 'san',
                    'subject_business_category', 'subject_jurisdiction_state',
                    'subject_jurisdiction_country', 'not_after'
                ]
            )
            with self.total_procs.get_lock():  # ensures atomic update
                self.total_procs.value += len(batched_entries)



    def start_processing_debug(self):
        while not self.stop_event.is_set():
            batched_entries = self.entries_queue.get()  # blocking get
            batch_start = time.time()
            avg = 0
            for entry in batched_entries:
                s = time.time()
                result = process_ctlog_entry(entry)
                e = time.time() - s
                avg+=e
            
            with self.total_procs.get_lock():  # ensures atomic update
                self.total_procs.value += len(batched_entries)
            elapsed = time.time() - batch_start
            avg_time = avg / len(batched_entries) if batched_entries else 0
            print(f"Elapsed: {elapsed:.2f}s - Processing Time avg {avg_time:.4f}s")

    # def worker(self):

    #     while not self.stop_event.is_set():
    #         entry = self.entries_queue.get()  # blocking get
    #         thread_name = threading.current_thread().name 
    #         try:
    #             s = time.time()
    #             result = process_ctlog_entry(entry)
    #             e = time.time() - s
    #             if result:
    #                 self.total_procs += 1
    #         except Exception as exc:
    #             print(f"Entry {entry} generated an exception: {exc}")
    #         finally:
    #             self.entries_queue.task_done()

    #         # Optional: log progress every 2 seconds
    #         elapsed = time.time() - self.start_time
    #         t = time.time()
    #         if int(elapsed) % 2 == 0:
    #             print(f"Elapsed: {elapsed:.2f}s - Processing Time {e} - ID: {short_id} - Total processed: {self.total_procs}")


    # def start_processing(self):
    #     time.sleep(1) # initial wait before starting processing
    #     while not self.stop_event.is_set():

    #         entries = []
    #         BATCH_SIZE = 20
    #         for _ in range(BATCH_SIZE):
    #             try:
    #                 entries.append(self.entries_queue.get_nowait())
    #             except queue.Empty:
    #                 break

    #         # print(f"Processing {len(entries)} entries from queue...")
    #         # print(f"Queue size now: {self.entries_queue.qsize()}")
            
    #         if entries:
    #             results = self.process_entries(entries)

    #             elapsed = time.time() - self.start_time
    #             self.total_procs += len(results)

    #             if int(elapsed) % 2 == 0:  # Print every 5 seconds
    #                 print(f"Elapsed time: {elapsed:.2f} seconds - Total processed entries: {self.total_procs}")
    #         else:
    #             break  # No more entries to process, exit the loop
    #     print("Processing finished.")
    
    # def process_entries(self, entries):
    #     results = []
    #     futures = {self.executor.submit(process_ctlog_entry, entry): entry for entry in entries}
        
    #     for future in as_completed(futures):
    #         entry = futures[future]
    #         try:
    #             data = future.result()
    #             if data:
    #                 results.append(data)
    #         except Exception as exc:
    #             print(f"Entry {entry} generated an exception: {exc}")
        
    #     return results

def safe_b64decode(data: str) -> bytes:
    # Add padding if missing
    missing_padding = len(data) % 4
    if missing_padding:
        data += '=' * (4 - missing_padding)
    return base64.b64decode(data)

def process_ctlog_entry(entry):
    """Process a single CT log entry to extract and decrypt certificate data."""
    cert_data = decrypt_ctlog(entry)
    result = process_certificate(cert_data)
    return result


def decrypt_ctlog(raw_ctlog):
    leaf_input = safe_b64decode(raw_ctlog["leaf_input"])
    extra_data = safe_b64decode(raw_ctlog["extra_data"])
    mtl = certlib.MerkleTreeHeader.parse(leaf_input)

    chain = []

    if mtl.LogEntryType == "X509LogEntryType":
        leaf_cert = crypto.load_certificate(
            crypto.FILETYPE_ASN1,
            certlib.Certificate.parse(mtl.Entry).CertData
        )
        extra_data_parsed = certlib.CertificateChain.parse(extra_data)
        chain = [crypto.load_certificate(crypto.FILETYPE_ASN1, cert.CertData)
                 for cert in extra_data_parsed.Chain]
        entry_type = "X509LogEntry"

    else:  # PreCert
        extra_data_parsed = certlib.PreCertEntry.parse(extra_data)
        leaf_cert = crypto.load_certificate(
            crypto.FILETYPE_ASN1,
            extra_data_parsed.LeafCert.CertData
        )
        chain = [crypto.load_certificate(crypto.FILETYPE_ASN1, cert.CertData)
                 for cert in extra_data_parsed.Chain]
        entry_type = "PreCertEntry"

    return {
        "type": entry_type,
        "leaf_cert": certlib.dump_cert(leaf_cert),
        "chain": [certlib.dump_cert(c) for c in chain]
    }

def process_certificate(cert_data):
    """Process certificate data to extract relevant fields for database."""
    try:
        der_cert = base64.b64decode(cert_data['leaf_cert']['as_der'].strip())
        certificate = x509.load_der_x509_certificate(der_cert, default_backend())

        # SHA1 fingerprint
        sha1_fingerprint = certificate.fingerprint(hashes.SHA1())
        sha1_fingerprint_hex = ':'.join(f'{byte:02X}' for byte in sha1_fingerprint)

        # Issuer attributes
        issuer_attributes = {attr.oid: attr.value for attr in certificate.issuer}
        issuer_country = issuer_attributes.get(x509.NameOID.COUNTRY_NAME, "")
        issuer_organization = issuer_attributes.get(x509.NameOID.ORGANIZATION_NAME, "")
        issuer_common_name = issuer_attributes.get(x509.NameOID.COMMON_NAME, "")

        # Subject attributes
        subject_attributes = {attr.oid: attr.value for attr in certificate.subject}
        subject_country = subject_attributes.get(x509.NameOID.COUNTRY_NAME, "")
        subject_state = subject_attributes.get(x509.NameOID.STATE_OR_PROVINCE_NAME, "")
        subject_organization = subject_attributes.get(x509.NameOID.ORGANIZATION_NAME, "")
        subject_serial_number = subject_attributes.get(x509.NameOID.SERIAL_NUMBER, "")
        subject_common_name = subject_attributes.get(x509.NameOID.COMMON_NAME, "")
        subject_business_category = subject_attributes.get(x509.NameOID.BUSINESS_CATEGORY, "")
        subject_jurisdiction_state = subject_attributes.get(x509.NameOID.JURISDICTION_STATE_OR_PROVINCE_NAME, "")
        subject_jurisdiction_country = subject_attributes.get(x509.NameOID.JURISDICTION_COUNTRY_NAME, "")

        # Subject Alternative Names
        try:
            san_extension = certificate.extensions.get_extension_for_oid(x509.ExtensionOID.SUBJECT_ALTERNATIVE_NAME)
            san_values = san_extension.value.get_values_for_type(x509.DNSName)
            san_values = [san for san in san_values if san != subject_common_name]
            san_string = ";".join(san_values)
        except x509.ExtensionNotFound:
            san_string = ""

        # Validity period (using UTC-aware property)
        not_after = certificate.not_valid_after_utc

        #TODO: NOT BEFORE not_after = certificate.not_valid_before_utc (?)

        return  {
            "cert_index": cert_data.get('cert_index', 0),
            "sha1_fingerprint": sha1_fingerprint_hex,
            "issuer_country": issuer_country,
            "issuer_organization": issuer_organization,
            "issuer_common_name": issuer_common_name,
            "subject_country": subject_country,
            "subject_state": subject_state,
            "subject_organization": subject_organization,
            "subject_serial_number": subject_serial_number,
            "subject_common_name": subject_common_name,
            "san": san_string,
            "subject_business_category": subject_business_category,
            "subject_jurisdiction_state": subject_jurisdiction_state,
            "subject_jurisdiction_country": subject_jurisdiction_country,
            "not_after": not_after
        }

    except Exception as e:
        return None