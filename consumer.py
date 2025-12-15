from confluent_kafka import Consumer, TopicPartition
import time

bootstrap_servers = 'localhost:9092'
topic_name = 'my_topic'  # can be a list of topics later
poll_interval = 5  # seconds between checks

consumer = Consumer({
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'monitor_group',
    'enable.auto.commit': False
})

while True:
    try:
        partitions = consumer.list_topics(topic_name).topics[topic_name].partitions
        total_messages = 0

        for p_id in partitions:
            tp = TopicPartition(topic_name, p_id)
            low, high = consumer.get_watermark_offsets(tp)
            total_messages += (high - low)

        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Total messages in '{topic_name}': {total_messages}")
        time.sleep(poll_interval)

    except KeyboardInterrupt:
        print("Monitoring stopped.")
        break
    except Exception as e:
        print(f"Error: {e}")
        time.sleep(poll_interval)

consumer.close()
