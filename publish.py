"""Writes dummy messages in json to PubSub, for testing purposes"""

import time
import json
import datetime
from google.cloud import pubsub


topic = 'projects/zdenky-15ba5/topics/crawled-features'

publisher = pubsub.PublisherClient()

if __name__ == '__main__':
    c = 0
    limit = 100  # number of messages
    while c < limit:
        now = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
        msg = {
            'key': str(int(time.time())),
            'page_keywords': 'hello, thanks, beam',
            'page_title': 'some title ' + str(datetime.datetime.now())
        }
        r = publisher.publish(topic, json.dumps(msg).encode())
        x = r.result()
        print(c)
        c += 1
        time.sleep(1)
