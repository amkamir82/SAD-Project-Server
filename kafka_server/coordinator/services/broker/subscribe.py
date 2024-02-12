import requests

import json


def send_subscribe_to_broker(broker_url, client_url, subscription_id):
    r = requests.post(broker_url, data=json.dumps({"client_url": client_url, "subscription_id": subscription_id}))
    return r.status_code
