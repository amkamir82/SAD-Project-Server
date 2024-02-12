import requests

import json


def get_all_subscriptions():
    r = requests.get("http://127.0.0.1:5001/subscribe/list_all")
    return r.status_code, r.json()


def send_subscribe_to_broker(broker_url, data):
    r = requests.post(f"{broker_url}/subscription-plan",
                      data=json.dumps({"subscription_plans": data}))
    return r.status_code
