import requests

import json


def send_subscribe_to_broker(broker_url, client_url, subscription_id):
    r = requests.post(f"{broker_url}/subscription-plan",
                      data=json.dumps({"client_url": client_url, "subscription_id": subscription_id}))
    return r.status_code
