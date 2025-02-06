import requests

def get_request(url):
    r = requests.get(url)
    r.raise_for_status()
    return r.json()
