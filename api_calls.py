import requests
def node_cpu_seconds_total():


    url = "http://localhost:9182/metrics"

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    print(response.text)
    return response