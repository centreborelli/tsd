import os
import json
import requests

url = "https://search.federated.geoapi-airbusds.com/api/v1/search"

headers = {
    'Authorization': 'Apikey {}'.format(os.environ['AIRBUS_DS_API']),
    'Cache-Control': 'no-cache',
    'Content-Type': 'application/json',
}

query = {
    "bbox": [-10, -10, 10, 10],
    "sensorType": "optical",
    "constellation": ["SPOT"],
    "acquisitionDate": "[2016-07-01,2016-07-31T23:59:59]",
    "incidenceAngle": "20]",
    "cloudCover": "10]",
    "count": 100,
    "startPage": 1,
    "sortKeys": "acquisitionDate"
}
r = requests.post(url, headers=headers, data=json.dumps(query))
print(r.text)
