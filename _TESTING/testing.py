import requests

response=requests.get("http://127.0.0.1:25510/v2/list/expirations?root=AAGR1").json()

print(response)