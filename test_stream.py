import requests
import sseclient

URL = "https://stream.wikimedia.org/v2/stream/recentchange"

print("Connecting...")
headers = {
    "Accept": "text/event-stream",
    "User-Agent": "VortexAI/1.0 (Learning project; contact: your-email@example.com)",
}
response = requests.get(URL, stream=True, headers=headers, timeout=10)
print(f"Status code: {response.status_code}")
response.raise_for_status()

client = sseclient.SSEClient(response)

count = 0
for event in client.events():
    print(f"Got event #{count}: type={event.event}, data preview={str(event.data)[:80]}")
    count += 1
    if count >= 5:
        break

print("Success — stream is reachable.")