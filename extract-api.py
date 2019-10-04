import requests
import json

# Fetch the Hackernews post as JSON
resp_json = requests.get("http://www.omdbapi.com/?apikey=b0bb3b02&s=The Avengers").json()

# Print the response parsed as JSON
print(json.dumps(resp_json, indent=4))

# Assign the title of the first result item
print(resp_json['Search'][0]['Title'])