import requests

response = requests.post(
    "http://localhost:11434/v1/chat/completions",
    json={
        "model": "qwen2.5:32b",
        "messages": [{"role": "user", "content": "reply with the word OK only"}]
    },
    timeout=120
)
print(response.status_code)
print(response.json())