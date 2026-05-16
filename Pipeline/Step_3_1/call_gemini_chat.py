import json
import os
import urllib.error
import urllib.request


def call_gemini_chat(messages, model=None):
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "GEMINI_API_KEY must be set to generate suggested headers for files without a header."
        )

    model = model or os.environ.get("GEMINI_MODEL", "gemini-flash-latest")
    base_url = os.environ.get("GEMINI_API_BASE", "https://generativelanguage.googleapis.com").rstrip("/")
    url = f"{base_url}/v1beta/models/{model}:generateContent"

    text = "\n\n".join(
        [f"{msg['role'].upper()}: {msg['content']}" for msg in messages]
    )

    payload = {
        "contents": [
            {
                "parts": [
                    {"text": text}
                ]
            }
        ]
    }

    request_data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=request_data,
        headers={
            "Content-Type": "application/json",
            "X-goog-api-key": api_key,
        },
    )

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response_json = json.loads(response.read().decode("utf-8"))
            contents = response_json.get("contents", [])
            if contents and isinstance(contents[0], dict):
                parts = contents[0].get("parts", [])
                if parts and isinstance(parts[0], dict):
                    return parts[0].get("text", "")
            return ""
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Gemini request failed: {exc.code} {exc.reason}") from exc
    except Exception as exc:
        raise RuntimeError(f"Gemini request failed: {exc}") from exc