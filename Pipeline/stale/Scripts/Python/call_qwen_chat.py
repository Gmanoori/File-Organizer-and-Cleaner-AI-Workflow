import os
import requests
from dotenv import load_dotenv
import time


MAX_RETRIES = 3
RETRY_DELAYS = [5, 15, 30]  # seconds to wait between retries


load_dotenv()  # Loads variables from .env
# api_key = os.getenv("HF_TOKEN")


def call_qwen_chat(messages, model=None):
    # api_key = os.environ.get("HF_TOKEN")
    # if not api_key:
    #     raise RuntimeError(
    #         "HF_TOKEN must be set to generate suggested headers for files without a header."
    #     )

    API_URL = "http://localhost:11434/v1/chat/completions"

    model = model or os.environ.get("QWEN_MODEL", "qwen2.5-coder:7b")

    payload = {
        "messages": messages,
        "model": model,
        "stream": False,
        "max_tokens": 200-300

    }

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(API_URL, json=payload, timeout=120)  # increased to 60s
            response.raise_for_status()
            result = response.json()
            return result["choices"][0]["message"]["content"]

        except requests.exceptions.Timeout:
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_DELAYS[attempt]
                print(f"  Timeout on attempt {attempt + 1}/{MAX_RETRIES}, retrying in {wait}s...")
                time.sleep(wait)
                continue
            raise RuntimeError(f"Qwen request failed after {MAX_RETRIES} attempts: read timeout") from None

        except requests.exceptions.RequestException as exc:
            try:
                error_detail = exc.response.text if hasattr(exc, "response") and exc.response else str(exc)
                print(f"API Error Details: {error_detail}")
            except Exception:
                pass
            # Don't retry on auth errors (401) or bad requests (400) — only on 5xx/timeout
            if hasattr(exc, "response") and exc.response is not None:
                status = exc.response.status_code
                if status in (400, 401, 403, 422):
                    raise RuntimeError(f"Qwen request failed (no retry on {status}): {exc}") from exc
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_DELAYS[attempt]
                print(f"  Request error on attempt {attempt + 1}/{MAX_RETRIES}, retrying in {wait}s...")
                time.sleep(wait)
                continue
            raise RuntimeError(f"Qwen request failed: {exc}") from exc

        except KeyError as exc:
            raise RuntimeError(f"Unexpected response format: {exc}") from exc