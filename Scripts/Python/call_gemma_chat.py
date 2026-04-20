import os
import requests
from dotenv import load_dotenv

load_dotenv()  # Loads variables from .env
api_key = os.getenv("HF_TOKEN")


def call_gemma_chat(messages, model=None):
    # api_key = os.environ.get("HF_TOKEN")
    if not api_key:
        raise RuntimeError(
            "HF_TOKEN must be set to generate suggested headers for files without a header."
        )

    API_URL = "https://router.huggingface.co/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
    }

    model = model or os.environ.get("GEMMA_MODEL", "google/gemma-4-31B-it:fastest")

    payload = {
        "messages": messages,
        "model": model
    }

    try:
        response = requests.post(API_URL, headers=headers, json=payload)
        response.raise_for_status()
        result = response.json()
        return result["choices"][0]["message"]["content"]
    except requests.exceptions.RequestException as exc:
        # Print full response for debugging
        try:
            error_detail = exc.response.text if hasattr(exc, 'response') else str(exc)
            print(f"API Error Details: {error_detail}")
        except:
            pass
        raise RuntimeError(f"Gemma request failed: {exc}") from exc
    except KeyError as exc:
        raise RuntimeError(f"Unexpected response format: {exc}") from exc