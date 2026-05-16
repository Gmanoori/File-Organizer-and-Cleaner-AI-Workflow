import time
from call_gemma_chat import call_gemma_chat


def test_llm_basic():
    messages = [
        {
            "role": "user",
            "content": "Write a short paragraph (5-6 sentences) about how data cleaning improves machine learning models. Keep it simple."
        }
    ]

    print("Sending test prompt to LLM...\n")

    start_time = time.time()

    try:
        response = call_gemma_chat(messages)
        end_time = time.time()

        print("\n--- RESPONSE RECEIVED ---\n")
        print(response)
        print("\n-------------------------\n")

        print(f"Response time: {round(end_time - start_time, 2)} seconds")

    except Exception as e:
        end_time = time.time()
        print("\n--- ERROR ---\n")
        print(str(e))
        print(f"\nTime before failure: {round(end_time - start_time, 2)} seconds")


if __name__ == "__main__":
    test_llm_basic()