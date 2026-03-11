import os
import time
from typing import Any
from langchain_gigachat.chat_models import GigaChat


class LLMClient:
    def __init__(self) -> None:
        self.llm = GigaChat(
            base_url=os.getenv("GIGACHAT_API_URL"),
            access_token=os.getenv("JPY_API_TOKEN"),
            model="GigaChat-2-Max",
            timeout=120,
        )

    def invoke(self, prompt: str, max_retries: int = 5, sleep_s: int = 5) -> Any:
        last_err = None
        for _ in range(max_retries):
            try:
                resp = self.llm.invoke(prompt)
                time.sleep(sleep_s)
                return resp
            except Exception as e:
                last_err = e
                time.sleep(sleep_s)
        raise RuntimeError(f"LLM failed after {max_retries} retries: {last_err}")
