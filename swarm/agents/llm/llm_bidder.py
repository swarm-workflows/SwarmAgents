# llm_bidder.py
# MIT License
#
# Author: Komal Thareja (kthare10@renci.org)

from __future__ import annotations

import json
import logging
import os
from typing import Dict, Any, Optional

from pydantic import BaseModel, Field
from pydantic_ai import Agent as PydanticAgent, ModelSettings

# Models
from pydantic_ai.models.google import GoogleModel
from pydantic_ai.models.openai import OpenAIChatModel

from swarm.agents.llm.llm_config import LlmConfig


class Bid(BaseModel):
    """Structured score from the LLM."""
    score: float = Field(ge=0.0, le=100.0)
    explanation: str = ""


class ScoringDeps(BaseModel):
    job: Dict[str, Any]
    agent: Dict[str, Any]


class LlmBidder:
    """
    Provider-agnostic LLM bidder using PydanticAI for structured outputs.

    Supports providers:
      - openai       -> OpenAIChatModel + OpenAIProvider
      - gemini/gemma -> GoogleModel
      - ollama       -> OpenAIChatModel + OllamaProvider (or OpenAIProvider with base_url)
    """

    def __init__(self, cfg: LlmConfig, logger: Optional[Any] = None):
        if PydanticAgent is None:
            raise RuntimeError("PydanticAI not installed. `pip install pydantic-ai`.")

        self.cfg = cfg
        self.logger = logger

        provider = (cfg.provider or "").strip().lower()
        model_name = (cfg.model or "gpt-4o-mini").strip()

        # ---------- Resolve provider/model ----------
        if provider == "openai":
            model = OpenAIChatModel(model_name)  # uses OpenAIProvider via env OPENAI_API_KEY
        elif provider in {"gemini", "gemma", "google"}:
            model = GoogleModel(model_name)  # uses Google credentials envs
        elif provider == "ollama":
            base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1")
            api_key = os.getenv("OLLAMA_API_KEY")  # optional (needed for Ollama Cloud)
            model = OpenAIChatModel(model_name, provider="ollama")
        else:
            raise ValueError(
                f"Unsupported provider: {cfg.provider!r}. Use 'openai', 'gemini'/'gemma', or 'ollama'."
            )

        system_prompt = (cfg.prompts or {}).get("cost") or (
            "You are a scheduler. Given a JSON job and an agent's resource state, "
            "return a JSON with fields: score (0..100, higher is better) and "
            "explanation (short string). Respond strictly in JSON."
        )

        # Build the agent with a structured result type.
        self.agent: PydanticAgent[Bid] = PydanticAgent[Bid](
            model=model,
            system_prompt=system_prompt,
        )

    def score(self, *, job: Dict[str, Any], agent_state: Dict[str, Any]) -> Bid:
        """
        Synchronously obtain a Bid from the LLM.
        """
        try:
            prompt = (
                "Given the job and agent state below, return ONLY JSON with fields:\n"
                "  - score: number in [0, 100]\n"
                "  - explanation: short string\n\n"
                f"JOB:\n{json.dumps(job, ensure_ascii=False, indent=2)}\n\n"
                f"AGENT_STATE:\n{json.dumps(agent_state, ensure_ascii=False, indent=2)}\n"
            )

            res = self.agent.run_sync(
                prompt,
                output_type=Bid,
                model_settings=ModelSettings(
                    temperature=(self.cfg.temperature if hasattr(self.cfg, "temperature") else 0.0),
                ),
            )
            # With Agent[Bid], res.output is already a Bid instance.
            return res.output
        except Exception as e:
            if self.logger:
                self.logger.exception("LLM scoring failed: %s", e)
            raise


if __name__ == "__main__":
    cfg_dict = {
        "enabled": True,
        "provider": "openai",  # change to "ollama" to use local/remote Ollama
        "model": "gpt-4o-mini",  # e.g., for Ollama: "llama3.1" / "qwen2.5" / etc.
        # Optional: include 'temperature' in your LlmConfig if you expose it
        # "temperature": 0.1,
    }

    job = {
        "id": "1",
        "execution_time": 1.3,
        "capacities": {"core": 1.5, "ram": 5.83, "disk": 8.37, "gpu": 0},
        "data_in": [{"name": "dtn4", "file": "/var/tmp/outgoing/file500M.txt"}],
        "data_out": [{"name": "dtn4", "file": "/var/tmp/outgoing/file100M.txt"}],
        "exit_status": 0,
    }

    agent_info = {
        "core": 2,
        "ram": 8,
        "disk": 100,
        "gpu": 0,
        "dtns": [
            {"name": "dtn4", "ip": "192.168.100.4", "user": "dtn_user4", "connectivity_score": 0.76}
        ],
    }

    model = LlmBidder(cfg=LlmConfig.from_dict(cfg_dict), logger=logging.getLogger(__name__))
    print(model.score(job=job, agent_state=agent_info))
