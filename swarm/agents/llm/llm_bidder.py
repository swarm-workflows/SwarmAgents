# llm_bidder.py
# MIT License
#
# Author: Komal Thareja (kthare10@renci.org)

from __future__ import annotations

import json
import logging
import os
import time
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
    reasoning_time: float | None = None   # seconds


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
        self.logger.info(f"[LLM_BIDDER] System Prompt: {system_prompt}")

        # Build the agent with a structured result type.
        self.agent: PydanticAgent[Bid] = PydanticAgent[Bid](
            model=model,
            system_prompt=system_prompt,
        )

    def score(self, *, job: Dict[str, Any], agent_state: Dict[str, Any],
              peer_context: Optional[Dict[str, Any]] = None) -> Bid:
        """
        Synchronously obtain a Bid from the LLM with optional peer context.

        :param job: Job information as dict
        :param agent_state: Agent resource state as dict
        :param peer_context: Optional peer information for load-aware scoring
        :return: Bid with score, explanation, and reasoning time
        """
        try:
            # Log LLM scoring start
            job_id = job.get('job_id', job.get('id', 'unknown'))
            agent_id = agent_state.get('agent_id', 'unknown')

            if self.logger:
                peer_count = len(peer_context.get('peer_agents', {})) if peer_context else 0
                self.logger.info(
                    f"[LLM_SCORE_START] Job={job_id} Agent={agent_id} "
                    f"Provider={self.cfg.provider} Model={self.cfg.model} "
                    f"PeerContext={'yes' if peer_context else 'no'} Peers={peer_count}"
                )

            # Prepare peer context summary if provided
            peer_info = ""
            if peer_context:
                peer_agents = ", ".join([f"Agent{k}: {v}" for k, v in peer_context.get('peer_agents', {}).items()])
                peer_info = (
                    f"\n\nPEER CONTEXT:\n"
                    f"- Total agents: {peer_context.get('total_agents', 'unknown')}\n"
                    f"- Peer Agents: {peer_agents if peer_agents else 'no peers'}\n"
                    f"- Recent conflicts: {peer_context.get('conflicts', 0)}\n"
                    f"- Topology: {peer_context.get('topology', 'mesh')}\n"
                    #f"- Child Agents: {peer_context.get('child_agents', 'no children')}\n"
                )

            prompt = (
                "Given the job and agent state below, return ONLY JSON with fields:\n"
                "  - score: number in [0, 100] (higher = better fit)\n"
                "  - explanation: SHORT string (max 15 words)\n\n"
                f"JOB:\n{json.dumps(job, ensure_ascii=False, indent=2)}\n\n"
                f"AGENT_STATE:\n{json.dumps(agent_state, ensure_ascii=False, indent=2)}"
                f"{peer_info}"
            )

            # Log the prompt at DEBUG level for detailed troubleshooting
            if self.logger:
                self.logger.debug(
                    f"[LLM_PROMPT] Job={job_id} Agent={agent_id}\n"
                    f"Prompt length: {len(prompt)} chars\n"
                    f"Full prompt:\n{prompt}"
                )

            start = time.perf_counter()
            res = self.agent.run_sync(
                prompt,
                output_type=Bid,
                model_settings=ModelSettings(
                    temperature=(self.cfg.temperature if hasattr(self.cfg, "temperature") else 0.0),
                ),
            )
            bid = res.output
            bid.reasoning_time = time.perf_counter() - start

            # Truncate long explanations to save tokens and improve performance
            if len(bid.explanation) > 100:
                bid.explanation = bid.explanation[:97] + "..."

            # Log successful LLM scoring completion
            if self.logger:
                self.logger.info(
                    f"[LLM_SCORE_COMPLETE] Job={job_id} Agent={agent_id} "
                    f"Score={bid.score:.2f} ReasoningTime={bid.reasoning_time:.3f}s "
                    f"Explanation=\"{bid.explanation}\""
                )

            return bid
        except Exception as e:
            if self.logger:
                self.logger.exception(
                    f"[LLM_SCORE_ERROR] Job={job.get('job_id', job.get('id', 'unknown'))} "
                    f"Agent={agent_state.get('agent_id', 'unknown')} Error: %s", e
                )
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
        "wall_time": 1.3,
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
