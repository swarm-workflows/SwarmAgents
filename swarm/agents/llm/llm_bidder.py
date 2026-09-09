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

from pydantic import BaseModel, Field, create_model
from pydantic_ai import Agent as PydanticAgent, ModelSettings, NativeOutput

# Models
from pydantic_ai.models.google import GoogleModel
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.ollama import OllamaProvider
from pydantic_ai.providers.openai import OpenAIProvider

from swarm.agents.llm.llm_config import LlmConfig


class Bid(BaseModel):
    """Structured score from the LLM, on the 0..100 default scale."""
    score: float = Field(ge=0.0, le=100.0)
    explanation: str = ""
    reasoning_time: float | None = None   # seconds


def bid_model_for_scale(scale: int) -> type[BaseModel]:
    """Build the `Bid` schema for a 0..`scale` rating.

    The bound is in the schema, not just the prompt, because Ollama's `NativeOutput` mode drives
    the model from the JSON schema — a prompt that says 0-1000 while the schema says 0-100 gets
    silently clamped answers. Returns the plain `Bid` for the default scale so the common path
    keeps a stable, importable type.
    """
    if int(scale) == 100:
        return Bid
    return create_model(
        f"Bid{int(scale)}",
        score=(float, Field(ge=0.0, le=float(scale))),
        explanation=(str, ""),
        reasoning_time=(Optional[float], None),
    )


def build_model(cfg: LlmConfig):
    """Resolve `cfg.provider`/`cfg.model` to a pydantic-ai model object.

    Shared by `LlmBidder` and `LlmDelegator` so the two LLM call sites cannot drift on which
    providers exist or on how the base URL and API key are resolved.
    """
    provider = (cfg.provider or "").strip().lower()
    model_name = (cfg.model or "gpt-4o-mini").strip()

    if provider == "openai":
        # `llm.base_url` was parsed into LlmConfig and then read only by the ollama branch, so a
        # config naming an OpenAI-COMPATIBLE gateway (the FABRIC one at
        # https://ai.fabric-testbed.net/v1, say) silently talked to api.openai.com instead —
        # the key promising one endpoint and delivering another. Same shape as the unenforced
        # `timeout_seconds` in §0.2. Env still wins, so a proxy can be interposed per host.
        base_url = os.getenv("OPENAI_BASE_URL") or cfg.base_url or ""
        if base_url:
            return OpenAIChatModel(model_name, provider=OpenAIProvider(
                base_url=base_url, api_key=os.getenv("OPENAI_API_KEY") or ""))
        return OpenAIChatModel(model_name)  # stock OpenAI, via env OPENAI_API_KEY
    if provider in {"gemini", "gemma", "google"}:
        return GoogleModel(model_name)  # uses Google credentials envs
    if provider == "ollama":
        # Env wins over config so a proxy can be interposed without rewriting per-agent configs.
        base_url = os.getenv("OLLAMA_BASE_URL") or cfg.base_url or "http://localhost:11434/v1"
        api_key = os.getenv("OLLAMA_API_KEY") or "ollama"  # local Ollama ignores it, client requires non-empty
        return OpenAIChatModel(model_name, provider=OllamaProvider(base_url=base_url, api_key=api_key))
    raise ValueError(
        f"Unsupported provider: {cfg.provider!r}. Use 'openai', 'gemini'/'gemma', or 'ollama'."
    )


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

        # Provider resolution is shared with LlmDelegator (see build_model).
        provider = (cfg.provider or "").strip().lower()
        model = build_model(cfg)

        # Rating range asked for. 100 keeps the schema and prompt the campaign measured.
        self.score_scale = int(getattr(cfg, "score_scale", 100) or 100)
        if self.score_scale < 2:
            raise ValueError(f"llm.score_scale must be >= 2, got {self.score_scale}")
        bid_model = bid_model_for_scale(self.score_scale)

        # Small local models emit malformed tool-call args; Ollama's json_schema mode is reliable.
        self.output_type = NativeOutput(bid_model) if provider == "ollama" else bid_model

        system_prompt = (cfg.prompts or {}).get("cost") or (
            "You are a scheduler. Given a JSON job and an agent's resource state, "
            "return a JSON with fields: score (0..{scale}, higher is better) and "
            "explanation (short string). Respond strictly in JSON."
        )
        # The prompt states the range, so a configured prompt written for 0-100 stays truthful
        # when the scale changes. `{scale}` is the only placeholder; a prompt without it is used
        # verbatim, and a stray brace must not blow up startup.
        try:
            system_prompt = system_prompt.format(scale=self.score_scale)
        except (KeyError, IndexError, ValueError):
            if self.logger:
                self.logger.warning(
                    "[LLM_BIDDER] prompt has braces that are not '{scale}'; using it verbatim. "
                    "It may still advertise the wrong rating range.")
        self.logger.info(f"[LLM_BIDDER] System Prompt: {system_prompt}")

        # Build the agent with a structured result type.
        self.agent: PydanticAgent = PydanticAgent(
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
        timeout_s = float(getattr(self.cfg, "timeout_seconds", 0) or 0)
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
            if peer_context and peer_context.get('peer_agents'):
                loads = ",".join(f"{k}:{v.get('load',0)}" for k, v in peer_context['peer_agents'].items())
                peer_info = f"\nPEERS({peer_context.get('total_agents','?')}agents):[{loads}]"

            prompt = (
                f"JOB:{json.dumps(job, ensure_ascii=False, separators=(',', ':'))}\n"
                f"AGENT:{json.dumps(agent_state, ensure_ascii=False, separators=(',', ':'))}"
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
                output_type=self.output_type,
                model_settings=ModelSettings(
                    temperature=(self.cfg.temperature if hasattr(self.cfg, "temperature") else 0.0),
                    # `llm.timeout_seconds` was parsed into LlmConfig and then used nowhere, so
                    # the key promised a bounded bid and delivered none: bids of 14.9s and 19.2s
                    # were observed under `timeout_seconds: 6`, and one misconfiguration blocked
                    # a single call for 20 minutes. On breach the request raises and the caller's
                    # existing except-path falls back (or, with llm.disable_fallback, abstains) —
                    # which is the resilience behaviour the agent already advertises. 0 disables.
                    **({"timeout": float(timeout_s)} if timeout_s and timeout_s > 0 else {}),
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
