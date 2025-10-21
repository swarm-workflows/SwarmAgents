# MIT License
#
# Copyright (c) 2025 swarm-workflows
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Author: Komal Thareja(kthare10@renci.org)
# llm_bidder.py
from typing import Dict, Any

from pydantic import BaseModel, Field

from swarm.agents.llm_agent.llm_config import LlmConfig

# --- PydanticAI-based bidder ---------------------------------------------------------------
try:
    from pydantic_ai import Agent as PydanticAgent  # type: ignore
    from pydantic_ai.models.openai import OpenAIChatModel  # type: ignore
    try:
        from pydantic_ai.models.gemini import GeminiModel  # type: ignore
    except Exception:  # optional provider
        GeminiModel = None  # type: ignore
except Exception:
    PydanticAgent = None  # type: ignore
    OpenAIChatModel = None  # type: ignore
    GeminiModel = None  # type: ignore


class Bid(BaseModel):
    score: float = Field(ge=0.0, le=100.0)
    explanation: str = ""


class LlmBidder:
    """Provider-agnostic LLM bidder using PydanticAI for structured outputs.

    Falls back by raising if PydanticAI or the chosen provider isn't available;
    caller should catch and switch to analytic scoring.
    """
    def __init__(self, cfg: LlmConfig, logger=None):
        self.cfg = cfg
        self.logger = logger
        if not PydanticAgent:
            raise RuntimeError("PydanticAI not installed. `pip install pydantic-ai`.")

        provider = (cfg.provider or "").lower()
        model_name = cfg.model or "gpt-4o-mini"

        if provider == "openai":
            if not OpenAIChatModel:
                raise RuntimeError("OpenAI model adapter unavailable.")
            model = OpenAIChatModel(model_name)
        elif provider in {"gemini", "gemma"}:
            if not GeminiModel:
                raise RuntimeError("Gemini/Gemma adapter unavailable.")
            model = GeminiModel(model_name)
        else:
            raise ValueError(f"Unsupported provider: {cfg.provider}")

        system_prompt = (cfg.prompts or {}).get("cost") or (
            "You are a scheduler. Given a JSON job and an agent's resource state, "
            "return a JSON with fields: score (0..100, higher better), explanation (str)."
        )
        self.agent = PydanticAgent[Bid](
            model=model,
            system_prompt=system_prompt,
            temperature=cfg.temperature,
        )

    def score(self, *, job: Dict[str, Any], agent_state: Dict[str, Any]) -> Bid:
        res = self.agent.run(
            "Given job and agent state, produce a score and short explanation.",
            input={"job": job, "agent": agent_state},
        )
        return res.output
