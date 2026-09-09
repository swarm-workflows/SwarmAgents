from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class LlmConfig:
    enabled: bool = False
    provider: str = "none"          # e.g., "openai", "vertex", "ollama", "none"
    model: str = ""
    # Honoured for BOTH `openai` (any OpenAI-compatible gateway) and `ollama`; the
    # provider's *_BASE_URL env var wins over it. Empty means the provider default.
    base_url: str = ""
    # Elicitation (P0-6). The campaign measured 59% of qwen2.5:3b bids at the identical value
    # 75.00 and 92% of gpt-oss-20b bids on just two values: asking for a 0-100 rating gets
    # answers in round steps, so the cost signal is mostly ties and cannot order agents.
    # `score_scale` is the range the model is asked for; the cost is normalised back to 0..100.
    score_scale: int = 100
    temperature: float = 0.0
    timeout_seconds: int = 6
    use_for_selection: bool = True
    prompts: Dict[str, str] = None

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "LlmConfig":
        return LlmConfig(
            enabled=bool(d.get("enabled", False)),
            provider=str(d.get("provider", "none")),
            model=str(d.get("model", "")),
            base_url=str(d.get("base_url", "")),
            score_scale=int(d.get("score_scale", 100) or 100),
            temperature=float(d.get("temperature", 0.0)),
            timeout_seconds=int(d.get("timeout_seconds", 6)),
            use_for_selection=bool(d.get("use_for_selection", True)),
            prompts=dict(d.get("prompts", {})),
        )
