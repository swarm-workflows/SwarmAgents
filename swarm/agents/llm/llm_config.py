from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class LlmConfig:
    enabled: bool = False
    provider: str = "none"          # e.g., "openai", "vertex", "ollama", "none"
    model: str = ""
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
            temperature=float(d.get("temperature", 0.0)),
            timeout_seconds=int(d.get("timeout_seconds", 6)),
            use_for_selection=bool(d.get("use_for_selection", True)),
            prompts=dict(d.get("prompts", {})),
        )
