import json
import os
import re
from typing import Any, Dict, Optional

# Source: https://azure.microsoft.com/en-us/pricing/details/cognitive-services/openai-service/ (spot-checked 2026-04). Users can override via env AZURE_OPENAI_MODEL_PRICING_JSON.
MODEL_PRICING_PER_1K_TOKENS: Dict[str, Dict[str, float]] = {
    "gpt-4o": {"prompt": 0.005, "completion": 0.015},
    "gpt-4o-mini": {"prompt": 0.00015, "completion": 0.0006},
    "gpt-4-turbo": {"prompt": 0.01, "completion": 0.03},
    "gpt-35-turbo": {"prompt": 0.0005, "completion": 0.0015},
    "text-embedding-ada-002": {"prompt": 0.0001, "completion": 0.0},
    "text-embedding-3-small": {"prompt": 0.00002, "completion": 0.0},
    "text-embedding-3-large": {"prompt": 0.00013, "completion": 0.0},
}

_DATE_SUFFIX_RE = re.compile(r"-\d{4}-\d{2}-\d{2}$")


def _normalize_model_name(model_name: Optional[str]) -> str:
    if not model_name:
        return ""
    name = str(model_name).strip().lower()
    return _DATE_SUFFIX_RE.sub("", name)


def load_pricing_overrides() -> Dict[str, Dict[str, float]]:
    raw = os.getenv("AZURE_OPENAI_MODEL_PRICING_JSON")
    if not raw:
        return {}

    try:
        parsed: Any = json.loads(raw)
        if not isinstance(parsed, dict):
            return {}

        normalized: Dict[str, Dict[str, float]] = {}
        for model_name, rates in parsed.items():
            if not isinstance(model_name, str) or not isinstance(rates, dict):
                continue

            prompt = rates.get("prompt")
            completion = rates.get("completion")
            if prompt is None or completion is None:
                continue

            normalized[_normalize_model_name(model_name)] = {
                "prompt": float(prompt),
                "completion": float(completion),
            }

        return normalized
    except Exception as e:
        print(f"[pricing] Warning: failed to parse AZURE_OPENAI_MODEL_PRICING_JSON: {e}")
        return {}


def _get_pricing() -> Dict[str, Dict[str, float]]:
    pricing = {k.lower(): dict(v) for k, v in MODEL_PRICING_PER_1K_TOKENS.items()}
    pricing.update(load_pricing_overrides())
    return pricing


def estimate_cost(
    model_name: Optional[str],
    prompt_tokens: Optional[int],
    completion_tokens: Optional[int],
) -> float:
    try:
        if model_name is None or prompt_tokens is None or completion_tokens is None:
            return 0.0
        if prompt_tokens < 0 or completion_tokens < 0:
            return 0.0
        if prompt_tokens == 0 and completion_tokens == 0:
            return 0.0

        normalized_model = _normalize_model_name(model_name)
        pricing = _get_pricing().get(normalized_model)
        if not pricing:
            return 0.0

        prompt_rate = float(pricing.get("prompt", 0.0))
        completion_rate = float(pricing.get("completion", 0.0))
        prompt_cost = (float(prompt_tokens) / 1000.0) * prompt_rate
        completion_cost = (float(completion_tokens) / 1000.0) * completion_rate
        return float(prompt_cost + completion_cost)
    except Exception:
        return 0.0
