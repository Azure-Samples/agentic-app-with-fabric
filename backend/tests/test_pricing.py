import importlib

from shared import pricing


def _reload_pricing():
    return importlib.reload(pricing)


def test_estimate_cost_known_model():
    module = _reload_pricing()
    cost = module.estimate_cost("gpt-4o-mini", 1000, 500)
    expected = (1000 / 1000) * module.MODEL_PRICING_PER_1K_TOKENS["gpt-4o-mini"]["prompt"] + (500 / 1000) * module.MODEL_PRICING_PER_1K_TOKENS["gpt-4o-mini"]["completion"]
    assert abs(cost - expected) < 0.0001


def test_estimate_cost_unknown_model_returns_zero():
    module = _reload_pricing()
    assert module.estimate_cost("unknown-model", 1000, 500) == 0.0


def test_estimate_cost_none_inputs_return_zero():
    module = _reload_pricing()
    assert module.estimate_cost(None, 1000, 500) == 0.0
    assert module.estimate_cost("gpt-4o-mini", None, 500) == 0.0
    assert module.estimate_cost("gpt-4o-mini", 1000, None) == 0.0


def test_estimate_cost_case_insensitive_model_match():
    module = _reload_pricing()
    lower = module.estimate_cost("gpt-4o-mini", 1000, 500)
    mixed = module.estimate_cost("GPT-4O-MINI", 1000, 500)
    assert abs(lower - mixed) < 1e-9


def test_estimate_cost_date_suffixed_model_name_maps_to_base():
    module = _reload_pricing()
    base = module.estimate_cost("gpt-4o-mini", 1000, 500)
    suffixed = module.estimate_cost("gpt-4o-mini-2024-11-20", 1000, 500)
    assert abs(base - suffixed) < 1e-9


def test_env_override_merges_without_breaking_known_models(monkeypatch):
    monkeypatch.setenv(
        "AZURE_OPENAI_MODEL_PRICING_JSON",
        '{"gpt-4o-mini": {"prompt": 0.0002, "completion": 0.0007}, "custom-model": {"prompt": 0.01, "completion": 0.02}}',
    )
    module = _reload_pricing()

    overridden = module.estimate_cost("gpt-4o-mini", 1000, 1000)
    assert abs(overridden - 0.0009) < 0.0001

    preserved = module.estimate_cost("gpt-4o", 1000, 1000)
    expected_preserved = module.MODEL_PRICING_PER_1K_TOKENS["gpt-4o"]["prompt"] + module.MODEL_PRICING_PER_1K_TOKENS["gpt-4o"]["completion"]
    assert abs(preserved - expected_preserved) < 0.0001


def test_malformed_env_falls_back_to_defaults(monkeypatch):
    monkeypatch.setenv("AZURE_OPENAI_MODEL_PRICING_JSON", "{not valid json")
    module = _reload_pricing()

    cost = module.estimate_cost("gpt-4o-mini", 1000, 500)
    expected = (1000 / 1000) * module.MODEL_PRICING_PER_1K_TOKENS["gpt-4o-mini"]["prompt"] + (500 / 1000) * module.MODEL_PRICING_PER_1K_TOKENS["gpt-4o-mini"]["completion"]
    assert abs(cost - expected) < 0.0001


def test_embedding_prompt_only_usage_is_billed():
    # text-embedding-ada-002 has prompt=0.0001, completion=0.0. A prompt-only
    # call (completion_tokens=0) must return a non-zero cost.
    module = _reload_pricing()
    cost = module.estimate_cost("text-embedding-ada-002", 10_000, 0)
    # 10_000 prompt tokens / 1000 * 0.0001 = 0.001
    assert abs(cost - 0.001) < 1e-9


def test_prompt_only_chat_call_is_billed():
    # A chat call that produced a prompt but no completion (stream cutoff,
    # content filter) still consumed prompt tokens and must be billed.
    module = _reload_pricing()
    cost = module.estimate_cost("gpt-4o-mini", 1000, 0)
    expected = (1000 / 1000) * module.MODEL_PRICING_PER_1K_TOKENS["gpt-4o-mini"]["prompt"]
    assert abs(cost - expected) < 1e-9


def test_zero_on_both_sides_returns_zero():
    module = _reload_pricing()
    assert module.estimate_cost("gpt-4o-mini", 0, 0) == 0.0


def test_negative_tokens_return_zero():
    module = _reload_pricing()
    assert module.estimate_cost("gpt-4o-mini", -1, 100) == 0.0
    assert module.estimate_cost("gpt-4o-mini", 100, -1) == 0.0
