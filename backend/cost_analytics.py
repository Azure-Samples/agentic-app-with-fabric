from datetime import datetime, timedelta
from typing import Any, Dict, List

from flask import Blueprint, jsonify, request
from sqlalchemy import and_, or_

from chat_data_model import ChatHistory
from shared.pricing import estimate_cost

cost_analytics_bp = Blueprint("cost_analytics", __name__, url_prefix="/api/analytics")


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _row_cost(row: ChatHistory) -> float:
    if row.estimated_cost_usd is not None:
        return _safe_float(row.estimated_cost_usd)
    return estimate_cost(row.model_name, row.prompt_tokens, row.completion_tokens)


def _usage_filter(query):
    return query.filter(
        or_(
            ChatHistory.estimated_cost_usd.isnot(None),
            and_(
                ChatHistory.prompt_tokens.isnot(None),
                ChatHistory.completion_tokens.isnot(None),
            ),
        )
    )


@cost_analytics_bp.route('/cost-summary', methods=['GET'])
def get_cost_summary():
    try:
        days = int(request.args.get('days', 7))
        if days <= 0:
            days = 7
    except Exception:
        days = 7

    cutoff = datetime.now() - timedelta(days=days)
    rows: List[ChatHistory] = _usage_filter(
        ChatHistory.query.filter(ChatHistory.trace_end >= cutoff)
    ).all()

    total_prompt_tokens = 0
    total_completion_tokens = 0
    total_tokens = 0
    total_cost_usd = 0.0
    traces = set()
    by_agent_cost: Dict[str, float] = {}
    by_model_cost: Dict[str, float] = {}

    for row in rows:
        prompt_tokens = row.prompt_tokens or 0
        completion_tokens = row.completion_tokens or 0
        row_total_tokens = row.total_tokens if row.total_tokens is not None else (prompt_tokens + completion_tokens)
        row_cost = _row_cost(row)

        total_prompt_tokens += prompt_tokens
        total_completion_tokens += completion_tokens
        total_tokens += row_total_tokens
        total_cost_usd += row_cost

        if row.trace_id:
            traces.add(row.trace_id)

        agent_name = row.agent_name or "unknown"
        by_agent_cost[agent_name] = by_agent_cost.get(agent_name, 0.0) + row_cost

        model_name = row.model_name or "unknown"
        by_model_cost[model_name] = by_model_cost.get(model_name, 0.0) + row_cost

    conversations = len(traces)
    avg_cost_per_conversation = (total_cost_usd / conversations) if conversations > 0 else 0.0

    by_agent = []
    for agent_name, cost in sorted(by_agent_cost.items(), key=lambda kv: kv[1], reverse=True):
        percent = (cost / total_cost_usd * 100.0) if total_cost_usd > 0 else 0.0
        by_agent.append({
            "agent_name": agent_name,
            "cost_usd": round(cost, 6),
            "percent": round(percent, 2),
        })

    by_model = [
        {"model_name": model_name, "cost_usd": round(cost, 6)}
        for model_name, cost in sorted(by_model_cost.items(), key=lambda kv: kv[1], reverse=True)
    ]

    return jsonify({
        "total_prompt_tokens": total_prompt_tokens,
        "total_completion_tokens": total_completion_tokens,
        "total_tokens": total_tokens,
        "total_cost_usd": round(total_cost_usd, 6),
        "conversations": conversations,
        "avg_cost_per_conversation": round(avg_cost_per_conversation, 6),
        "by_agent": by_agent,
        "by_model": by_model,
    })


@cost_analytics_bp.route('/trace/<trace_id>/cost', methods=['GET'])
def get_trace_cost(trace_id: str):
    rows: List[ChatHistory] = _usage_filter(
        ChatHistory.query.filter_by(trace_id=trace_id)
    ).order_by(ChatHistory.routing_step.asc(), ChatHistory.trace_end.asc()).all()

    total_cost = 0.0
    per_step = []

    for row in rows:
        prompt_tokens = row.prompt_tokens or 0
        completion_tokens = row.completion_tokens or 0
        row_total_tokens = row.total_tokens if row.total_tokens is not None else (prompt_tokens + completion_tokens)
        cost = _row_cost(row)
        total_cost += cost

        per_step.append({
            "agent_name": row.agent_name or "unknown",
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": row_total_tokens,
            "cost_usd": round(cost, 6),
            "model_name": row.model_name,
        })

    return jsonify({
        "trace_id": trace_id,
        "total_cost_usd": round(total_cost, 6),
        "per_step": per_step,
    })
