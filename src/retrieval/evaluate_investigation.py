"""Deterministic evaluation for investigation packets (no LLM calls)."""

from __future__ import annotations

import argparse
import json
import re
from typing import Any

from src.retrieval.investigation_graph import run_investigation_graph


def _parse_query_fields(query: str) -> dict[str, str | None]:
    account_match = re.search(r"\b(ACC\d{3})\b", query, re.IGNORECASE)
    security_match = re.search(r"\b(SEC\d{3})\b", query, re.IGNORECASE)
    date_match = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", query)
    return {
        "account_id": account_match.group(1).upper() if account_match else None,
        "security_id": security_match.group(1).upper() if security_match else None,
        "target_date": date_match.group(1) if date_match else None,
    }


def _evidence_matches_query(
    evidence: dict[str, Any],
    *,
    account_id: str | None,
    security_id: str | None,
    target_date: str | None,
) -> bool:
    if not (account_id and security_id and target_date):
        return False
    if evidence.get("account_id") != account_id:
        return False
    if evidence.get("security_id") != security_id:
        return False
    return evidence.get("position_date") == target_date or evidence.get("event_date") == target_date


def _contains_any_supported_value(summary: str, values: list[str]) -> bool:
    return any(value and value in summary for value in values)


def _unsupported_claim_risk(packet: dict[str, Any]) -> str:
    summary = packet.get("summary", "") or ""
    structured_break = packet.get("structured_break") or {}
    retrieved = packet.get("retrieved_evidence", []) or []

    if summary and not structured_break and not retrieved:
        return "HIGH"

    if structured_break:
        supported_values = [
            str(structured_break.get("account_id", "")),
            str(structured_break.get("security_id", "")),
            str(structured_break.get("position_date", "")),
            str(structured_break.get("expected_position", "")),
            str(structured_break.get("reported_position", "")),
            str(structured_break.get("position_difference", "")),
            str(structured_break.get("break_reason_code", "")),
            str(structured_break.get("root_cause_reason_code", "")),
            str(structured_break.get("severity", "")),
        ]
        return "LOW" if _contains_any_supported_value(summary, supported_values) else "MEDIUM"

    evidence_values: list[str] = []
    for evidence in retrieved:
        evidence_values.extend(
            [
                str(evidence.get("account_id", "")),
                str(evidence.get("security_id", "")),
                str(evidence.get("position_date", "")),
                str(evidence.get("event_date", "")),
                str(evidence.get("document_type", "")),
                str(evidence.get("title", "")),
            ]
        )
    return "LOW" if _contains_any_supported_value(summary, evidence_values) else "MEDIUM"


def evaluate_investigation(query: str, *, top_k: int = 5) -> dict[str, Any]:
    parsed = _parse_query_fields(query)
    packet = run_investigation_graph(query=query, top_k=top_k)

    structured_break = packet.get("structured_break")
    quality_flags = packet.get("quality_flags", []) or []
    event_context = packet.get("event_context", []) or []
    retrieved = packet.get("retrieved_evidence", []) or []
    missing_data_notes = packet.get("missing_data_notes", []) or []

    structured_break_found = bool(
        structured_break
        and structured_break.get("account_id") == parsed["account_id"]
        and structured_break.get("security_id") == parsed["security_id"]
        and structured_break.get("position_date") == parsed["target_date"]
    )

    exact_evidence_found = any(
        _evidence_matches_query(
            evidence,
            account_id=parsed["account_id"],
            security_id=parsed["security_id"],
            target_date=parsed["target_date"],
        )
        for evidence in retrieved
    )

    retrieved_types = {evidence.get("document_type", "") for evidence in retrieved}
    coverage_requirements: list[bool] = []
    if structured_break:
        coverage_requirements.append("RECONCILIATION_BREAK_SUMMARY" in retrieved_types)
    if quality_flags:
        coverage_requirements.append("TRADE_QUALITY_SUMMARY" in retrieved_types)
    evidence_type_coverage = all(coverage_requirements) if coverage_requirements else True

    missing_data_coverage = bool(not event_context and missing_data_notes)
    unsupported_claim_risk = _unsupported_claim_risk(packet)

    passed_checks = [
        structured_break_found,
        exact_evidence_found,
        evidence_type_coverage,
        missing_data_coverage,
        unsupported_claim_risk == "LOW",
    ]
    groundedness_score = round(sum(1 for passed in passed_checks if passed) / len(passed_checks), 2)

    if groundedness_score >= 0.8:
        recommendation = "Investigation packet is well grounded in structured and retrieved evidence."
    elif groundedness_score >= 0.6:
        recommendation = "Investigation packet is usable, but review missing support before relying on summary."
    else:
        recommendation = "Investigation packet needs manual review because grounding support is incomplete."

    return {
        "query": query,
        "checks": {
            "structured_break_found": {
                "passed": structured_break_found,
                "expected_key": {
                    "account_id": parsed["account_id"],
                    "security_id": parsed["security_id"],
                    "target_date": parsed["target_date"],
                },
            },
            "exact_evidence_found": {
                "passed": exact_evidence_found,
                "matching_document_ids": [
                    evidence.get("document_id")
                    for evidence in retrieved
                    if _evidence_matches_query(
                        evidence,
                        account_id=parsed["account_id"],
                        security_id=parsed["security_id"],
                        target_date=parsed["target_date"],
                    )
                ],
            },
            "evidence_type_coverage": {
                "passed": evidence_type_coverage,
                "retrieved_types": sorted(retrieved_types),
            },
            "missing_data_coverage": {
                "passed": missing_data_coverage,
                "event_context_count": len(event_context),
                "missing_data_notes_count": len(missing_data_notes),
            },
        },
        "groundedness_score": groundedness_score,
        "unsupported_claim_risk": unsupported_claim_risk,
        "recommendation": recommendation,
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Evaluate deterministic investigation groundedness.",
    )
    parser.add_argument("--query", required=True, help="Investigation query text.")
    parser.add_argument("--top-k", type=int, default=5, help="Top-k retrieved evidence docs.")
    args = parser.parse_args(argv)

    evaluation = evaluate_investigation(query=args.query, top_k=args.top_k)
    print(json.dumps(evaluation, indent=2))


if __name__ == "__main__":
    main()
