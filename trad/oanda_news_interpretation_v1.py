"""Auditable headline interpretation, separate from published forecast factors.

Directions describe textual claims or conditional economic mechanisms, not price
predictions. No magnitude, probability, surprise or causal attribution is inferred.
"""
from __future__ import annotations

import hashlib
import re

VERSION = "fx_headline_interpretation_v1"
BANKS = {"fed": "USD", "federal reserve": "USD", "ecb": "EUR",
         "european central bank": "EUR", "bank of england": "GBP",
         "boe": "GBP", "bank of japan": "JPY", "boj": "JPY"}
BANK_PATTERN = r"\b(?:" + "|".join(sorted(BANKS, key=len, reverse=True)) + r")\b"
UNCERTAIN = r"\b(?:may|might|could|would|if|expects?|expected|anticipates?|forecast|await|awaits|likely|unlikely|not|no|never|denies|denied|rules? out|ruled out|calls? on|urges?)\b"
ACTION = re.compile(r"\b(?P<verb>rais(?:e|es|ed|ing)|hik(?:e|es|ed|ing)|cut(?:s|ting)?|lower(?:s|ed|ing)?)\s+(?:its\s+|the\s+)?(?:interest\s+|policy\s+)?rates?\b", re.I)


def interpret_headline(headline: str) -> dict:
    """Only bind explicit headline evidence; do not use outcomes or article age."""
    text = " ".join(str(headline).split())
    claims = []

    def add(kind, evidence, *, currency=None, direction=None, status="reported",
            mechanism=None, instrument=None):
        claims.append({"kind": kind, "evidence": evidence, "currency": currency,
                       "direction": direction, "status": status,
                       "mechanism": mechanism, "instrument": instrument})

    # Bind each clause independently: Fed hikes / ECB cuts must not share signs.
    clauses = re.split(r"[;!?]|\s+(?:while|whereas|but|and)\s+", text, flags=re.I)
    for clause in clauses:
        banks = list(re.finditer(BANK_PATTERN, clause, re.I))
        if len(banks) != 1:
            if banks:
                add("policy_subject_ambiguous", clause, status="unresolved")
            continue
        bank = banks[0]
        currency = BANKS[bank.group().lower()]
        # A forecast/outlook label alone is not uncertainty about an explicit
        # adjective ("EUR/USD outlook: Hawkish Fed"). Modal language is.
        uncertain = bool(re.search(UNCERTAIN, clause, re.I))
        action = ACTION.search(clause, bank.end())
        if action:
            between = clause[bank.end():action.start()].strip()
            # Do not bind "Fed says ECB..." or a distant commentator's action.
            direct = bool(re.fullmatch(r"(?:(?:has|have|had|today|unexpectedly)\s*)*", between, re.I))
            direction = -1 if re.match(r"cut|lower", action['verb'], re.I) else 1
            add("policy_rate_action", clause, currency=currency,
                direction=direction if direct and not uncertain else None,
                status="reported_action" if direct and not uncertain else "conditional_or_negated",
                mechanism="rate_differential_channel_conditional_on_expectations")
        # Adjectives must immediately describe the bank, not another actor.
        stance = re.search(r"\b(hawkish|dovish)\s+$", clause[:bank.start()], re.I)
        after = re.match(r"\s+(?:is|turns?|turned|remains?)\s+(hawkish|dovish)\b", clause[bank.end():], re.I)
        stance = stance or after
        if stance:
            modal = bool(re.search(r"\b(?:not|no|never|may|might|could|if|less|unlikely)\b", clause, re.I))
            add("policy_stance", clause, currency=currency,
                direction=None if modal else (1 if stance[1].lower() == "hawkish" else -1),
                status="conditional_or_negated" if modal else "reported_stance",
                mechanism="rate_expectations")

    if re.search(r"\b(?:inflation|cpi|consumer prices)\b", text, re.I):
        add("inflation_context", text, status="surprise_unknown",
            mechanism="inflation_to_policy_requires_consensus_and_repricing")
    if re.search(r"\b(?:oil|energy|fuel)\b", text, re.I):
        add("energy_context", text, status="direction_unresolved",
            mechanism="inflation_growth_and_terms_of_trade_channels_can_conflict")

    # Preserve reported reaction and analyst view as different evidence types.
    pair = re.search(r"\b([A-Z]{3})[/_]([A-Z]{3})\b", text)
    if pair:
        tail = text[pair.end():]
        instrument = pair[1] + "_" + pair[2]
        up = re.search(r"\b(?:rebounds?|rallies|rises|rose|gains?|climbs?)\b", tail, re.I)
        down = re.search(r"\b(?:falls?|fell|drops?|declines?|slides?|slumps?)\b", tail, re.I)
        if up or down:
            modal = bool(re.search(UNCERTAIN, tail, re.I))
            # A section label "Price Forecast" does not make the embedded
            # observation "rebounds from 1.1460 low" a prediction. Retain that
            # explicit observation only; the target remains uninterpreted.
            observed_rebound = re.search(r"\brebounds?\s+from\s+\d+(?:\.\d+)?\s+low\b", tail, re.I)
            if observed_rebound and not re.search(UNCERTAIN.replace("|forecast", ""), tail, re.I):
                modal = False
            add("reported_pair_move", text, instrument=instrument,
                direction=None if modal or (up and down) else (1 if up else -1),
                status="conditional_or_ambiguous" if modal or (up and down) else "retrospective")
        if re.search(r"\b(?:long|short) opportunity\b", text, re.I):
            negated = bool(re.search(r"\b(?:not|no|never)\b", text, re.I))
            add("analyst_pair_view", text, instrument=instrument,
                direction=None if negated else (1 if re.search(r"\blong opportunity\b", text, re.I) else -1),
                status="opinion")
    # Explicit EUR response wording; do not infer it from a Fed mention alone.
    if re.search(r"\beuro\b.{0,35}\b(?:relief|on the ropes|under pressure)\b", text, re.I):
        ambiguous = bool(re.search(r"\b(?:not|no|never|may|might|could|if)\b", text, re.I))
        add("reported_currency_response", text, currency="EUR",
            direction=None if ambiguous else (1 if re.search(r"\brelief\b", text, re.I) else -1),
            status="conditional_or_negated" if ambiguous else "retrospective")

    return {"version": VERSION, "input_sha256": hashlib.sha256(text.encode()).hexdigest(),
            "input_scope": "headline_only", "claims": claims,
            "interpretation_status": "claims_extracted" if claims else "unresolved",
            "surprise": "unknown", "forecast_eligible": False,
            "causal_effect_established": False, "research_only": True}
