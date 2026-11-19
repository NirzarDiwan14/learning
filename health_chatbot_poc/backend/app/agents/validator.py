"""Layer [7]: deterministic grounding check - every number in the answer must be traceable to
evidence (tool outputs, user context, the question itself). One LLM repair pass if not."""

from __future__ import annotations

import re

_DATE = re.compile(r"\b\d{4}-\d{2}(?:-\d{2})?\b|\b\d{1,2}:\d{2}\b")
_NUM = re.compile(
    r"(?<![\w.])[-+−]?\d{1,3}(?:,\d{3})+(?:\.\d+)?|(?<![\w.])[-+−]?\d+(?:\.\d+)?"
)

# Patterns whose numbers are generic coaching vocabulary, not claims about the user's data.
_IGNORE_CONTEXT = re.compile(
    r"(\d+\s*[x×]\s*\d+)|(\bzone\s*\d)|(\b\d+\s*[-–]\s*\d+\s*(?:min|minutes|s|sec|seconds|reps|%))",
    re.I,
)


def _numbers(text: str) -> list[float]:
    text = _DATE.sub(" ", text)
    out = []
    for m in _NUM.findall(text):
        try:
            out.append(float(m.replace(",", "").replace("−", "-")))
        except ValueError:
            pass
    return out


def check_grounding(answer: str, evidence: list[str]) -> dict:
    ev = set()
    for e in evidence:
        for n in _numbers(e):
            ev.add(n)
            ev.add(abs(n))
    cleaned = _IGNORE_CONTEXT.sub(" ", answer)
    claims = [
        n for n in _numbers(cleaned) if not (abs(n) <= 10 and float(n).is_integer())
    ]  # skip small counts
    unverified = []
    for n in claims:
        tol = max(0.6, abs(n) * 0.02)
        if not any(abs(abs(n) - e) <= tol for e in ev):
            unverified.append(n)
    total = len(claims)
    return {
        "numbers_checked": total,
        "unverified": sorted(set(unverified)),
        "grounded_ratio": round(1 - len(unverified) / total, 2) if total else 1.0,
    }


RED_FLAGS = re.compile(
    r"chest pain|chest tightness|faint(ed|ing)?|passed out|short(ness)? of breath|can'?t breathe|"
    r"palpitations|irregular heartbeat|suicid|self[- ]harm|severe headache|numbness|slurred speech",
    re.I,
)

SAFETY_MESSAGE = (
    "I'm not able to assess symptoms like this. If you have chest pain, trouble breathing, fainting, "
    "an irregular heartbeat or any severe or sudden symptom, **stop exercising and seek medical help now** "
    "(call your local emergency number if it's severe). Once you've been checked by a professional, "
    "I'm happy to help you plan a safe return to training using your data."
)
