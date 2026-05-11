"""
detect_earnings_call.py
-----------------------
Utility to identify 8-K filings that contain an earnings call / earnings
release transcript.

Background
----------
8-K Item 2.02 ("Results of Operations and Financial Condition") is the
standard item used when a company discloses quarterly or annual results.
However, the actual earnings call transcript (if included) is typically
attached as an Exhibit 99.1 and may span dozens of pages — making 8-K
files much larger than the usual short event notices.

This module provides:
  - ``is_earnings_call(text)``       — quick boolean check
  - ``earnings_call_confidence(text)`` — confidence score (0.0–1.0)
  - ``extract_transcript_section(text)`` — attempt to extract the
                                           transcript body from the text

Usage
-----
    from detect_earnings_call import is_earnings_call, earnings_call_confidence

    with open("my_8k.txt") as f:
        text = f.read()

    if is_earnings_call(text):
        print("This 8-K likely contains an earnings call transcript.")
    else:
        conf = earnings_call_confidence(text)
        print(f"Confidence: {conf:.0%}")
"""

import re
from typing import Optional

# ---------------------------------------------------------------------------
# Regex patterns that are strongly indicative of an earnings call transcript
# ---------------------------------------------------------------------------

# Typical transcript preamble phrases
_TRANSCRIPT_PREAMBLE = re.compile(
    r"(earnings\s+(call|conference|results)\s+(transcript|q&a|discussion)"
    r"|quarterly\s+earnings\s+call"
    r"|conference\s+call\s+transcript"
    r"|prepared\s+remarks"
    r"|operator(\s+instructions)?:"
    r"|good\s+(morning|afternoon|evening|day).{0,60}(ladies\s+and\s+gentlemen|everyone|participants)"
    r"|this\s+is\s+your\s+operator"
    r"|\bq[1-4]\s+20\d{2}\s+earnings\b)",
    re.IGNORECASE | re.DOTALL,
)

# Speaker-attribution lines (e.g. "John Smith – CEO:" or "Operator:")
_SPEAKER_LINE = re.compile(
    r"^([A-Z][A-Za-z\s\-\.]{2,50}(–|-)\s*[A-Za-z\s,\.]+:)",
    re.MULTILINE,
)

# Analyst / Q&A section markers
_QA_MARKERS = re.compile(
    r"(question.{0,20}answer\s+session"
    r"|\bq\s*&\s*a\b"
    r"|analyst\s+questions?"
    r"|your\s+(first|next|following)\s+question\s+comes?\s+from"
    r"|please\s+(go\s+ahead|state\s+your\s+question)"
    r"|the\s+(floor|line)\s+is\s+(now\s+)?open)",
    re.IGNORECASE,
)

# Item 2.02 mention (results of operations) — necessary but not sufficient
_ITEM_202 = re.compile(r"item\s+2\.02", re.IGNORECASE)


def earnings_call_confidence(text: str) -> float:
    """Return a confidence score (0.0–1.0) that *text* contains an earnings
    call transcript.

    Scoring heuristic
    ~~~~~~~~~~~~~~~~~
    - Presence of Item 2.02                     → +0.15
    - Transcript preamble match                 → +0.40
    - 3+ speaker attribution lines              → +0.30
    - Q&A section marker                        → +0.15
    - Document length > 10 000 chars            → +0.05  (transcripts are long)

    The raw sum is clamped to [0.0, 1.0].
    """
    score = 0.0

    if _ITEM_202.search(text):
        score += 0.15

    if _TRANSCRIPT_PREAMBLE.search(text):
        score += 0.40

    speaker_hits = len(_SPEAKER_LINE.findall(text))
    if speaker_hits >= 3:
        score += 0.30
    elif speaker_hits >= 1:
        score += 0.15

    if _QA_MARKERS.search(text):
        score += 0.15

    if len(text) > 10_000:
        score += 0.05

    return min(score, 1.0)


def is_earnings_call(text: str, threshold: float = 0.50) -> bool:
    """Return *True* if *text* is likely an earnings call transcript.

    Parameters
    ----------
    text:
        The plain-text content of an 8-K filing (after HTML stripping).
    threshold:
        Minimum confidence score to classify as earnings call.
        Default is 0.50.
    """
    return earnings_call_confidence(text) >= threshold


def extract_transcript_section(text: str) -> Optional[str]:
    """Attempt to extract the body of the earnings call transcript from *text*.

    The function looks for the first occurrence of a preamble phrase and
    returns everything from that point onwards (up to an "End of call" / 
    "safe harbor" / signature footer if detectable).

    Returns ``None`` if no transcript section is found.
    """
    match = _TRANSCRIPT_PREAMBLE.search(text)
    if not match:
        return None

    start = match.start()
    end_text = text[start:]

    # Try to find a natural end marker
    end_pattern = re.compile(
        r"(end\s+of\s+(call|conference|transcript)"
        r"|\*\s*\*\s*\*"
        r"|this\s+concludes\s+(today'?s?|the)\s+(call|conference|earnings)"
        r"|thank\s+you\s+for\s+(joining|participating|attending)"
        r"|forward[- ]looking\s+statements?\s+disclaimer)",
        re.IGNORECASE,
    )
    end_match = end_pattern.search(end_text)
    if end_match:
        return end_text[: end_match.end()].strip()

    return end_text.strip()
