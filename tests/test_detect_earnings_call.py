"""
Unit tests for detect_earnings_call.py
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from detect_earnings_call import (
    earnings_call_confidence,
    is_earnings_call,
    extract_transcript_section,
)

SAMPLE_TRANSCRIPT = """
Item 2.02  Results of Operations and Financial Condition.

Q3 2024 Earnings Call Transcript

Operator: Good morning, ladies and gentlemen, and welcome to the Acme Corp
third quarter 2024 earnings conference call.

Jane Smith – CEO: Thank you, Operator. Good morning, everyone.
We are pleased to report strong results for the quarter.

John Doe – CFO: Thanks Jane. Revenue was $500 million, up 12% year-over-year.

Operator: We will now open the line for analyst questions. Your first question
comes from Alice Johnson at Goldman Sachs. Please go ahead.

Alice Johnson – Goldman Sachs: Thanks for taking my question. Can you elaborate
on the margin expansion?

Jane Smith – CEO: Absolutely. We saw operating leverage across all segments.

Operator: This concludes today's earnings call. Thank you for participating.
"""

SAMPLE_PLAIN_8K = """
Item 1.01  Entry into a Material Definitive Agreement.

On November 1, 2024, Acme Corp entered into a Credit Agreement with
Bank of America, N.A., as administrative agent. The Credit Agreement
provides for a senior secured revolving credit facility of $200 million.
"""


def test_transcript_confidence_high():
    score = earnings_call_confidence(SAMPLE_TRANSCRIPT)
    assert score >= 0.80, f"Expected >= 0.80, got {score:.2f}"


def test_plain_8k_confidence_low():
    score = earnings_call_confidence(SAMPLE_PLAIN_8K)
    assert score <= 0.20, f"Expected <= 0.20, got {score:.2f}"


def test_is_earnings_call_true():
    assert is_earnings_call(SAMPLE_TRANSCRIPT) is True


def test_is_earnings_call_false():
    assert is_earnings_call(SAMPLE_PLAIN_8K) is False


def test_extract_transcript_section():
    section = extract_transcript_section(SAMPLE_TRANSCRIPT)
    assert section is not None
    assert "Operator" in section
    assert "CEO" in section


def test_extract_transcript_section_none():
    section = extract_transcript_section(SAMPLE_PLAIN_8K)
    assert section is None
