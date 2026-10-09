"""Property tests: random limits, random token sizes and random limit changes.
Whatever happens, no send may break the trailing-window rule."""

import random

from harness import saturate, violations
from hypothesis import event, given, settings
from hypothesis import strategies as st

from gateway.clock import FakeClock
from gateway.limiter import Limits, RateLimiter


@st.composite
def limits_for(draw, max_tokens: int) -> Limits:
    """Limits that always fit the largest request, with either RPM or TPM binding."""
    rpm = draw(st.integers(min_value=0, max_value=1_500))
    # ratio < 1: TPM is the binding limit; ratio > 1: RPM is.
    ratio = draw(st.floats(min_value=0.2, max_value=3.0))
    tpm = int(rpm * (max_tokens / 2) * ratio) + 2 * max_tokens
    return Limits(rpm=rpm, tpm=tpm)


@st.composite
def scenarios(draw):
    max_tokens = draw(st.integers(min_value=1, max_value=5_000))
    initial = draw(limits_for(max_tokens))
    changes = draw(
        st.lists(
            st.tuples(st.floats(min_value=0, max_value=240), limits_for(max_tokens)),
            max_size=5,
        )
    )
    return max_tokens, initial, changes


@settings(max_examples=80, deadline=None)
@given(
    scenario=scenarios(),
    utilization=st.floats(min_value=0.5, max_value=1.0),
    burst_s=st.floats(min_value=0.1, max_value=5.0),
    seed=st.integers(),
)
def test_no_send_ever_exceeds_the_trailing_window(scenario, utilization, burst_s, seed):
    max_tokens, initial, changes = scenario
    rng = random.Random(seed)
    sizes = [rng.randint(1, max_tokens) for _ in range(10_000)]
    clock = FakeClock()
    limiter = RateLimiter(initial, clock, utilization=utilization, burst_s=burst_s)

    sends = saturate(
        limiter,
        clock,
        duration_s=300,
        tokens_for=lambda i: sizes[i % len(sizes)],
        limit_changes=changes,
    )

    event(f"sends: {'0' if not sends else '<1k' if len(sends) < 1_000 else '1k+'}")
    assert violations(sends) == []
