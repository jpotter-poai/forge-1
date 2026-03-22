from __future__ import annotations

from backend.progress import ProgressBar, reset_progress_context, set_progress_context


def test_progress_bar_emits_known_total_events() -> None:
    events: list[dict] = []
    token = set_progress_context("node_x", events.append)
    try:
        values = list(
            ProgressBar(
                [10, 20, 30],
                label="Known total",
                throttle_seconds=0.0,
            )
        )
    finally:
        reset_progress_context(token)

    assert values == [10, 20, 30]
    assert events[0]["type"] == "node_progress"
    assert events[0]["node_id"] == "node_x"
    assert events[0]["current"] == 0
    assert events[0]["total"] == 3
    assert any(event.get("done") is True for event in events)
    assert events[-1]["current"] == 3
    assert events[-1]["total"] == 3
    assert events[-1]["percent"] == 1.0


def test_progress_bar_unknown_total_emits_without_percent() -> None:
    events: list[dict] = []
    token = set_progress_context("node_y", events.append)
    try:
        source = (i for i in range(4))
        values = list(
            ProgressBar(
                source,
                label="Unknown total",
                throttle_seconds=0.0,
            )
        )
    finally:
        reset_progress_context(token)

    assert values == [0, 1, 2, 3]
    assert events[0]["current"] == 0
    assert "total" not in events[0]
    assert "percent" not in events[0]
    assert events[-1]["done"] is True
    assert events[-1]["current"] == 4
    assert "total" not in events[-1]
