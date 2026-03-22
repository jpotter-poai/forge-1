from __future__ import annotations

from contextvars import ContextVar, Token
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Callable, Iterable, Iterator, TypeVar

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class ProgressContext:
    node_id: str
    emit: Callable[[dict[str, Any]], None]


_progress_context: ContextVar[ProgressContext | None] = ContextVar(
    "forge_progress_context",
    default=None,
)


def set_progress_context(
    node_id: str,
    emit: Callable[[dict[str, Any]], None],
) -> Token[ProgressContext | None]:
    return _progress_context.set(ProgressContext(node_id=node_id, emit=emit))


def reset_progress_context(token: Token[ProgressContext | None]) -> None:
    _progress_context.reset(token)


def _emit_progress(
    *,
    current: int,
    total: int | None,
    label: str | None,
    done: bool,
) -> None:
    context = _progress_context.get()
    if context is None:
        return

    payload: dict[str, Any] = {
        "type": "node_progress",
        "node_id": context.node_id,
        "current": int(current),
        "done": bool(done),
    }
    if total is not None:
        payload["total"] = int(total)
        if total > 0:
            payload["percent"] = max(0.0, min(1.0, float(current) / float(total)))
    if label:
        payload["label"] = label
    context.emit(payload)


class ProgressBar(Iterable[T]):
    """
    Lightweight tqdm-like iterable wrapper that emits node progress events when
    execution is running under PipelineRunner.
    """

    def __init__(
        self,
        iterable: Iterable[T],
        *,
        total: int | None = None,
        label: str | None = None,
        throttle_seconds: float = 0.2,
        min_delta: int = 1,
    ) -> None:
        self._iterable = iterable
        self._total = total if total is not None else self._infer_total(iterable)
        self._label = label
        self._throttle_seconds = max(float(throttle_seconds), 0.0)
        self._min_delta = max(int(min_delta), 1)

    def __iter__(self) -> Iterator[T]:
        count = 0
        last_emit_count = -self._min_delta
        last_emit_at = perf_counter() - self._throttle_seconds

        _emit_progress(
            current=0,
            total=self._total,
            label=self._label,
            done=False,
        )

        for item in self._iterable:
            count += 1
            now = perf_counter()
            count_delta = count - last_emit_count
            time_delta = now - last_emit_at
            if count_delta >= self._min_delta and time_delta >= self._throttle_seconds:
                _emit_progress(
                    current=count,
                    total=self._total,
                    label=self._label,
                    done=False,
                )
                last_emit_count = count
                last_emit_at = now
            yield item

        _emit_progress(
            current=count,
            total=self._total,
            label=self._label,
            done=True,
        )

    @staticmethod
    def _infer_total(iterable: Iterable[T]) -> int | None:
        if hasattr(iterable, "__len__"):
            try:
                value = int(len(iterable))  # type: ignore[arg-type]
                if value >= 0:
                    return value
            except Exception:
                return None
        return None


def progress_iter(
    iterable: Iterable[T],
    *,
    total: int | None = None,
    label: str | None = None,
    throttle_seconds: float = 0.2,
    min_delta: int = 1,
) -> ProgressBar[T]:
    return ProgressBar(
        iterable,
        total=total,
        label=label,
        throttle_seconds=throttle_seconds,
        min_delta=min_delta,
    )
