from __future__ import annotations

from copy import deepcopy
import inspect
from typing import Any, get_args, get_origin

from backend.block import BlockParams
from backend.registry import BlockRegistry


def _annotation_allows_none(annotation: Any) -> bool:
    if annotation is Any or annotation is None or annotation is type(None):
        return True

    origin = get_origin(annotation)
    if origin is None:
        return False

    return any(_annotation_allows_none(arg) for arg in get_args(annotation))


def _sample_value(annotation: Any) -> Any:
    if annotation is Any:
        return "sample"

    args = [arg for arg in get_args(annotation) if arg is not type(None)]
    if args:
        return _sample_value(args[0])

    origin = get_origin(annotation)
    if origin is list:
        inner = get_args(annotation)[0] if get_args(annotation) else Any
        return [_sample_value(inner)]
    if origin is tuple:
        inner_args = get_args(annotation)
        if not inner_args:
            return ()
        if len(inner_args) == 2 and inner_args[1] is Ellipsis:
            return (_sample_value(inner_args[0]),)
        return tuple(_sample_value(arg) for arg in inner_args)
    if origin is dict:
        return {}
    if origin is set:
        return set()

    if annotation is bool:
        return True
    if annotation is int:
        return 1
    if annotation is float:
        return 1.0
    if annotation is str:
        return "sample"

    return "sample"


def _base_payload(params_cls: type[BlockParams]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, field in params_cls.model_fields.items():
        if field.is_required():
            payload[key] = _sample_value(field.annotation)
            continue
        payload[key] = deepcopy(field.get_default(call_default_factory=True))
    return payload


def test_defaulted_non_nullable_block_params_accept_explicit_null_repo_wide() -> None:
    registry = BlockRegistry(blocks_dir="blocks", package_name="blocks")
    registry.discover(force_reload=True)

    for block_cls in registry._blocks.values():
        params_cls = getattr(block_cls, "Params", None)
        if not inspect.isclass(params_cls) or not issubclass(params_cls, BlockParams):
            continue

        base_payload = _base_payload(params_cls)
        for key, field in params_cls.model_fields.items():
            if field.is_required():
                continue
            if _annotation_allows_none(field.annotation):
                continue

            payload = deepcopy(base_payload)
            payload[key] = None
            validated = params_cls.model_validate(payload)
            expected = deepcopy(field.get_default(call_default_factory=True))

            assert getattr(validated, key) == expected, (
                f"{block_cls.__name__}.{key} should fall back to its default when"
                " the incoming payload provides null."
            )
