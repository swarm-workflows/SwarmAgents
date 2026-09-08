"""YAML loading that refuses to silently discard a duplicate key.

`yaml.safe_load` implements the YAML spec's "last one wins" rule for duplicate mapping keys,
without a warning. That is how `config_swarm_multi.yml` came to define
`runtime.peer_expiry_seconds` twice — 300 near the top of the block and 45 eight lines later —
so the effective value was 45 while the file, the comment beside it and CLAUDE.md all said 300.
Nothing in a run reports the value it is using, so reading the file top-down gave the wrong
answer for the whole campaign.

Use `safe_load` from this module wherever a config file is read. A duplicate key raises
`DuplicateKeyError`, naming the key and both line numbers.
"""
from __future__ import annotations

from typing import IO, Any

import yaml


class DuplicateKeyError(yaml.YAMLError):
    """A mapping in the document defines the same key twice."""


class _StrictLoader(yaml.SafeLoader):
    """SafeLoader that rejects duplicate keys instead of keeping the last one."""


def _construct_mapping(loader: _StrictLoader, node: yaml.MappingNode, deep: bool = False) -> dict:
    mapping: dict = {}
    seen: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in seen:
            first = seen[key].start_mark
            second = key_node.start_mark
            raise DuplicateKeyError(
                f"duplicate key {key!r} in mapping: first defined at "
                f"line {first.line + 1}, redefined at line {second.line + 1} "
                f"({second.name}). YAML would silently keep the second value — "
                f"delete one."
            )
        seen[key] = key_node
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_StrictLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_mapping
)


def safe_load(stream: str | bytes | IO) -> Any:
    """`yaml.safe_load`, but a duplicate mapping key raises `DuplicateKeyError`."""
    return yaml.load(stream, Loader=_StrictLoader)
