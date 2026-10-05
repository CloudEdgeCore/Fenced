"""Backward-compatibility shims for the AgentOS -> Fenced rename.

This mirrors ``internal/platform/compat`` in the Go tree. A deployment written
before the rename keeps working if the process calls :func:`apply_legacy_compat`
once at start-up, so every ``AGENTOS_*`` environment variable becomes visible
under its ``FENCED_*`` name.

Compatibility covers only the inputs this process reads. gRPC service names and
protobuf type URLs changed with the protobuf package and are NOT aliased here; a
client built before the rename cannot talk to a server built after it. See
``docs/COMPATIBILITY.md`` for the full boundary.
"""

from __future__ import annotations

import os
import sys
from typing import Iterable, MutableMapping, TextIO

LEGACY_PREFIX = "AGENTOS_"
CURRENT_PREFIX = "FENCED_"

LEGACY_EXECUTION_HEADER = "X-Agentos-Execution"
CURRENT_EXECUTION_HEADER = "X-Fenced-Execution"

_LEGACY_PROTOCOL_PREFIX = "agentos."
_CURRENT_PROTOCOL_PREFIX = "fenced."


def alias_legacy_env(environ: MutableMapping[str, str] | None = None) -> list[str]:
    """Copy every ``AGENTOS_*`` variable to ``FENCED_*`` when that name is unset.

    A value already present under ``FENCED_*`` always wins, so an operator can
    migrate one variable at a time; a variable explicitly set to the empty
    string also wins and is not overwritten. The function is idempotent and
    returns the sorted names of the variables it aliased.
    """
    env = os.environ if environ is None else environ
    aliased: list[str] = []
    for name in list(env):
        if not name.startswith(LEGACY_PREFIX):
            continue
        current = CURRENT_PREFIX + name[len(LEGACY_PREFIX) :]
        if current in env:
            continue
        env[current] = env[name]
        aliased.append(current)
    aliased.sort()
    return aliased


def warn_legacy_env(stream: TextIO | None, aliased: Iterable[str]) -> None:
    """Write one deprecation notice to *stream* when *aliased* is non-empty."""
    names = list(aliased)
    if not names or stream is None:
        return
    print(
        f"fenced: deprecated {LEGACY_PREFIX}* environment variables detected; "
        f"aliased {len(names)} variable(s) to {CURRENT_PREFIX}* "
        f"({', '.join(names)}); rename them before the next major release",
        file=stream,
    )


def apply_legacy_compat(stream: TextIO | None = None) -> list[str]:
    """Alias legacy environment variables and warn; for process entry points.

    Typical use in an adapter ``__main__``::

        from fenced_runtime import apply_legacy_compat

        if __name__ == "__main__":
            apply_legacy_compat()
            ...
    """
    aliased = alias_legacy_env()
    warn_legacy_env(sys.stderr if stream is None else stream, aliased)
    return aliased


def normalize_protocol(identifier: str) -> str:
    """Map a legacy ``agentos.*`` protocol identifier to its ``fenced.*`` form.

    Already-canonical values, empty strings, and values that do not contain a
    protocol prefix are returned unchanged.
    """
    if not identifier or _LEGACY_PROTOCOL_PREFIX not in identifier:
        return identifier
    return identifier.replace(_LEGACY_PROTOCOL_PREFIX, _CURRENT_PROTOCOL_PREFIX)


def normalize_header(name: str) -> str:
    """Map a legacy AgentOS HTTP header name to its Fenced equivalent.

    The comparison is case-insensitive; any other name is returned unchanged.
    It exists so a receiver can accept requests from a worker built before the
    rename.
    """
    if name.lower() == LEGACY_EXECUTION_HEADER.lower():
        return CURRENT_EXECUTION_HEADER
    return name
