#!/usr/bin/env python3
"""Check the documentation policy.

The repository publishes a defined set of directories under docs/ and keeps
everything else local. A published document that the policy ignores never
reaches the repository: `git add docs/` skips it without a word, and the
commit that claims to add it carries nothing.

This check fails when a file under docs/ is ignored and is not one of the
local-only paths the policy allows.
"""

import subprocess
import sys

ALLOWED_LOCAL_PREFIXES = (
    "docs/internal/",
)


def main() -> int:
    result = subprocess.run(
        ["git", "status", "--porcelain", "--ignored", "docs/"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        print("cannot read the documentation status:", result.stderr.strip())
        return 1

    offenders = []
    for line in result.stdout.splitlines():
        if not line.startswith("!! "):
            continue
        path = line[3:].strip().strip('"')
        if path.startswith(ALLOWED_LOCAL_PREFIXES):
            continue
        offenders.append(path)

    if offenders:
        print("a document is ignored by the policy and would never be committed:")
        for path in offenders:
            print(f"  {path}")
        print("publish it (add the directory to the policy) or keep it local (docs/internal/)")
        return 1

    print("the documentation policy holds: every document is either published or local")
    return 0


if __name__ == "__main__":
    sys.exit(main())
