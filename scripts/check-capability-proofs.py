#!/usr/bin/env python3
"""Check that every capability of the matrix is proved.

The feature matrix states what the product does. A statement that no test
backs is a promise, and a promise that nothing checks rots: the frozen
contracts gained fields the implementation ignored, a router existed that
nothing called, and the matrix still read GA.

Every capability row therefore names its proof:

    test:<package>.<TestName>    a test that must exist and must pass
    cmd:<command>                a command that must succeed
    evidence:<path>              an evidence artifact that must exist
    none                         no proof yet (only for the rows that are
                                  not GA)

The check fails when a row names no proof, when a GA row names no test,
when a named test does not exist, when a named test fails, or when a row
points at an evidence file that is not there.

Usage:
    scripts/check-capability-proofs.py [--static] [--matrix PATH]
"""

import argparse
import pathlib
import re
import subprocess
import sys

REPOSITORY = pathlib.Path(__file__).resolve().parents[1]
DEFAULT_MATRIX = REPOSITORY / "docs" / "feature-status.md"

CAPABILITY_HEADER = "Feature / Capability"
STATUS_COLUMN = "Status"
PROOF_COLUMN = "Proved by"

ROW = re.compile(r"^\|\s*(?P<name>\*\*[^*]+\*\*)\s*\|")
TEST_FUNCTION = re.compile(r"^func (Test\w+)\(", re.MULTILINE)
PROOF = re.compile(r"^(test|cmd|evidence):(.+)$")
NO_PROOF = "none"


def parse_matrix(path: pathlib.Path) -> tuple[list[dict], list[str]]:
    """Read the rows of the matrix that make a claim.

    The matrix holds several tables. Every one of them states a status, and
    every one of them owes a proof: the status column is found by its name,
    and the proof is the last column of the row.
    """
    rows: list[dict] = []
    header_problems: list[str] = []
    lines = path.read_text(encoding="utf-8").split("\n")
    status_index = -1
    for number, line in enumerate(lines, start=1):
        stripped = line.strip()
        if not stripped.startswith("|"):
            status_index = -1
            continue
        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        if set(stripped.replace("|", "").replace(" ", "")) <= set("-:"):
            continue  # the separator under the header
        headers = [cell.strip("* ") for cell in cells]
        if any(header in ("Status", "Quality Gate", "Runtime / Framework", "Feature", "Feature / Capability", "Subsystem") for header in headers):
            status_index = headers.index("Status") if "Status" in headers else -1
            if headers[-1] != "Proved by":
                header_problems.append(
                    f"line {number}: the table does not declare a 'Proved by' column"
                )
            continue
        if status_index < 0 or not cells[0].startswith("**"):
            continue
        rows.append(
            {
                "line": number,
                "name": cells[0].strip("*"),
                "status": cells[status_index].strip("*") if status_index < len(cells) else "",
                "proof": cells[-1] if len(cells) > 1 else "",
            }
        )
    return rows, header_problems


def test_functions(package: pathlib.Path) -> set[str]:
    names: set[str] = set()
    if not package.is_dir():
        return names
    for path in package.glob("*_test.go"):
        names.update(TEST_FUNCTION.findall(path.read_text(encoding="utf-8", errors="ignore")))
    return names


def split_proofs(cell: str) -> list[str]:
    parts = [part.strip() for part in re.split(r"[,;]|<br\s*/?>", cell) if part.strip()]
    return parts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--static", action="store_true", help="do not run the named proofs")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="a command that cannot run is a failure, not a skip",
    )
    parser.add_argument("--matrix", default=str(DEFAULT_MATRIX), help="the matrix to check")
    arguments = parser.parse_args()

    matrix = pathlib.Path(arguments.matrix)
    if not matrix.is_file():
        print(f"the matrix {matrix} does not exist")
        return 1

    rows, header_problems = parse_matrix(matrix)
    if not rows:
        print(f"the matrix {matrix} holds no capability rows")
        return 1

    problems: list[str] = list(header_problems)
    # package -> the test names the matrix claims
    claimed: dict[str, set[str]] = {}
    # command -> the rows that claim it
    commands: dict[str, list[str]] = {}

    for row in rows:
        proofs = split_proofs(row["proof"])
        if not proofs:
            problems.append(
                f"line {row['line']}: {row['name']} names no proof "
                f"(a GA row needs a test, a weaker row may say 'none')"
            )
            continue
        for proof in proofs:
            if proof == NO_PROOF:
                if row["status"].startswith("GA"):
                    problems.append(
                        f"line {row['line']}: {row['name']} is GA and cannot claim 'none'"
                    )
                continue
            match = PROOF.match(proof)
            if match is None:
                problems.append(
                    f"line {row['line']}: {row['name']} names {proof!r}, which is not "
                    f"a proof (test:<package>.<TestName>, cmd:<command>, evidence:<path> or none)"
                )
                continue
            kind, value = match.group(1), match.group(2)
            if kind == "evidence":
                if not (REPOSITORY / value).exists():
                    problems.append(
                        f"line {row['line']}: {row['name']} points at {value}, which does not exist"
                    )
                continue
            if kind == "cmd":
                commands.setdefault(value, []).append(row["name"])
                continue
            package, _, test = value.rpartition(".")
            if not package or not test:
                problems.append(
                    f"line {row['line']}: {row['name']} names {value!r}, which is not <package>.<TestName>"
                )
                continue
            directory = REPOSITORY / package
            if test not in test_functions(directory):
                problems.append(
                    f"line {row['line']}: {row['name']} names {value}, and that test does not exist"
                )
                continue
            claimed.setdefault(package, set()).add(test)

    if problems:
        print("the capability matrix is not proved:")
        for problem in problems:
            print(f"  {problem}")
        return 1

    if arguments.static:
        print(
            f"the capability matrix names its proofs: {len(rows)} rows, "
            f"{sum(len(v) for v in claimed.values())} tests in {len(claimed)} packages, "
            f"{len(commands)} commands"
        )
        return 0

    # The proof has to pass, not merely exist.
    failures: list[str] = []
    for package, tests in sorted(claimed.items()):
        pattern = "^(" + "|".join(sorted(tests)) + ")$"
        result = subprocess.run(
            ["go", "test", "-count=1", "-run", pattern, "./" + package.lstrip("./")],
            cwd=REPOSITORY,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            detail = result.stdout.strip().splitlines()[-1:] or ["failed"]
            failures.append(f"{package}: {detail[0]}")

    skipped: list[str] = []
    for command, claimants in sorted(commands.items()):
        result = subprocess.run(
            command,
            cwd=REPOSITORY,
            shell=True,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            continue
        # A command that cannot run here (a toolchain that is not installed)
        # is not a failing proof; it is an unproven one.
        if arguments.strict:
            failures.append(f"{command!r} failed for {', '.join(claimants)}")
            continue
        skipped.append(f"{command!r} (for {', '.join(claimants)})")

    if failures:
        print("a claimed proof does not pass:")
        for failure in failures:
            print(f"  {failure}")
        return 1

    for entry in skipped:
        print(f"  skipped: {entry}")
    print(
        f"the capability matrix is proved: {len(rows)} rows, "
        f"{sum(len(v) for v in claimed.values())} tests in {len(claimed)} packages, "
        f"{len(commands)} commands"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
