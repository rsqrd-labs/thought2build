"""Small fence-aware ATX heading scanner shared by context and validation."""

from __future__ import annotations

import re


def headings(text: str) -> list[tuple[str, int, int]]:
    found: list[tuple[str, int, int]] = []
    fence: str | None = None
    in_comment = False
    offset = 0
    for line in text.splitlines(keepends=True):
        original = line
        # HTML comments cannot satisfy required document headings.
        if in_comment:
            if "-->" in line:
                in_comment = False
            offset += len(original)
            continue
        if fence:
            if re.fullmatch(
                r" {0,3}" + re.escape(fence[0]) + "{" + str(len(fence)) + r",}\s*", line
            ):
                fence = None
        elif "<!--" in line:
            in_comment = "-->" not in line.split("<!--", 1)[1]
        else:
            candidate = line.lstrip(" ")
            if not candidate.startswith(("#", "`", "~")):
                offset += len(original)
                continue
            opened = re.match(r"^ {0,3}(`{3,}|~{3,})", line)
            if opened:
                fence = opened.group(1)
            else:
                match = re.match(r"^ {0,3}(#{1,6})[ \t]+(.+?)\s*$", line)
                if match:
                    body = re.sub(r"[ \t]+#+[ \t]*$", "", match.group(2))
                    found.append(
                        (f"{match.group(1)} {body}", offset, offset + len(original))
                    )
        offset += len(original)
    return found


def matches_heading(actual: str, required: str) -> bool:
    """Allow the prompt contract's optional trailing parenthetical only."""
    return actual == required or bool(
        re.fullmatch(re.escape(required) + r"[ \t]+\([^()\n]*\)", actual)
    )
