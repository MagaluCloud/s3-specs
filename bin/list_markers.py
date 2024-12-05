#!/usr/bin/env python3

import sys
import toml

def get_markers():
    """Reads pytest markers from pyproject.toml."""
    pyproject = toml.load("pyproject.toml")
    markers = pyproject.get("tool", {}).get("pytest", {}).get("ini_options", {}).get("markers", [])
    return sorted([marker.split(":")[0].strip() for marker in markers])

def get_legacy_markers():
    """Reads legacy s3-tester/shellspec markers from pyproject.toml."""
    pyproject = toml.load("pyproject.toml")
    markers = pyproject.get("tool", {}).get("s3-tester", {}).get("markers", [])
    return sorted([marker.split(":")[0].strip() for marker in markers])

if __name__ == "__main__":
    legacy_mode = "--legacy" in sys.argv
    markers = get_legacy_markers() if legacy_mode else get_markers()
    if markers:
        print("\n".join(markers))
    else:
        print("No markers found.")

