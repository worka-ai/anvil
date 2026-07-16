#!/usr/bin/env python3
import argparse
import json
import sys
import urllib.error
import urllib.request


def main() -> int:
    parser = argparse.ArgumentParser(description="Check whether a crates.io crate version exists")
    parser.add_argument("crate")
    parser.add_argument("version")
    args = parser.parse_args()

    url = f"https://crates.io/api/v1/crates/{args.crate}/{args.version}"
    request = urllib.request.Request(url, headers={"User-Agent": "anvil-release-ci"})
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = json.load(response)
    except urllib.error.HTTPError as err:
        if err.code == 404:
            return 1
        raise

    return 0 if payload.get("version", {}).get("num") == args.version else 1


if __name__ == "__main__":
    raise SystemExit(main())
