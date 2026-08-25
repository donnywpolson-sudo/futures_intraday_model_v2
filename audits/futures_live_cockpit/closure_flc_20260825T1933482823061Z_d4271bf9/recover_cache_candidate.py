#!/usr/bin/env python3
"""Try a bounded exact-byte reconstruction of the pre-launch empty binding pages."""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path


PAGE_SIZE = 4096
TABLE_ROOT_PAGE = 8
INDEX_ROOT_PAGE = 9


def empty_leaf_page(kind: int) -> bytes:
    page = bytearray(PAGE_SIZE)
    page[0] = kind
    page[5:7] = PAGE_SIZE.to_bytes(2, "big")
    return bytes(page)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("cache", type=Path)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    original = args.cache.read_bytes()
    expected = args.expected_sha256.lower()
    if len(original) % PAGE_SIZE:
        raise SystemExit("cache size is not page aligned")

    table_offset = (TABLE_ROOT_PAGE - 1) * PAGE_SIZE
    index_offset = (INDEX_ROOT_PAGE - 1) * PAGE_SIZE
    current_counter = int.from_bytes(original[24:28], "big")
    current_valid_for = int.from_bytes(original[92:96], "big")
    print(
        f"current_counter={current_counter} current_valid_for={current_valid_for} "
        f"table_cells={int.from_bytes(original[table_offset + 3:table_offset + 5], 'big')} "
        f"index_cells={int.from_bytes(original[index_offset + 3:index_offset + 5], 'big')}"
    )

    page_variants = (
        (empty_leaf_page(0x0D), empty_leaf_page(0x0A), "zeroed-empty-pages"),
    )
    for table_page, index_page, label in page_variants:
        base = bytearray(original)
        base[table_offset : table_offset + PAGE_SIZE] = table_page
        base[index_offset : index_offset + PAGE_SIZE] = index_page
        for decrement in range(0, 129):
            candidate = bytearray(base)
            candidate[24:28] = ((current_counter - decrement) & 0xFFFFFFFF).to_bytes(4, "big")
            candidate[92:96] = ((current_valid_for - decrement) & 0xFFFFFFFF).to_bytes(4, "big")
            digest = hashlib.sha256(candidate).hexdigest()
            if digest != expected:
                continue
            print(f"MATCH variant={label} counter_decrement={decrement} sha256={digest}")
            if args.output is not None:
                args.output.parent.mkdir(parents=True, exist_ok=True)
                args.output.write_bytes(candidate)
                print(f"wrote={args.output.resolve()}")
            return
    print("NO_MATCH")
    raise SystemExit(1)


if __name__ == "__main__":
    main()
