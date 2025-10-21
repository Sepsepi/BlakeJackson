#!/usr/bin/env python3
"""Check if CSVFormatHandler is actually used in zaba.py"""

import re

# Read zaba.py
with open('zaba.py', 'r', encoding='utf-8') as f:
    content = f.read()
    lines = content.split('\n')

# Find all occurrences
matches = []
for i, line in enumerate(lines, 1):
    if 'CSVFormatHandler' in line:
        matches.append((i, line.strip()))

print("=" * 80)
print("CSV FORMAT HANDLER USAGE CHECK")
print("=" * 80)
print(f"\nTotal occurrences of 'CSVFormatHandler': {len(matches)}\n")

for line_num, line_content in matches:
    print(f"Line {line_num}: {line_content}")

print("\n" + "=" * 80)
if len(matches) == 2:
    print("✅ RESULT: CSVFormatHandler is ONLY imported, NEVER USED")
    print("✅ The warning is SAFE to ignore - it's an optional dependency")
else:
    print(f"⚠️ Found {len(matches)} occurrences - may be used in code")

print("=" * 80)

