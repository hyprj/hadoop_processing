#!/usr/bin/env python3
import sys
import csv
from io import StringIO

# Pomijamy nagłówek
next(sys.stdin)

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    f = list(csv.reader(StringIO(line)))[0]
    try:
        hospital_id = f[1]
        date = f[5]
        age = int(f[6])
        year = date[:4]
        print(f"{hospital_id}\t{year}\t{age}")
    except Exception:
        continue
