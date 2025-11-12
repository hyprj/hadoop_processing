#!/usr/bin/env python3
import sys

current_key = None
count = 0
age_sum = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        hospital_id, year, age = line.split("\t")
        age = int(age)
    except ValueError:
        continue

    key = f"{hospital_id}\t{year}"

    # Jeżeli zmienił się klucz – wypisz wynik dla poprzedniej grupy
    if current_key and key != current_key:
        avg_age = age_sum / count if count else 0
        print(f"{current_key}\t{count}\t{avg_age:.2f}")
        count = 0
        age_sum = 0

    current_key = key
    count += 1
    age_sum += age

# Wypisanie ostatniej grupy
if current_key:
    avg_age = age_sum / count if count else 0
    print(f"{current_key}\t{count}\t{avg_age:.2f}")
