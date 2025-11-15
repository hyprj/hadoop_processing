#!/bin/bash
# Skrypt uruchamiający Hive z parametrami
# Użycie: ./run_hive.sh input_dir3 input_dir4 output_dir6

INPUT_DIR3=$1
INPUT_DIR4=$2
OUTPUT_DIR6=$3

if [ -z "$INPUT_DIR3" ] || [ -z "$INPUT_DIR4" ] || [ -z "$OUTPUT_DIR6" ]; then
    echo "Użycie: $0 <input_dir3> <input_dir4> <output_dir6>"
    exit 1
fi

# Ścieżka do skryptu Hive
HIVE_SCRIPT="hive.hql"

# Usuwamy poprzedni wynik (jeśli istnieje)
hdfs dfs -rm -r -f $OUTPUT_DIR6

# Uruchamiamy Hive z przekazaniem parametrów
hive \
  -hiveconf input_dir3=$INPUT_DIR3 \
  -hiveconf input_dir4=$INPUT_DIR4 \
  -hiveconf output_dir6=$OUTPUT_DIR6 \
  -f $HIVE_SCRIPT

# Podgląd wyniku
echo ""
echo "✅ Zadanie Hive zakończone. Wynik w: $OUTPUT_DIR6"
echo "📄 Podgląd wyniku:"
hdfs dfs -cat $OUTPUT_DIR6/* | head