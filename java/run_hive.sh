#!/bin/bash
INPUT_DIR3="$1"
INPUT_DIR4="$2"
OUTPUT_DIR6="$3"
HIVE_SCRIPT="./Hive/hive.hql"

if [ -z "$INPUT_DIR3" ] || [ -z "$INPUT_DIR4" ] || [ -z "$OUTPUT_DIR6" ]; then
  echo "Usage: $0 <input_dir3> <input_dir4> <output_dir6>"
  exit 1
fi

# remove output if exists
hdfs dfs -test -d "$OUTPUT_DIR6"
if [ $? -eq 0 ]; then
  hdfs dfs -rm -r -skipTrash "$OUTPUT_DIR6"
fi

beeline -u "jdbc:hive2://localhost:10000/default" \
  -f "$HIVE_SCRIPT" \
  --hivevar input_dir3="$INPUT_DIR3" \
  --hivevar input_dir4="$INPUT_DIR4" \
  --hivevar output_dir6="$OUTPUT_DIR6"

echo "=== Preview JSON output (first 50 lines) ==="
hdfs dfs -cat "$OUTPUT_DIR6"/* | sed -n '1,50p'
