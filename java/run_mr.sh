#!/bin/bash
INPUT_DIR="$1"
OUTPUT_DIR="$2"
JAR_PATH="./mr/target/hospitals-mr-1.0-jar-with-dependencies.jar"

if [ -z "$INPUT_DIR" ] || [ -z "$OUTPUT_DIR" ]; then
  echo "Proper usage: $0 <input_dir1> <output_dir3>"
  exit 1
fi

if [ ! -f "$JAR_PATH" ]; then
  echo "Jar not found. Build with: cd mr && mvn clean package"
  exit 2
fi

echo "Removing HDFS output (if exists): $OUTPUT_DIR"
hdfs dfs -test -d "$OUTPUT_DIR"
if [ $? -eq 0 ]; then
  hdfs dfs -rm -r -skipTrash "$OUTPUT_DIR"
fi

echo "Running MapReduce (JAR)"
hadoop jar "$JAR_PATH" org.example.VisitsDriver "$INPUT_DIR" "$OUTPUT_DIR"

hdfs dfs -test -d "$OUTPUT_DIR"
if [ $? -eq 0 ]; then
  hdfs dfs -cat "$OUTPUT_DIR/part-00000" | sed -n '1,50p'
else
  echo "Output dir not found in HDFS: $OUTPUT_DIR"
fi
