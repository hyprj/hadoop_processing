#!/bin/bash
# Skrypt uruchamiający zadanie MapReduce (Hadoop Streaming)
# Użycie: ./run_mr.sh input_dir1 output_dir3

INPUT_DIR="$1"
OUTPUT_DIR="$2"
LOG_FILE="run_mr.log"

# Sprawdzenie czy podano parametry
if [ -z "$INPUT_DIR" ] || [ -z "$OUTPUT_DIR" ]; then
    echo "Użycie: $0 <input_dir1> <output_dir3>"
    exit 1
fi

# Ścieżki do mappera, reducera i combinera
MAPPER="./MapReduce/mapper.py"
REDUCER="./MapReduce/reducer.py"
COMBINER="./MapReduce/combiner.py"

# Upewniamy się, że pliki są wykonywalne
chmod +x "$MAPPER"
chmod +x "$REDUCER"
chmod +x "$COMBINER"

# Sprawdzenie czy istnieje Hadoop Streaming JAR
HADOOP_STREAMING_JAR=/Users/krzysztofzurkiewicz/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.0.jar
if [ ! -f $HADOOP_STREAMING_JAR ]; then
    echo "❌ Nie znaleziono Hadoop Streaming JAR w $HADOOP_STREAMING_JAR"
    exit 1
fi

# Usuwamy stare wyniki z HDFS, jeśli istnieją
hdfs dfs -rm -r -f "$OUTPUT_DIR" >> "$LOG_FILE" 2>&1

# Uruchomienie Hadoop Streaming z combinerem
# Wszystkie logi lecą jednocześnie do konsoli i do pliku logów
hadoop jar "$HADOOP_STREAMING_JAR" \
    -input "$INPUT_DIR" \
    -output "$OUTPUT_DIR" \
    -mapper "$MAPPER" \
    -combiner "$COMBINER" \
    -reducer "$REDUCER" \
    -file "$MAPPER" \
    -file "$COMBINER" \
    -file "$REDUCER" 2>&1 | tee "$LOG_FILE"

# Wyświetlenie krótkiego podglądu wyników w konsoli
echo ""
echo "✅ Zadanie zakończone. Wynik w katalogu HDFS: $OUTPUT_DIR"
echo "📄 Podgląd pierwszych 10 rekordów:"
hdfs dfs -cat "$OUTPUT_DIR/part-00000" | head -n 10

echo ""
echo "📝 Pełny log zapisany w: $LOG_FILE"
