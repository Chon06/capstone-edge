#!/usr/bin/env bash
# Dùng: bash scripts/run_experiment.sh <số_lần> <đường_dẫn_model> [FPS]
RUNS=${1:-3}
MODEL=${2:-$HOME/capstone-edge/models/baseline.joblib}
FPS_SET=${3:-5}
KAFKA=$HOME/Downloads/kafka_2.13-3.7.0
ROOT=$HOME/capstone-edge
source "$ROOT/.venv/bin/activate"

for i in $(seq 1 "$RUNS"); do
  echo "=== Lần $i ==="
  "$KAFKA/bin/kafka-topics.sh" --delete --topic edge-frames --bootstrap-server localhost:9092 2>/dev/null
  sleep 10
  "$KAFKA/bin/kafka-topics.sh" --create --topic edge-frames --bootstrap-server localhost:9092
  (cd "$ROOT/edge" && MODEL_PATH="$MODEL" MAX_FRAMES=360 FPS="$FPS_SET" \
     DATA_DIR="$ROOT/data/neu-det/NEU-DET/validation/images" python3 producer.py)
  (cd "$ROOT" && EXPECTED=360 python3 consumer/check_drops.py) | tee "$ROOT/results/fps${FPS_SET}_run_${i}.txt"
done
