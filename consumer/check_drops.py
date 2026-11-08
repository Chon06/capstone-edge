import json
import sys
import os
from datetime import datetime  
from kafka import KafkaConsumer
from sklearn.metrics import classification_report

BOOTSTRAP = f"{os.getenv('KAFKA_HOST', 'localhost')}:{os.getenv('KAFKA_PORT', '9092')}"
TOPIC = "edge-frames"
EXPECTED = int(os.getenv("EXPECTED", "0"))
WARMUP = int(os.getenv("WARMUP","10"))

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",
    consumer_timeout_ms=5000,
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)

# Danh sách lưu trữ độ trễ và nhãn
seqs = []
latencies = []
inference_latencies = []
upload_latencies = []

# Danh sách lưu trữ sau khi bỏ warmup
valid_timestamps = []
latencies_warm = []
inference_warm = []
upload_warm = []

# Biến đếm trạng thái
ok_count = 0
ng_count = 0

# Danh sách nhãn cho classification_report
y_true = []
y_pred = []

for record in consumer:
    msg = record.value
    seq = msg["frame_seq"]
    seqs.append(seq)
    
    # Gom dữ liệu độ trễ
    latencies.append(msg.get("edge_latency_ms", 0))
    inference_latencies.append(msg.get("inference_ms", 0))
    upload_latencies.append(msg.get("upload_ms", 0))
    
    ts_str = msg.get("timestamp")
    if ts_str and seq > WARMUP:
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        valid_timestamps.append(ts)
        latencies_warm.append(msg.get("edge_latency_ms", 0))
        inference_warm.append(msg.get("inference_ms", 0))
        upload_warm.append(msg.get("upload_ms", 0))
        
    # Đếm số lượng OK/NG
    if msg.get("edge_status") == "NG":
        ng_count += 1
    else: 
        ok_count += 1
        
    # Gom nhãn thực tế và dự đoán để chấm điểm F1
    if "true_label" in msg and "defect_label" in msg:
        y_true.append(msg["true_label"])
        y_pred.append(msg["defect_label"])

if not seqs:
    print("Không nhận được dữ liệu nào từ Kafka.")
    sys.exit(0)

total_received = len(seqs)
actual_seqs = set(seqs)
unique_received = len(actual_seqs)
duplicates = total_received - unique_received
min_seq, max_seq = min(seqs), max(seqs)

if EXPECTED > 0:
    expected_seqs = set(range(1, EXPECTED + 1))
else:
    expected_seqs = set(range(min_seq, max_seq + 1))
missing_seqs = sorted(expected_seqs - actual_seqs)

# Tinh toan FPS thuc te
actual_fps = 0
if len(valid_timestamps) > 1:
    time_diff = (max(valid_timestamps) - min(valid_timestamps)).total_seconds()
    if time_diff > 0:
        actual_fps = (len(valid_timestamps) - 1) / time_diff

def percentile(values, p):
    if not values: return 0
    ordered = sorted(values)
    k = max(0, min(len(ordered) - 1, round(p / 100 * len(ordered)) - 1))
    return ordered[k]


def print_latency_stats(name, values_all, values_warm):
    if not values_all: return

    m_all = sum(values_all) / len(values_all)
    p95_all = percentile(values_all, 95)
    p99_all = percentile(values_all, 99)
    max_all = max(values_all)
    
    print(f"{name}:")
    if values_warm:
        m_warm = sum(values_warm) / len(values_warm)
        p95_warm = percentile(values_warm, 95)
        p99_warm = percentile(values_warm, 99)
        print(f"  - Tất cả: TB {m_all:.2f} | P95 {p95_all:.2f} | P99 {p99_all:.2f} | Max {max_all:.2f}")
        print(f"  - Ổn định (sau {WARMUP} khung đầu): TB {m_warm:.2f} | P95 {p95_warm:.2f} | P99 {p99_warm:.2f}")
    else:
        print(f"  - Tất cả: TB {m_all:.2f} | P95 {p95_all:.2f} | P99 {p99_all:.2f} | Max {max_all:.2f}")

print(f"\n             BÁO CÁO TOÀN VẸN DỮ LIỆU         ")
print(f"Nhận: {total_received} tin ({unique_received} khung), frame_seq từ {min_seq} đến {max_seq}")
print(f"Trùng lặp: {duplicates}, Thiếu: {len(missing_seqs)} khung")

print(f"\n--- PHÂN TÍCH ĐỘ TRỄ & HIỆU SUẤT ---")
if actual_fps > 0:
    print(f"FPS thực tế tại Consumer: {actual_fps:.2f} khung/giây (đã bỏ {WARMUP} khung warmup)")
else:
    print("Không đủ dữ liệu timestamp để tính FPS thực tế.")

print("")
print_latency_stats("1. edge_latency_ms (Tổng độ trễ Edge)", latencies, latencies_warm)
print_latency_stats("2. inference_ms (Tốc độ chạy AI Random Forest)", inference_latencies, inference_warm)
print_latency_stats("3. upload_ms (Tốc độ đẩy ảnh lên MinIO)", upload_latencies, upload_warm)

print(f"\n--- ĐÁNH GIÁ CHẤT LƯỢNG AI ---")
if y_true and y_pred:
    print(classification_report(y_true, y_pred))
else:
    print("⚠️ Thiếu trường 'true_label' để tính F1-Score.")
