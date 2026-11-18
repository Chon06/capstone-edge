"""Tầng Edge: nhận ảnh từ camera giả lập -> phát hiện lỗi -> nén JPEG -> gửi lên Kafka.

    python -m edge.edge_producer --images data/stream_images --detector classic --frames 200 --fps 25

edge_latency_ms được đo từ lúc nhận khung hình đến khi có kết quả phát hiện + ảnh JPEG
(chưa tính base64/JSON, thường < 1 ms với ảnh nhỏ).
"""
import argparse
import time

from common.message import build_message, now_ms, serialize, validate_message
from common.stats import summarize
from config import KAFKA_BOOTSTRAP, MAX_MESSAGE_BYTES, TOPIC
from common.imaging import encode_jpeg
from edge.camera_simulator import CameraSimulator
from edge.detectors import make_detector


def run_producer(*, bootstrap, topic, images_dir, detector, fps=25.0, num_frames=1000, cameras=1,
                 jpeg_quality=80, linger_ms=5, on_full="block", validate=False,
                 producer_buffer_kb=65536, log_every=0) -> dict:
    """Chạy producer, trả về dict thống kê.

    on_full: khi hàng đợi nội bộ của producer đầy -> "block" (chờ, không mất ảnh)
             hoặc "drop" (bỏ ảnh, tính vào số khung rớt - mô phỏng camera không chờ được).
    """
    from confluent_kafka import Producer

    sim = CameraSimulator(images_dir, fps=fps, num_frames=num_frames, cameras=cameras)
    prod = Producer({
        "bootstrap.servers": bootstrap,
        "message.max.bytes": MAX_MESSAGE_BYTES,
        "linger.ms": linger_ms,
        "acks": 1,
        "queue.buffering.max.kbytes": producer_buffer_kb,
    })
    delivered, failed = [0], [0]

    def on_delivery(err, _msg):
        if err is not None:
            failed[0] += 1
        else:
            delivered[0] += 1

    edge_lat, sizes = [], []
    enqueued = dropped = ng = 0
    t_begin = time.perf_counter()
    for frame in sim:
        t0 = time.perf_counter()
        dets = detector.detect(frame.image)
        jpg = encode_jpeg(frame.image, jpeg_quality)
        ts_done = now_ms()
        lat_ms = (time.perf_counter() - t0) * 1000
        msg = build_message(image_id=frame.image_id, camera_id=frame.camera_id, frame_seq=frame.frame_seq,
                            ts_capture_ms=frame.ts_capture_ms, ts_edge_done_ms=ts_done,
                            edge_latency_ms=lat_ms, detections=dets, jpeg_bytes=jpg,
                            jpeg_quality=jpeg_quality)
        if validate:
            validate_message(msg)
        payload = serialize(msg)
        while True:
            try:
                prod.produce(topic, key=frame.camera_id, value=payload, on_delivery=on_delivery)
                enqueued += 1
                break
            except BufferError:  # hàng đợi nội bộ đầy
                if on_full == "drop":
                    dropped += 1
                    break
                prod.poll(0.05)  # "block": đợi gửi bớt rồi thử lại
        prod.poll(0)
        edge_lat.append(lat_ms)
        sizes.append(len(payload))
        ng += bool(dets)
        if log_every and (frame.frame_seq * sim.cameras) % log_every == 0:
            print(f"  ... đã xử lý {enqueued + dropped}/{num_frames} khung")
    loop_s = time.perf_counter() - t_begin
    prod.flush(60)

    stats = {
        "frames_target": num_frames, "enqueued": enqueued, "delivered": delivered[0],
        "failed": failed[0], "dropped_edge": dropped, "ng_frames": ng,
        "duration_s": loop_s, "producer_fps": enqueued / loop_s if loop_s else 0.0,
        "target_fps": fps, "late_frames": sim.late_frames, "max_lag_ms": sim.max_lag_ms,
        "payload_bytes_total": int(sum(sizes)), "payload_bytes_mean": float(sum(sizes) / max(1, len(sizes))),
        "edge_to_queue_mbps": (sum(sizes) * 8 / 1e6) / loop_s if loop_s else 0.0,
    }
    stats.update(summarize(edge_lat, "edge_lat_"))
    return stats


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--images", default="data/stream_images")
    ap.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    ap.add_argument("--topic", default=TOPIC)
    ap.add_argument("--fps", type=float, default=25.0, help="0 = phát nhanh hết mức")
    ap.add_argument("--frames", type=int, default=1000)
    ap.add_argument("--cameras", type=int, default=3)
    ap.add_argument("--detector", choices=["none", "classic", "yolo"], default="classic")
    ap.add_argument("--weights", default="edge/weights/best.pt")
    ap.add_argument("--imgsz", type=int, default=224)
    ap.add_argument("--conf", type=float, default=0.25)
    ap.add_argument("--yolo-threads", type=int, default=None, help="giới hạn số nhân CPU để giả lập Edge yếu")
    ap.add_argument("--jpeg-quality", type=int, default=80)
    ap.add_argument("--linger-ms", type=int, default=5)
    ap.add_argument("--on-full", choices=["block", "drop"], default="block")
    ap.add_argument("--validate", action="store_true", help="kiểm tra từng gói tin theo schema (chậm hơn)")
    a = ap.parse_args()

    det = make_detector(a.detector, a.weights, a.imgsz, a.conf, a.yolo_threads)
    stats = run_producer(bootstrap=a.bootstrap, topic=a.topic, images_dir=a.images, detector=det,
                         fps=a.fps, num_frames=a.frames, cameras=a.cameras, jpeg_quality=a.jpeg_quality,
                         linger_ms=a.linger_ms, on_full=a.on_full, validate=a.validate, log_every=100)
    print("\n=== KẾT QUẢ EDGE PRODUCER ===")
    for k, v in stats.items():
        print(f"{k:>22}: {v:.3f}" if isinstance(v, float) else f"{k:>22}: {v}")


if __name__ == "__main__":
    main()
