"""Các bộ phát hiện lỗi ở Tầng Edge. Mỗi bộ có hàm detect(gray_image) -> list[dict]

dict có dạng {"defect_class": str, "confidence": float 0..1, "bbox_xyxy": [x1, y1, x2, y2]}.

- none    : không phát hiện gì (đo thuần hạ tầng Kafka/consumer, loại bỏ ảnh hưởng của AI).
- classic : bộ lọc nhanh bằng xử lý ảnh cổ điển (nền - ảnh), không cần huấn luyện.
            Chỉ trả về lớp chung 'anomaly' - đúng vai trò "lọc nhanh lỗi rõ ràng" ở Edge.
- yolo    : YOLOv8-nano đã huấn luyện trên NEU-DET (phân loại 6 lớp lỗi).
"""
import numpy as np

from common.imaging import gaussian_blur


class NoneDetector:
    name = "none"

    def detect(self, gray):
        return []


class ClassicDetector:
    name = "classic"

    def __init__(self, sigma: float = 12.0, thresh: int = 28, min_ratio: float = 0.004):
        self.sigma, self.thresh, self.min_ratio = sigma, thresh, min_ratio

    def detect(self, gray):
        bg = gaussian_blur(gray, self.sigma)
        diff = np.abs(gray.astype(np.int16) - bg.astype(np.int16))
        mask = diff > self.thresh
        ratio = float(mask.mean())
        if ratio < self.min_ratio:
            return []
        ys, xs = np.nonzero(mask)
        x1, x2 = np.percentile(xs, [1, 99])
        y1, y2 = np.percentile(ys, [1, 99])
        conf = float(min(0.99, 0.5 + ratio * 8))  # độ tin cậy heuristic, không phải xác suất thật
        return [{"defect_class": "anomaly", "confidence": round(conf, 3),
                 "bbox_xyxy": [int(x1), int(y1), int(x2), int(y2)]}]


class YoloDetector:
    name = "yolo"

    def __init__(self, weights: str, imgsz: int = 224, conf: float = 0.25, threads: int | None = None):
        from ultralytics import YOLO  # import trễ: chỉ cần khi dùng YOLO
        if threads:
            import torch
            torch.set_num_threads(int(threads))  # giả lập thiết bị Edge yếu (ít nhân CPU)
        self.model = YOLO(weights)
        self.imgsz, self.conf = imgsz, conf
        self.detect(np.full((200, 200), 128, np.uint8))  # warm-up để lần đo đầu không bị lệch

    def detect(self, gray):
        img3 = np.repeat(gray[..., None], 3, axis=2)  # YOLO cần 3 kênh
        res = self.model.predict(img3, imgsz=self.imgsz, conf=self.conf, device="cpu", verbose=False)[0]
        out = []
        for b in res.boxes:
            out.append({
                "defect_class": res.names[int(b.cls)],
                "confidence": round(float(b.conf), 4),
                "bbox_xyxy": [round(float(v), 1) for v in b.xyxy[0].tolist()],
            })
        return out


def make_detector(kind: str, weights: str | None = None, imgsz: int = 224, conf: float = 0.25,
                  threads: int | None = None):
    if kind == "none":
        return NoneDetector()
    if kind == "classic":
        return ClassicDetector()
    if kind == "yolo":
        if not weights:
            raise ValueError("Dùng --detector yolo thì phải có --weights (ví dụ edge/weights/best.pt)")
        return YoloDetector(weights, imgsz=imgsz, conf=conf, threads=threads)
    raise ValueError(f"Detector không hợp lệ: {kind}")
