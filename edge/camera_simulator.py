"""Camera băng chuyền giả lập: phát lại ảnh từ một thư mục với tốc độ FPS định trước.

fps <= 0 nghĩa là phát nhanh hết mức (dùng để tìm giới hạn tải của hệ thống).
Nhiều camera: ảnh được chia vòng tròn cho CAM_LINE_01, CAM_LINE_02, ...
(cần nhiều camera/khóa khác nhau để Kafka chia tải ra nhiều partition).
"""
import time
from collections import namedtuple

from common.imaging import list_images, read_gray
from common.message import now_ms

Frame = namedtuple("Frame", "image_id camera_id frame_seq ts_capture_ms image")


class CameraSimulator:
    def __init__(self, image_dir, fps: float = 25.0, num_frames: int = 1000, cameras: int = 1,
                 camera_prefix: str = "CAM_LINE_"):
        paths = list_images(image_dir)
        if not paths:
            raise FileNotFoundError(f"Không có ảnh nào trong {image_dir}")
        self.images = [read_gray(p) for p in paths]  # nạp sẵn vào RAM để không đo nhầm thời gian đọc đĩa
        self.fps, self.num_frames, self.cameras = fps, num_frames, max(1, cameras)
        self.prefix = camera_prefix
        self.late_frames = 0       # số khung bị trễ hơn 1 chu kỳ so với lịch (Edge không theo kịp FPS)
        self.max_lag_ms = 0.0

    def __iter__(self):
        period = 1.0 / self.fps if self.fps > 0 else 0.0
        seq = [0] * self.cameras
        start = time.perf_counter()
        for i in range(self.num_frames):
            if period:
                due = start + i * period
                remaining = due - time.perf_counter()
                if remaining > 0.002:
                    time.sleep(remaining - 0.001)
                while time.perf_counter() < due:  # chờ chính xác phần cuối
                    pass
                lag = (time.perf_counter() - due) * 1000
                self.max_lag_ms = max(self.max_lag_ms, lag)
                if lag > period * 1000:
                    self.late_frames += 1
            cam = i % self.cameras
            seq[cam] += 1
            yield Frame(
                image_id=f"IMG_{i + 1:06d}",
                camera_id=f"{self.prefix}{cam + 1:02d}",
                frame_seq=seq[cam],
                ts_capture_ms=now_ms(),
                image=self.images[i % len(self.images)],
            )
