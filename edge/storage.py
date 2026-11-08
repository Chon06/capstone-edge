import os
from pathlib import Path
from minio import Minio

ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9002")
BUCKET = "frames"

client = Minio(
    ENDPOINT,
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False,        
  # TODO 1: Đặt False để sử dụng HTTP thay vì HTTPS.
)

def upload_frame(path: Path, camera_id: str, seq: int) -> str:
    object_name = f"{camera_id}/{seq:06d}.jpg"
    
    # TODO 2: Tải file lên MinIO bucket
    client.fput_object(BUCKET, object_name, str(path))
    
    # TODO 3: Trả về đường dẫn định dạng minio://
    return f"minio://{BUCKET}/{object_name}"

if __name__ == "__main__":
    test = next(Path("data/neu-det/NEU-DET/train/images").rglob("*.jpg"))
    print(upload_frame(test, "cam01", 1))
