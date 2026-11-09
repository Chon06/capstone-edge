"""Huấn luyện YOLOv8-nano trên NEU-DET (chạy được trên CPU, hoặc Google Colab nếu muốn nhanh hơn).

    python -m edge.train_yolo --data data/yolo/neu_det.yaml --epochs 30

Kết quả: edge/weights/best.pt (dùng cho --detector yolo). Thêm --export-onnx để xuất ONNX
(thường suy luận nhanh hơn trên CPU; cần `pip install onnx onnxruntime`).
"""
import argparse
import shutil
from pathlib import Path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="data/yolo/neu_det.yaml")
    ap.add_argument("--model", default="yolov8n.pt", help="trọng số khởi tạo (tự tải về lần đầu)")
    ap.add_argument("--epochs", type=int, default=30)
    ap.add_argument("--imgsz", type=int, default=224, help="bội số của 32; ảnh gốc 200x200")
    ap.add_argument("--batch", type=int, default=16)
    ap.add_argument("--device", default="cpu")
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--export-onnx", action="store_true")
    a = ap.parse_args()

    from ultralytics import YOLO

    model = YOLO(a.model)
    model.train(data=a.data, epochs=a.epochs, imgsz=a.imgsz, batch=a.batch, device=a.device,
                workers=a.workers, patience=10, exist_ok=True, project="runs", name="neu_det")
    best = Path(model.trainer.save_dir) / "weights" / "best.pt"
    dst = Path("edge/weights/best.pt")
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(best, dst)
    print(f"\nĐã lưu trọng số: {dst}")

    final = YOLO(str(dst))
    m = final.val(data=a.data, imgsz=a.imgsz, device=a.device, verbose=False)
    print(f"mAP50 = {m.box.map50:.4f} | mAP50-95 = {m.box.map:.4f}  <- ghi vào báo cáo (độ chính xác Edge)")
    if a.export_onnx:
        onnx_path = final.export(format="onnx", imgsz=a.imgsz)
        print(f"Đã xuất ONNX: {onnx_path}")


if __name__ == "__main__":
    main()
