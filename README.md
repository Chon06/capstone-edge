# Tầng Edge: xử lý ảnh công nghiệp thời gian thực theo kiến trúc phân tầng

Đồ án tối ưu kiến trúc phân tầng (Edge → Queue → Processing/Data Lake) cho phân tích dữ liệu hình ảnh thời gian thực trong sản xuất công nghiệp.
Repo này hiện chứa **tầng Edge**: camera giả lập → phân loại lỗi bằng AI → lưu ảnh vào MinIO → gửi kết quả vào Kafka.
Tầng xử lý (Spark Structured Streaming) và các thí nghiệm tối ưu thuộc giai đoạn sau, chưa có trong repo.

Cập nhật lần cuối: 07/11/2026.

## 1. Luồng dữ liệu

```
camera_sim.py ──► features.py + RandomForest ──► storage.py ──► MinIO (bucket frames)
 (đọc ảnh theo FPS)   (nhãn lỗi, confidence)      (upload ảnh)
                                  │
                                  └──► producer.py ──► Kafka topic edge-frames ──► check_drops.py
                                       (JSON theo schema)                          (đếm khung rớt, đo độ trễ, F1)
```

Ảnh gốc nằm ở MinIO, Kafka chỉ chứa JSON có đường dẫn `image_ref`, nên tin nhắn nhỏ (không nhúng base64).

## 2. Cấu trúc thư mục

| Đường dẫn | Nội dung |
|---|---|
| `edge/camera_sim.py` | Phát ảnh theo FPS cấu hình được, lặp vô hạn, giữ đúng nhịp |
| `edge/features.py` | Trích 21 đặc trưng bằng OpenCV |
| `edge/storage.py` | Upload ảnh lên MinIO, trả về `image_ref` |
| `edge/producer.py` | Ghép tất cả và gửi JSON vào Kafka |
| `consumer/check_drops.py` | Đếm khung thiếu/trùng, thống kê độ trễ (có/không tính 10 khung đầu), FPS thực, đánh giá F1 |
| `schema/schema.json` | JSON Schema của tin nhắn (phiên bản 1.0) |
| `scripts/train_baseline.py` | Huấn luyện bộ phân loại, in F1 và ma trận nhầm lẫn |
| `scripts/compare_trees.py` | So sánh 25/50/100 cây (F1 và thời gian suy luận) |
| `scripts/validate_schema.py` | Kiểm tra tin trong Kafka theo schema |
| `scripts/run_experiment.sh` | Chạy thí nghiệm tự động, ghi `results/fps<FPS>_run_<n>.txt` |
| `models/` | Mô hình đã huấn luyện (`rf_50.joblib` là bản đang dùng) |
| `results/` | Kết quả từng lần chạy |
| `data/` | Dataset (không đưa lên Git) |

## 3. Môi trường thực nghiệm

Mọi số đo trong README này lấy từ **một máy ảo VMware**, chưa phải thiết bị Edge thật (Raspberry Pi, Jetson).

| Thành phần | Phiên bản |
|---|---|
| Hệ điều hành | Ubuntu 26.04 (trong VMware), kernel 7.0.0-38-generic |
| CPU / RAM của máy ảo | 2 vCPU (13th Gen Intel(R) Core(TM) i7-1355U), RAM 7.2Gi |
| Java | OpenJDK 17.0.20.1 |
| Kafka | 3.7.0 (Scala 2.13), chế độ ZooKeeper 3.8.3 |
| Hadoop | 3.5.0 (đã cài, chưa dùng ở tầng Edge) |
| Spark | 4.2.0, Scala 2.13.18 (đã cài, chưa dùng ở tầng Edge) |
| Docker | docker.io 29.1.3 |
| MinIO | `scnd/minio-mirror:RELEASE.2025-09-07T16-13-09Z`, image ID: sha256:52dfd5c0bbd38d3219f2058c7af216d9f9a27a994b7b5baad09bbd38866015ff; repo digest: scnd/minio-mirror@sha256:52dfd5c0bbd38d3219f2058c7af216d9f9a27a994b7b5baad09bbd38866015ff |
| Python | 3.14.4 |
| Thư viện chính | OpenCV 5.0.0, kafka-python 3.0.12; các thư viện khác xem `requirements.txt` |

Lưu ý về MinIO: image chính thức `minio/minio` đã bị gỡ khỏi Docker Hub, nên dùng một **bản sao của bên thứ ba** (phiên bản cố định, giấy phép AGPLv3 theo log khởi động). Chỉ dùng trong máy ảo học tập, không đưa ra mạng ngoài, và không có cập nhật bảo mật.

## 4. Dataset

NEU-DET (khuyết tật bề mặt thép cán nóng), nguồn: Kaggle, bộ NEU surface defect database (NEU-DET).

- 1800 ảnh xám 200×200, 6 lớp × 300 ảnh: `crazing`, `inclusion`, `patches`, `pitted_surface`, `rolled-in_scale`, `scratches`.
- Chia sẵn: `train` 240 ảnh/lớp (1440 ảnh), `validation` 60 ảnh/lớp (360 ảnh).
- **Không có ảnh bình thường (OK).** Mô hình chỉ phân biệt 6 loại lỗi.
- Giải nén vào `data/neu-det/NEU-DET/`.

## 5. Cài đặt và khởi chạy

Mỗi dịch vụ chạy foreground cần một terminal riêng.

```bash
# 1) ZooKeeper (terminal 1)
cd ~/Downloads/kafka_2.13-3.7.0
bin/zookeeper-server-start.sh config/zookeeper.properties

# 2) Kafka (terminal 2)
bin/kafka-server-start.sh config/server.properties

# 3) MinIO (cổng API 9002, console 9001)
sudo docker start minio        # lần đầu: xem lệnh `docker run` ở mục 11

# 4) Môi trường Python
cd ~/capstone-edge
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Tạo topic và bucket (một lần): topic `edge-frames` bằng `kafka-topics.sh --create`, bucket `frames` trên giao diện `http://localhost:9001`.

Lưu ý:
- Dữ liệu ZooKeeper (`dataDir`) và Kafka (`log.dirs`) phải nằm ở thư mục cố định, nếu để `/tmp` thì mất topic khi khởi động lại máy.
- Cổng 9000 đang được Hadoop NameNode dùng, nên API của MinIO ánh xạ ra cổng 9002.
- Cảnh báo `DeprecationWarning: value_deserializer ...` của kafka-python không ảnh hưởng kết quả.

## 6. Schema tin nhắn (v1.0)

Chi tiết ở `schema/schema.json` (`additionalProperties: false`). Các trường:

| Trường | Ý nghĩa |
|---|---|
| `schema_version`, `run_id` | Phiên bản schema; mã lần chạy (UTC), `frame_seq` bắt đầu lại từ 1 ở mỗi `run_id` |
| `camera_id`, `frame_seq`, `timestamp` | Nguồn (`cam_01`), số thứ tự khung (dùng đếm khung rớt), thời điểm UTC |
| `edge_status`, `defect_label`, `confidence` | `NG` nếu `confidence >= 0.5`, ngược lại `OK`; nhãn dự đoán; xác suất |
| `inference_ms`, `upload_ms`, `edge_latency_ms` | Đọc ảnh + trích đặc trưng + dự đoán; upload MinIO; tổng hai phần |
| `image_ref` | `minio://frames/<camera_id>/<frame_seq>.jpg` |
| `true_label` | Nhãn thật, chỉ để đánh giá offline |

Ngưỡng 0,5 của `edge_status` là **giả định** vì dataset không có ảnh OK (khoảng 4 khung mỗi lần chạy có `confidence` dưới 0,5 nên được gán `OK`; đó là khung mô hình "không chắc", không phải ảnh bình thường).

## 7. Huấn luyện và đánh giá mô hình

```bash
python3 scripts/train_baseline.py     # in F1, ma trận nhầm lẫn
python3 scripts/compare_trees.py      # so sánh 25/50/100 cây
```

Bộ phân loại: RandomForest (`random_state=42`, `n_jobs=1`) trên 21 đặc trưng (độ sáng, độ lệch chuẩn, mật độ cạnh Canny, gradient Sobel theo x và y, histogram 16 bin).

| Số cây | macro-F1 (validation) | `predict_proba` (ms/ảnh, ngoại tuyến) |
|---|---|---|
| 25 | 0,963 | 1,79 |
| **50** | **0,972** | **2,93** |
| 100 | 0,966 | 5,87 |

Chênh lệch F1 giữa các mức nằm trong mức dao động bình thường (khoảng 2 ảnh trên 360), nên kết luận chỉ là "50 cây không kém 100 cây nhưng nhanh gấp đôi". Lỗi chủ yếu là ảnh `inclusion` bị nhầm sang `pitted_surface` và `scratches` (recall `inclusion` khoảng 0,82–0,87).

Phân rã thời gian suy luận ngoại tuyến (100 cây, trung bình trên 360 ảnh validation): đọc ảnh 1,39 ms, trích đặc trưng 1,39 ms, `predict_proba` 6,85 ms.

## 8. Chạy thí nghiệm

```bash
# <số_lần> <model> [FPS]; mỗi lần: xóa/tạo lại topic, gửi đúng 360 khung validation, chạy consumer
bash scripts/run_experiment.sh 3 ~/capstone-edge/models/rf_50.joblib 5
```

Biến môi trường của producer: `MAX_FRAMES`, `FPS`, `DATA_DIR`, `MODEL_PATH`, `NG_THRESHOLD`, `CAMERA_ID`, `KAFKA_HOST`, `KAFKA_PORT`, `RUN_ID`.
Consumer: `EXPECTED` (số khung mong đợi), `WARMUP` (số khung đầu bị loại khỏi thống kê "ổn định", mặc định 10). Mỗi lần thí nghiệm phải dùng topic sạch.

## 9. Kết quả

Tất cả dùng mô hình `rf_50`, 360 khung ảnh `validation`, máy ảo VMware.

### 9.1 Toàn vẹn dữ liệu
Mọi lần chạy (5 lần ở 5 FPS và mỗi mức 10, 20, 30, 40, 60, 80, 100 FPS) đều nhận đủ 360/360 khung, không trùng, không thiếu. Producer chạy tuần tự (xử lý xong khung này mới đến khung sau), nên "không rớt khung" là hệ quả của thiết kế, **chưa chứng minh khả năng chịu tải**. Khi không kịp nhịp, producer chậm lại chứ không bỏ khung; chỉ số có ý nghĩa là FPS thực đạt được (mục 9.3).

### 9.2 Độ trễ ở 5 FPS (5 lần chạy)

| Lần | Trễ nhịp | `inference_ms` TB / P95 | `upload_ms` TB / P95 | `edge_latency_ms` TB / P95 / P99 |
|---|---|---|---|---|
| 1 | 0 | 9,63 / 13,81 | 9,40 / 13,90 | 19,03 / 26,03 / 32,15 |
| 2 | 5 | 12,97 / 15,54 | 12,22 / 24,49 | 25,19 / 40,06 / 160,21 |
| 3 | 0 | 9,98 / 14,34 | 8,90 / 13,52 | 18,87 / 26,45 / 32,80 |
| 4 (bỏ 10 khung đầu) | 0 | 9,00 / 13,56 | 9,04 / 12,83 | 18,04 / 25,73 / 32,42 |
| 5 (bỏ 10 khung đầu) | 0 | 8,99 / 12,97 | 8,42 / 12,08 | 17,42 / 23,91 / 29,05 |

Lần 2 có một đợt đứng ở khung 344–359 (khoảng 3 giây), trong đó cả `inference_ms` lẫn `upload_ms` cùng tăng. Nguyên nhân chưa được xác định (nghi do tranh tài nguyên của máy ảo). Lần chạy này được giữ lại trong bảng.

### 9.3 Thay đổi FPS

| FPS đặt | FPS thực | Trễ nhịp | `edge_latency_ms` TB / P95 / P99 | `inference_ms` TB / P95 | `upload_ms` TB / P95 |
|---|---|---|---|---|---|
| 5 (lần 5) | 5,00 | 0 | 17,42 / 23,91 / 29,05 | 8,99 / 12,97 | 8,42 / 12,08 |
| 10 | — | 3 | 13,63 / 19,21 / 30,61 | 5,29 / 8,11 | 8,34 / 13,54 |
| 20 | — | 3 | 11,54 / 18,98 / 23,33 | 5,15 / 7,09 | 6,39 / 10,86 |
| 30 | — | 7 | 11,07 / 18,76 / 26,10 | 5,34 / 8,06 | 5,73 / 9,83 |
| 40 | — | 14 | 11,41 / 18,72 / 22,78 | 5,36 / 8,24 | 6,05 / 9,91 |
| 60 | 60,91 | 155 | 13,23 / 18,85 / 22,49 | 6,74 / 11,33 | 6,48 / 9,18 |
| 80 | 68,98 | 360 | 13,22 / 18,15 / 25,94 | 6,90 / 11,27 | 6,32 / 9,10 |
| 100 | 67,80 | 360 | 13,51 / 18,37 / 21,68 | 7,02 / 10,51 | 6,48 / 9,73 |

Ghi chú: các mức 10–40 FPS chạy trước khi consumer có tính năng đo FPS thực và loại khung khởi động, nên số liệu gồm cả khung đầu và không có cột FPS thực. Các mức 5 (lần 5), 60, 80, 100 đã bỏ 10 khung đầu.

Nhận xét:
- **Producer tuần tự bão hòa ở khoảng 68–69 FPS:** đặt 80 và 100 FPS nhưng FPS thực chỉ đạt 69,0 và 67,8, trễ nhịp ở 360/360 khung. Mỗi khung mất khoảng 14,7 ms, gồm `edge_latency_ms` trung bình khoảng 13,2 ms cộng khoảng 1,5 ms chi phí vòng lặp (dựng JSON, gửi, in log). Ở 60 FPS vẫn giữ được nhịp (60,91) dù nhiều khung trễ vì P95 (18,9 ms) lớn hơn khoảng cách hai khung (16,7 ms).
- **Độ trễ không tăng khi bão hòa** vì không có hàng đợi phía trước producer; độ trễ end-to-end chưa được đo nên không thấy được hàng đợi phía Kafka.
- **`inference_ms` thay đổi theo FPS:** cao ở 5 FPS (khoảng 9 ms), thấp nhất ở 10–40 FPS (khoảng 5,3 ms), tăng nhẹ ở 60–100 FPS (khoảng 6,7–7,0 ms). Quan sát lặp lại ở nhiều lần chạy; nguyên nhân chưa xác định (giả thuyết: máy ảo nghỉ lâu giữa các khung ở FPS thấp nên CPU/bộ nhớ đệm nguội, còn FPS cao thì tranh CPU với Kafka, ZooKeeper, MinIO).
- **So với mục tiêu 15 ms của tầng Edge** (chưa chốt áp dụng cho phần nào): `inference_ms` P95 nằm trong khoảng 7–14 ms ở hầu hết các lần (chỉ lần 2 ở 5 FPS đạt 15,54 ms), P99 đôi lúc vượt (đến 17,2 ms); `edge_latency_ms` trung bình dưới 15 ms từ 10 FPS trở lên nhưng P95 luôn trên 18 ms.
- Để vượt ngưỡng khoảng 68 FPS cần song song hóa (upload ở luồng riêng, nhiều worker), thuộc giai đoạn tối ưu sau.

### 9.4 Chất lượng phân loại (luồng thật, 360 khung validation)
Accuracy 0,97, macro-F1 0,97, trùng khớp kết quả ngoại tuyến. Mọi lần chạy cho cùng kết quả vì mô hình và thứ tự ảnh cố định.

| Lớp | Precision | Recall | F1 |
|---|---|---|---|
| crazing | 0,98 | 1,00 | 0,99 |
| inclusion | 1,00 | 0,87 | 0,93 |
| patches | 1,00 | 0,98 | 0,99 |
| pitted_surface | 0,92 | 0,98 | 0,95 |
| rolled-in_scale | 0,98 | 1,00 | 0,99 |
| scratches | 0,95 | 1,00 | 0,98 |

## 10. Hạn chế

1. Máy ảo VMware, không phải Raspberry Pi hay Jetson; các số đo chỉ có giá trị tương đối.
2. Mỗi lần chạy chỉ 360 khung nên P99 dựa trên khoảng 4 giá trị cao nhất; 5 lần lặp ở 5 FPS và chỉ 1 lần ở các mức FPS khác; ngưỡng bão hòa 68–69 FPS dựa trên hai lần chạy bão hòa.
3. Thống kê ở các lần chạy sớm (5 FPS lần 1–3, quét 10–40 FPS) gồm cả khung khởi động nguội; các lần sau loại 10 khung đầu (`WARMUP=10`). "FPS thực" tính từ trường `timestamp` trong tin nhắn nên là FPS phía producer.
4. Dùng bộ phân loại OpenCV + RandomForest thay cho YOLOv8-nano trong kế hoạch ban đầu; chưa so sánh với YOLO. Đây là bài toán phân loại cả ảnh, không phải phát hiện vật thể (bỏ qua hộp giới hạn trong file `.xml`).
5. Không có tập test độc lập (chọn số cây và đánh giá đều trên validation); dataset không có ảnh OK nên `edge_status` chưa có nhãn thật.
6. `edge_latency_ms` chỉ đo phía producer; độ trễ end-to-end, mức dùng CPU/RAM và băng thông chưa được đo.
7. MinIO dùng bản sao bên thứ ba, không còn cập nhật bảo mật.
8. Ngân sách 15 ms của tầng Edge chưa chốt áp dụng cho `inference_ms` hay cho `edge_latency_ms`.

## 11. Tái lập

- Cố định: `random_state=42`, thứ tự ảnh (`sorted`), phiên bản thư viện trong `requirements.txt`, tag image Docker, schema có `schema_version`.
- Lệnh `docker run` của MinIO:

```bash
sudo docker run -d --name minio \
  -p 9002:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  -v ~/minio-data:/data \
  scnd/minio-mirror:RELEASE.2025-09-07T16-13-09Z \
  server /data --console-address ":9001"
```

## 12. Việc tiếp theo

- Chốt `schema.json` và ngân sách 15 ms với cả nhóm.
- Đo độ trễ end-to-end, CPU/RAM, băng thông.
- Thử YOLOv8-nano (hoặc ghi rõ lý do dùng baseline) và so sánh với bộ phân loại hiện tại.
- Song song hóa producer (upload luồng riêng, nhiều worker) và so sánh với baseline tuần tự.
- Spark Structured Streaming đọc Kafka; thí nghiệm tối ưu (buffer, số worker, mức nén ảnh).
