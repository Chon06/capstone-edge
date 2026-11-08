import json
import time
from kafka import KafkaProducer
import  os
from datetime import datetime, timezone
from camera_sim import frames, FPS
from storage import upload_frame
import cv2 
import joblib 
from features import extract
from pathlib import Path
MODEL_PATH = Path(os.getenv("MODEL_PATH", Path(__file__).resolve().parent.parent / "models" / "baseline.joblib"))
MODEL =  joblib.load(MODEL_PATH)
NG_THRESHOLD = float(os.getenv("NG_THRESHOLD", "0.5"))
MAX_FRAMES = int(os.getenv("MAX_FRAMES", "0"))
CAMERA_ID = os.environ.get("CAMERA_ID", "cam_01")
BOOTSTRAP =f"{os.getenv('KAFKA_HOST', 'localhost')}:{os.getenv('KAFKA_PORT','9092')}"
TOPIC ="edge-frames"
RUN_ID = os.getenv("RUN_ID", datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
# cấu hình KafkaProducer với value_serializer để tự động chuyển Python dict thành  JSON bytes 
producer = KafkaProducer(
	bootstrap_servers = BOOTSTRAP,  
	value_serializer = lambda v: json.dumps(v).encode("utf-8")
)
def main():
	print(f" Bắt đầu gửi dữ liệu từ {CAMERA_ID} lên ({BOOTSTRAP}), FPS={FPS}...")
	interval =1.0/ FPS
	next_time = time.perf_counter()
	sent =0
	late =0
	try:
		for seq, path, true_label  in frames():
			if MAX_FRAMES and seq> MAX_FRAMES:
				break
			t0 = time.perf_counter()
			img = cv2.imread(str(path), cv2.IMREAD_GRAYSCALE)
			proba = MODEL.predict_proba([extract(img)])[0]
			i = proba.argmax()
			defect_label = str(MODEL.classes_[i])
			confidence = float(proba[i])
			t1 = time.perf_counter()
			image_ref = upload_frame(path, CAMERA_ID, seq)
			t2= time.perf_counter()
			edge_status ="NG" if confidence >= NG_THRESHOLD else "OK"
			ai_latency_ms = round((t1-t0)* 1000, 2)
			upload_latency_ms = round((t2-t1)*1000, 2)
			edge_latency_ms = round((t2-t0)*1000, 2)
			message ={
				"camera_id" : CAMERA_ID,
				"frame_seq" : seq,
				"timestamp" : datetime.now(timezone.utc).isoformat().replace("+00:00","Z") ,
				"edge_status" : edge_status,
				"defect_label" : defect_label,
				"confidence" : confidence,
				"edge_latency_ms" : edge_latency_ms,
				"image_ref" : image_ref,
				"inference_ms" : ai_latency_ms,
				"upload_ms" : upload_latency_ms,
				"schema_version" : "1.0",
				"run_id" : RUN_ID,
				"true_label" :true_label 
			}
			#Gui message vao topic
			producer.send(TOPIC,message)
			sent +=1
			print(seq, message ["edge_latency_ms"])
			next_time += interval	
			delay =next_time - time.perf_counter()
			if delay >0:
				time.sleep(delay)
			else :
				late+= 1
				print(f"Trễ {-delay *1000:.1f} ms ở khung {seq}")
	except KeyboardInterrupt:
		print (" Dừng producer ...")
	finally:
		producer.close()
		print(f"Đã gửi {sent} khung, trễ nhịp {late} lần.")
if  __name__=="__main__":
	main()

