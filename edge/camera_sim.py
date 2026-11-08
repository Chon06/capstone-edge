import  os
import time 
from itertools import cycle
from pathlib import Path
FPS = float(os.getenv("FPS","10"))
DATA_DIR = Path(os.getenv("DATA_DIR", "data/neu-det/NEU-DET/train/images"))
def frames():
	files = sorted(DATA_DIR.rglob("*.jpg"))
	for seq, path in enumerate(cycle(files), start =1):
		yield seq, path, path.parent.name
if __name__ == "__main__":
	interval =1.0/FPS
	next_time =time.perf_counter()
	for seq, path, label in frames():
		print (seq, label, path.name)
		next_time +=interval
		delay = next_time - time.perf_counter()
		if delay>0:
			time.sleep(delay)
