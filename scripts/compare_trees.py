import sys
import time
from pathlib import Path
import cv2
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import f1_score

sys.path.insert(0, "edge")
from features import extract
ROOT = Path("data/neu-det/NEU-DET")
def load(split):
	X,y = [], []
	for p in sorted((ROOT / split / "images").rglob("*.jpg")):
		X.append(extract(cv2.imread(str(p), cv2.IMREAD_GRAYSCALE)))
		y.append(p.parent.name)
	return X, y
X_train, y_train = load("train")
X_val, y_val = load("validation")
for n in (25, 50, 100):
	model = RandomForestClassifier(n_estimators=n, random_state=42, n_jobs=1)
	model.fit(X_train, y_train)
	f1= f1_score(y_val, model.predict(X_val), average="macro")
	t= time.perf_counter()
	for x in X_val:
		model.predict_proba([x])
	ms = (time.perf_counter() -t) / len(X_val)*1000
	print(f"n_estimators={n}: macro-F1={f1:.3f}, predict_proba={ms:.2f} ms/ảnh")
	joblib.dump(model, f"models/rf_{n}.joblib")
