import sys 
from pathlib import Path
import cv2
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report

sys.path.insert(0, 'edge')
from features import extract

ROOT = Path("data/neu-det/NEU-DET")
def load(split):
	X, y =[], []
	for path in sorted((ROOT / split / "images").rglob("*.jpg")):
		img = cv2.imread(str(path), cv2.IMREAD_GRAYSCALE)
		if img is not None:
			X.append(extract(img))
			y.append(path.parent.name)
	return X, y
X_train, y_train = load("train")
X_val, y_val = load("validation")
model = RandomForestClassifier(n_estimators=100, random_state= 42, n_jobs=1)
model.fit(X_train, y_train)
y_pred = model.predict(X_val)
print(classification_report(y_val, y_pred))
Path("models").mkdir(exist_ok=True)
joblib.dump(model, "models/baseline.joblib")
from sklearn.metrics import confusion_matrix
labels = sorted(set(y_val))
print(labels)
print(confusion_matrix(y_val,y_pred, labels = labels))

