import cv2
import numpy as np
def extract(img_gray):
	"""Nhận ảnh xám, trả về vector đặc trung (numpy 1 chiều)."""
	if img_gray.ndim == 3:
		img_gray = cv2.cvtColor(img_gray, cv2.COLOR_BGR2GRAY)
	img = cv2.resize(img_gray, (200,200))
	mean = float(img.mean())
	std = float(img.std())
	hist = cv2.calcHist([img],[0], None, [16], [0,256]).flatten()
	hist = hist/hist.sum()
	edges = cv2.Canny(img, 50, 150)
	edge_density = float(np.count_nonzero(edges))/ edges.size
	gx = float(np.abs(cv2.Sobel(img, cv2.CV_32F, 1, 0)).mean())
	gy = float(np.abs(cv2.Sobel(img, cv2.CV_32F, 0, 1)).mean())
	return  np.array([mean, std, edge_density, gx, gy, *hist])
