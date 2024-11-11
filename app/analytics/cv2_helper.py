# YOLO object detection
import cv2 as cv
import numpy as np

import json


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()

        return super(NpEncoder, self).default(obj)


# Load names of classes and get random colors
classes = open("coco.names").read().strip().split("\n")
np.random.seed(42)
colors = np.random.randint(0, 255, size=(len(classes), 3), dtype="uint8")

# Give the configuration and weight files for the model and load the network.
net = cv.dnn.readNetFromDarknet("yolov3-tiny.cfg", "yolov3-tiny.weights")
net.setPreferableBackend(cv.dnn.DNN_BACKEND_OPENCV)
net.setPreferableTarget(cv.dnn.DNN_TARGET_CPU)

# determine the output layer
ln = net.getLayerNames()
# print(len(ln), ln)
# print(net.getUnconnectedOutLayers())
ln = [ln[i - 1] for i in net.getUnconnectedOutLayers()]


# def track(cap, track_window):
#    track_window = (x, y, w, h)
#    set up the ROI for tracking
#    roi = frame[y : y + h, x : x + w]
#    hsv_roi = cv.cvtColor(roi, cv.COLOR_BGR2HSV)
#    mask = cv.inRange(
#        hsv_roi, np.array((0.0, 60.0, 32.0)), np.array((180.0, 255.0, 255.0))
#    )
#    roi_hist = cv.calcHist([hsv_roi], [0], mask, [180], [0, 180])
#    cv.normalize(roi_hist, roi_hist, 0, 255, cv.NORM_MINMAX)
#    # Setup the termination criteria, either 10 iters or move by at least 1pt
#    term_crit = (cv.TERM_CRITERIA_EPS | cv.TERM_CRITERIA_COUNT, 10, 1)
#    while 1:
#        ret, frame = cap.read()
#        if ret == True:
#            hsv = cv.cvtColor(frame, cv.COLOR_BGR2HSV)
#            dst = cv.calcBackProject([hsv], [0], roi_hist, [0, 180], 1)
#            # apply camshift to get the new location
#            ret, track_window = cv.CamShift(dst, track_window, term_crit)
#            # Draw it on image
#            pts = cv.boxPoints(ret)
#            pts = np.int0(pts)
#            img2 = cv.polylines(frame, [pts], True, 255, 2)


def decode(frame):
    nparr = np.frombuffer(frame, np.uint8)
    # print(nparr.shape, nparr.dtype)
    # nparr = nparr.reshape((1920, 1080))
    # print(nparr.shape, nparr.dtype)
    return cv.imdecode(nparr, cv.IMREAD_COLOR)


def encode(img):
    is_success, encoded_img = cv.imencode(".jpg", img)
    assert is_success, "encoding failure"
    encoded_data = np.array(encoded_img)
    encoded_bytes = encoded_data.tobytes()
    return encoded_bytes


def np_serialize(outputs):
    # outputs = [output.tolist() for output in outputs]
    # shapes = [output.shape for output in outputs]
    # outputs = [output.tobytes() for output in outputs]
    # return outputs, shapes
    pass


def np_deserialize(outputs_bytes):
    # outputs_lists = json.loads(outputs_lists)
    # outputs = [np.asarray(o_list) for o_list in outputs_lists]
    # outputs = [
    #     np.frombuffer(output, dtype="float32").reshape(shape)
    #     for output, shape in zip(outputs_list, shapes)
    # ]
    # return outputs
    pass


def json_serialize(outputs):
    return json.dumps(outputs, cls=NpEncoder)


def json_deserialize(outputs_bytes):
    return json.loads(outputs_bytes)


def detect(img):
    # construct a blob from the image
    blob = cv.dnn.blobFromImage(
        img,
        1 / 255.0,
        (416, 416),
        swapRB=True,
        crop=False,
    )
    _ = blob[0, 0, :, :]

    net.setInput(blob)
    # t0 = time.time()
    outputs = net.forward(ln)
    # t1 = time.time()
    # print("forward time=", t1 - t0)
    # print(type(outputs[0]))
    # print(outputs[0].dtype)
    boxes = []
    confidences = []
    classIDs = []
    h, w = img.shape[:2]

    for output in outputs:
        for detection in output:
            scores = detection[5:]
            classID = np.argmax(scores)
            confidence = scores[classID]
            if confidence > 0.5:
                box = detection[:4] * np.array([w, h, w, h])
                (centerX, centerY, width, height) = box.astype("int")
                x = int(centerX - (width / 2))
                y = int(centerY - (height / 2))
                box = [x, y, int(width), int(height)]
                boxes.append(box)
                confidences.append(float(confidence))
                classIDs.append(classID)

    fBoxes = []
    fConfidences = []
    fClassIDs = []
    indices = cv.dnn.NMSBoxes(boxes, confidences, 0.5, 0.4)
    if len(indices) > 0:
        for i in indices.flatten():
            fBoxes.append(boxes[i])
            fConfidences.append(confidences[i])
            fClassIDs.append(classIDs[i])
    outputs = [fBoxes, fConfidences, fClassIDs]
    return outputs


def annotate(img, outputs):
    for box, confidence, classID in zip(outputs[0], outputs[1], outputs[2]):
        (x, y) = (box[0], box[1])
        (w, h) = (box[2], box[3])
        color = [int(c) for c in colors[classID]]
        cv.rectangle(img, (x, y), (x + w, y + h), color, 2)
        text = "{}: {:.4f}".format(classes[classID], confidence)
        cv.putText(
            img,
            text,
            (x, y - 5),
            cv.FONT_HERSHEY_SIMPLEX,
            0.5,
            color,
            1,
        )

    # t2 = time.time()
    # print("annotate time=", t2 - t1)
    return img


if __name__ == "__main__":
    img = cv.imread("traffic.jpeg")

    outputs = detect(img)
    res = annotate(outputs, img)
    cv.imwrite("result.jpeg", res)
