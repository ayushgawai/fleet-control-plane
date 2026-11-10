"""Frame worker: consumes frames, detects people/vehicles, publishes events.

Uses YOLO via ultralytics when installed; otherwise a deterministic stub
(same input -> same boxes) so tests and CI need no model download.
Kafka is optional: with KAFKA_BOOTSTRAP unset it runs on in-memory lists,
which is what the tests drive. `python worker.py --demo` runs one stub frame.
"""
import hashlib, json, os, sys, time

try:
    from ultralytics import YOLO  # type: ignore
    _model = YOLO("yolov8n.pt")
except Exception:  # no ultralytics / no weights -> stub
    _model = None

CLASSES = {0: "person", 2: "vehicle", 5: "vehicle", 7: "vehicle"}


def stub_detect(frame: bytes):
    """Deterministic pseudo-detections derived from the frame hash."""
    h = hashlib.sha256(frame).digest()
    boxes = []
    for i in range(h[0] % 3):  # 0-2 boxes
        x, y = h[1 + i * 4] / 255 * 0.8, h[2 + i * 4] / 255 * 0.8  # keep x+0.2 <= 1
        boxes.append({"label": "person" if i % 2 == 0 else "vehicle",
                      "confidence": round(0.5 + h[3 + i * 4] / 510, 3),
                      "bbox": [round(x, 3), round(y, 3), round(x + 0.2, 3), round(y + 0.2, 3)]})
    return boxes


def detect(frame: bytes):
    if _model is None:
        return stub_detect(frame)
    import numpy as np, cv2  # only on the real path
    img = cv2.imdecode(np.frombuffer(frame, np.uint8), cv2.IMREAD_COLOR)
    out = []
    for r in _model(img, verbose=False):
        for b in r.boxes:
            label = CLASSES.get(int(b.cls))
            if label:
                out.append({"label": label, "confidence": round(float(b.conf), 3),
                            "bbox": [round(float(v), 3) for v in b.xyxyn[0].tolist()]})  # normalized, same as stub
    return out


def process(device_id: str, frame: bytes, ts: float = None) -> dict:
    return {"device_id": device_id, "ts": time.time() if ts is None else ts, "detections": detect(frame)}


def main():
    if "--demo" in sys.argv:
        print(json.dumps(process("cam-demo", b"demo-frame")))
        return
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP")
    if not bootstrap:
        print("KAFKA_BOOTSTRAP unset: set it to consume 'frames' and publish 'detections' (stub detector active: %s)" % (_model is None))
        return
    from kafka import KafkaConsumer, KafkaProducer  # kafka-python, prod only
    consumer = KafkaConsumer("frames", bootstrap_servers=bootstrap, value_deserializer=bytes)
    producer = KafkaProducer(bootstrap_servers=bootstrap, value_serializer=lambda v: json.dumps(v).encode())
    open("/tmp/ready", "w").close()  # k8s exec probes check this file
    for msg in consumer:
        ev = process(msg.key.decode() if msg.key else "unknown", msg.value)
        producer.send("detections", ev, key=ev["device_id"].encode())


if __name__ == "__main__":
    main()
