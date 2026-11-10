import pytest

import worker
from worker import process, stub_detect


@pytest.fixture(autouse=True)
def force_stub(monkeypatch):
    # Tests pin the deterministic stub even if ultralytics is installed.
    monkeypatch.setattr(worker, "_model", None)


def test_stub_is_deterministic():
    assert stub_detect(b"frame-a") == stub_detect(b"frame-a")


def test_event_shape():
    ev = process("cam-1", b"\x00\x01\x02frame", ts=123.0)
    assert ev["device_id"] == "cam-1" and ev["ts"] == 123.0
    for d in ev["detections"]:
        assert d["label"] in ("person", "vehicle")
        assert 0.5 <= d["confidence"] <= 1.0
        x1, y1, x2, y2 = d["bbox"]
        assert 0 <= x1 < x2 <= 1 and 0 <= y1 < y2 <= 1  # normalized coords


def test_some_frames_have_boxes():
    # Over many frames the stub must emit both labels, else the contract is vacuous.
    labels = {d["label"] for i in range(50) for d in stub_detect(b"f%d" % i)}
    assert labels == {"person", "vehicle"}


def test_empty_frame_ok():
    assert process("cam-1", b"", ts=1.0)["detections"] == stub_detect(b"")
