-- Device registry + append-only config history (shadows live in DynamoDB).
CREATE TABLE IF NOT EXISTS devices (
  id TEXT PRIMARY KEY, org TEXT NOT NULL, model TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS devices_org ON devices(org);
CREATE TABLE IF NOT EXISTS config_history (
  id BIGSERIAL PRIMARY KEY, device_id TEXT NOT NULL REFERENCES devices(id),
  version BIGINT NOT NULL, desired JSONB NOT NULL, reported JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(device_id, version)
);
CREATE TABLE IF NOT EXISTS detection_events (
  id BIGSERIAL PRIMARY KEY, device_id TEXT NOT NULL, ts TIMESTAMPTZ NOT NULL,
  detections JSONB NOT NULL
);
