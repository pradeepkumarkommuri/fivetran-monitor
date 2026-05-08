"""Raw ingestion: Fivetran API → MinIO."""
from __future__ import annotations
import logging
from datetime import datetime, timezone
from src.ingestion.fivetran_client import FivetranClient
from src.utils import storage, metrics

log = logging.getLogger(__name__)

def run_ingestion(group_id: str | None = None) -> dict:
    """
    Fetch all connectors + sync history and persist to MinIO.
    Returns summary dict suitable for XCom in Airflow.
    """
    storage.ensure_bucket()
    client     = FivetranClient()
    connectors = client.list_connectors(group_id=group_id)
    metrics.connectors_total.set(len(connectors))

    ts      = datetime.now(timezone.utc)
    written = []

    for connector in connectors:
        cid = connector["id"]
        try:
            history = client.get_sync_history(cid, limit=20)
            payload = {
                "ingested_at": ts.isoformat(),
                "connector":   connector,
                "history":     history,
            }
            key = storage.raw_key_for(cid, ts)
            storage.put_json(storage.get_settings().storage.raw_prefix, key, payload)
            written.append(cid)
        except Exception as exc:
            log.error("Ingestion failed for %s: %s", cid, exc)
            metrics.ingestion_errors.labels(connector_id=cid).inc()

    log.info("Ingested %d/%d connectors → MinIO", len(written), len(connectors))
    return {
        "ingested_at": ts.isoformat(),
        "total": len(connectors),
        "succeeded": len(written),
        "failed": len(connectors) - len(written),
        "partition": ts.strftime("%Y/%m/%d/%H"),
    }
