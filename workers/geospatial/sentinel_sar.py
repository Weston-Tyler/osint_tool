"""Sentinel-1 SAR dark-vessel detection worker.

Searches the Copernicus Data Space STAC catalogue for Sentinel-1 GRD imagery,
downloads products, runs a simplified CFAR (Constant False Alarm Rate) vessel
detector, cross-references SAR detections against AIS positions to identify
"dark" vessels, and publishes results to Kafka.

Reference: https://dataspace.copernicus.eu/
Requires free registration for COPERNICUS_CLIENT_ID / COPERNICUS_CLIENT_SECRET.
"""

import json
import logging
import math
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import requests
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.geospatial.sentinel_sar")

COPERNICUS_CLIENT_ID = os.getenv("COPERNICUS_CLIENT_ID", "")
COPERNICUS_CLIENT_SECRET = os.getenv("COPERNICUS_CLIENT_SECRET", "")
COPERNICUS_TOKEN_URL = (
    "https://identity.dataspace.copernicus.eu/auth/realms/CDSE"
    "/protocol/openid-connect/token"
)
COPERNICUS_STAC_URL = "https://catalogue.dataspace.copernicus.eu/stac"
COPERNICUS_DOWNLOAD_URL = "https://zipper.dataspace.copernicus.eu/odata/v1"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

DEFAULT_OUTPUT_DIR = os.getenv("SAR_OUTPUT_DIR", "/tmp/sentinel_sar")

# CFAR parameters (simplified threshold-based).
CFAR_GUARD_CELLS = 4
CFAR_BACKGROUND_CELLS = 16
CFAR_THRESHOLD_DB = 8.0  # dB above local background mean

# AIS cross-reference radius.
AIS_MATCH_RADIUS_KM = 2.0


class SentinelSARProcessor:
    """Search, download, and process Sentinel-1 GRD imagery for vessel
    detection and dark-vessel identification."""

    def __init__(
        self,
        client_id: str | None = None,
        client_secret: str | None = None,
        kafka_bootstrap: str | None = None,
        output_dir: str | None = None,
    ):
        self.client_id = client_id or COPERNICUS_CLIENT_ID
        self.client_secret = client_secret or COPERNICUS_CLIENT_SECRET
        self.kafka_bootstrap = kafka_bootstrap or KAFKA_BOOTSTRAP
        self.output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self._access_token: str | None = None
        self._token_expiry: float = 0.0
        self._producer: KafkaProducer | None = None
        self._dark_vessel_buffer: list[dict] = []

    # ------------------------------------------------------------------
    # OAuth2 authentication
    # ------------------------------------------------------------------

    def _authenticate(self) -> str:
        """Obtain or refresh an OAuth2 access token from Copernicus."""
        if self._access_token and time.time() < self._token_expiry - 60:
            return self._access_token

        if not self.client_id or not self.client_secret:
            raise RuntimeError(
                "COPERNICUS_CLIENT_ID and COPERNICUS_CLIENT_SECRET must be set"
            )

        resp = requests.post(
            COPERNICUS_TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=30,
        )
        resp.raise_for_status()
        token_data = resp.json()
        self._access_token = token_data["access_token"]
        self._token_expiry = time.time() + token_data.get("expires_in", 600)
        logger.info("Copernicus access token refreshed")
        return self._access_token

    def _auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._authenticate()}"}

    # ------------------------------------------------------------------
    # Kafka
    # ------------------------------------------------------------------

    def _get_producer(self) -> KafkaProducer:
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            )
        return self._producer

    # ------------------------------------------------------------------
    # STAC catalogue search
    # ------------------------------------------------------------------

    def search_imagery(
        self,
        bbox: list[float],
        start_date: str,
        end_date: str,
        product_type: str = "GRD",
        max_items: int = 50,
    ) -> list[dict]:
        """Search the Copernicus STAC catalogue for Sentinel-1 imagery.

        Parameters
        ----------
        bbox : list[float]
            ``[west, south, east, north]`` in WGS-84.
        start_date, end_date : str
            ISO-8601 date strings (``YYYY-MM-DD``).
        product_type : str
            ``"GRD"`` (default) or ``"SLC"``.
        max_items : int
            Maximum results to return.

        Returns
        -------
        list[dict]  STAC Feature dicts.
        """
        collection = "sentinel-1-grd" if product_type == "GRD" else "sentinel-1-slc"
        payload = {
            "collections": [collection],
            "bbox": bbox,
            "datetime": f"{start_date}T00:00:00Z/{end_date}T23:59:59Z",
            "limit": max_items,
        }

        resp = requests.post(f"{COPERNICUS_STAC_URL}/search", json=payload, timeout=60)
        resp.raise_for_status()
        features = resp.json().get("features", [])
        logger.info(
            "STAC search: %d %s items for bbox=%s (%s to %s)",
            len(features), product_type, bbox, start_date, end_date,
        )
        return features

    # ------------------------------------------------------------------
    # Product download
    # ------------------------------------------------------------------

    def download_product(self, product_id: str, output_dir: str | None = None) -> Path:
        """Download a Sentinel-1 product via Copernicus Data Space S3/OData.

        Parameters
        ----------
        product_id : str
            Product UUID from the STAC catalogue.
        output_dir : str, optional
            Override for default output directory.

        Returns
        -------
        Path to the downloaded zip file.
        """
        dest = Path(output_dir) if output_dir else self.output_dir
        dest.mkdir(parents=True, exist_ok=True)
        out_path = dest / f"{product_id}.zip"

        if out_path.exists():
            logger.info("Product %s already on disk: %s", product_id, out_path)
            return out_path

        url = f"{COPERNICUS_DOWNLOAD_URL}/Products('{product_id}')/$value"
        logger.info("Downloading product %s ...", product_id)
        with requests.get(url, headers=self._auth_headers(), stream=True, timeout=600) as resp:
            resp.raise_for_status()
            with open(out_path, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                    fh.write(chunk)

        size_mb = out_path.stat().st_size / 1e6
        logger.info("Downloaded %s (%.1f MB)", out_path.name, size_mb)
        return out_path

    # ------------------------------------------------------------------
    # SAR image loading
    # ------------------------------------------------------------------

    @staticmethod
    def _load_sar_array(image_path: str | Path) -> np.ndarray:
        """Load a SAR image into a 2-D float64 numpy array.

        Supports GeoTIFF via *rasterio* (preferred) and ``.npy`` files for
        unit-testing.  In a production pipeline the downloaded zip would first
        be decompressed and the measurement TIFF extracted.
        """
        path = Path(image_path)
        ext = path.suffix.lower()

        if ext in (".tif", ".tiff"):
            try:
                import rasterio  # type: ignore[import-untyped]

                with rasterio.open(path) as src:
                    return src.read(1).astype(np.float64)
            except ImportError:
                logger.warning("rasterio not installed; cannot read GeoTIFF")
                raise

        if ext == ".npy":
            return np.load(str(path)).astype(np.float64)

        raise ValueError(f"Unsupported SAR image format: {ext}")

    @staticmethod
    def _to_db(linear: np.ndarray) -> np.ndarray:
        """Convert linear power to dB (clamped to avoid log(0))."""
        return 10.0 * np.log10(np.clip(linear, 1e-10, None))

    # ------------------------------------------------------------------
    # CFAR vessel detection
    # ------------------------------------------------------------------

    def detect_vessels(self, image_path: str | Path) -> list[dict]:
        """Simplified CFAR vessel detection on SAR backscatter.

        Algorithm
        ---------
        1. Convert image to dB.
        2. For each pixel compute a local background mean in a ring
           (guard-band excluded) around the pixel.
        3. Pixels exceeding ``background_mean + CFAR_THRESHOLD_DB`` are
           flagged as detections.
        4. Adjacent flagged pixels are clustered (connected components) and
           centroid / peak returned.

        Parameters
        ----------
        image_path : str or Path
            Path to a GeoTIFF or ``.npy`` SAR image.

        Returns
        -------
        list[dict]
            Detections with ``pixel_row``, ``pixel_col``, ``intensity_db``,
            ``background_db``, ``snr_db``, ``num_pixels``.
        """
        arr = self._load_sar_array(image_path)
        db = self._to_db(arr)
        rows, cols = db.shape
        guard = CFAR_GUARD_CELLS
        bg = CFAR_BACKGROUND_CELLS
        win = guard + bg

        # Pad for boundary handling.
        padded = np.pad(db, win, mode="edge")

        # Integral image for O(1) box sums.
        integral = np.cumsum(np.cumsum(padded, axis=0), axis=1)

        def _box_sum(r1: int, c1: int, r2: int, c2: int) -> float:
            return float(
                integral[r2, c2]
                - integral[r1, c2]
                - integral[r2, c1]
                + integral[r1, c1]
            )

        mask = np.zeros((rows, cols), dtype=bool)
        bg_mean_map = np.zeros((rows, cols), dtype=np.float64)

        for r in range(rows):
            for c in range(cols):
                pr, pc = r + win, c + win
                outer = _box_sum(pr - win, pc - win, pr + win, pc + win)
                outer_n = (2 * win + 1) ** 2
                inner = _box_sum(pr - guard, pc - guard, pr + guard, pc + guard)
                inner_n = (2 * guard + 1) ** 2
                bg_sum = outer - inner
                bg_n = outer_n - inner_n
                if bg_n == 0:
                    continue
                bg_mu = bg_sum / bg_n
                bg_mean_map[r, c] = bg_mu
                if db[r, c] > bg_mu + CFAR_THRESHOLD_DB:
                    mask[r, c] = True

        detections = self._cluster_detections(mask, db, bg_mean_map)
        logger.info("Detected %d vessel candidates in %s", len(detections), image_path)
        return detections

    @staticmethod
    def _cluster_detections(
        mask: np.ndarray,
        db: np.ndarray,
        bg_mean: np.ndarray,
    ) -> list[dict]:
        """Cluster connected True pixels into discrete detections."""
        try:
            from scipy.ndimage import label as scipy_label  # type: ignore[import-untyped]
        except ImportError:
            # Fallback: each pixel is its own detection.
            results = []
            ys, xs = np.nonzero(mask)
            for y, x in zip(ys, xs):
                results.append({
                    "pixel_row": int(y),
                    "pixel_col": int(x),
                    "num_pixels": 1,
                    "intensity_db": round(float(db[y, x]), 2),
                    "background_db": round(float(bg_mean[y, x]), 2),
                    "snr_db": round(float(db[y, x] - bg_mean[y, x]), 2),
                })
            return results

        labelled, n_features = scipy_label(mask)
        results: list[dict] = []
        for lbl in range(1, n_features + 1):
            ys, xs = np.nonzero(labelled == lbl)
            peak_idx = int(np.argmax(db[ys, xs]))
            centroid_r = int(np.mean(ys))
            centroid_c = int(np.mean(xs))
            peak_db = float(db[ys[peak_idx], xs[peak_idx]])
            bg_db = float(bg_mean[ys[peak_idx], xs[peak_idx]])
            results.append({
                "pixel_row": centroid_r,
                "pixel_col": centroid_c,
                "num_pixels": int(len(ys)),
                "intensity_db": round(peak_db, 2),
                "background_db": round(bg_db, 2),
                "snr_db": round(peak_db - bg_db, 2),
            })
        return results

    # ------------------------------------------------------------------
    # AIS cross-reference
    # ------------------------------------------------------------------

    @staticmethod
    def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Great-circle distance in kilometres."""
        R = 6371.0
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(math.radians(lat1))
            * math.cos(math.radians(lat2))
            * math.sin(dlon / 2) ** 2
        )
        return R * 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))

    def cross_reference_ais(
        self,
        detections: list[dict],
        ais_positions: list[dict],
        match_radius_km: float = AIS_MATCH_RADIUS_KM,
    ) -> list[dict]:
        """Compare SAR detections to known AIS positions and return *dark*
        vessels -- SAR targets with no nearby AIS match.

        Parameters
        ----------
        detections : list[dict]
            Must contain ``lat`` / ``lon`` keys.
        ais_positions : list[dict]
            Must contain ``lat`` / ``lon`` keys; optional ``mmsi``,
            ``vessel_name``.
        match_radius_km : float
            Maximum distance for a SAR-AIS match.

        Returns
        -------
        list[dict]  Dark-vessel events.
        """
        dark_vessels: list[dict] = []

        for det in detections:
            det_lat, det_lon = det.get("lat"), det.get("lon")
            if det_lat is None or det_lon is None:
                continue

            best_dist = float("inf")
            best_ais: dict | None = None

            for ais in ais_positions:
                a_lat, a_lon = ais.get("lat"), ais.get("lon")
                if a_lat is None or a_lon is None:
                    continue
                dist = self._haversine_km(det_lat, det_lon, a_lat, a_lon)
                if dist < best_dist:
                    best_dist = dist
                    best_ais = ais

            if best_dist > match_radius_km:
                dark = {
                    "event_type": "DARK_VESSEL_DETECTED",
                    "source": "sentinel_sar",
                    "lat": det_lat,
                    "lon": det_lon,
                    "intensity_db": det.get("intensity_db"),
                    "snr_db": det.get("snr_db"),
                    "nearest_ais_distance_km": round(best_dist, 2) if best_ais else None,
                    "nearest_ais_mmsi": best_ais.get("mmsi") if best_ais else None,
                    "confidence": 0.75,
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                }
                dark_vessels.append(dark)
                self._dark_vessel_buffer.append(dark)

        logger.info(
            "Cross-ref: %d SAR vs %d AIS -> %d dark vessels",
            len(detections), len(ais_positions), len(dark_vessels),
        )
        return dark_vessels

    # ------------------------------------------------------------------
    # Publisher
    # ------------------------------------------------------------------

    def publish_dark_vessels(self) -> int:
        """Publish buffered dark-vessel detections to Kafka."""
        if not self._dark_vessel_buffer:
            logger.info("No dark vessels to publish")
            return 0

        producer = self._get_producer()
        count = 0
        for record in self._dark_vessel_buffer:
            try:
                record.setdefault(
                    "event_id",
                    f"sar_dark_{record.get('lat', 0):.4f}_{record.get('lon', 0):.4f}"
                    f"_{record.get('detected_at', '')}",
                )
                producer.send("mda.sentinel.dark_vessels", record)
                count += 1
            except Exception as exc:
                logger.error("Kafka publish failed: %s", exc)
                producer.send(
                    "mda.dlq",
                    {"source": "sentinel_sar", "error": str(exc), "raw": str(record)[:500]},
                )

        producer.flush()
        self._dark_vessel_buffer.clear()
        logger.info("Published %d dark vessel detections", count)
        return count

    # ------------------------------------------------------------------
    # End-to-end convenience
    # ------------------------------------------------------------------

    @staticmethod
    def _geo_reference(
        detections: list[dict],
        bbox: list[float],
        stac_item: dict,
    ) -> list[dict]:
        """Approximate geo-referencing of pixel coordinates using scene bbox.

        Production systems should use the GeoTIFF affine transform instead.
        """
        west, south, east, north = bbox
        props = stac_item.get("properties", {})
        nrows = props.get("rows", 10000)
        ncols = props.get("cols", 10000)

        for det in detections:
            r = det.get("pixel_row", 0)
            c = det.get("pixel_col", 0)
            det["lat"] = north - (r / max(nrows, 1)) * (north - south)
            det["lon"] = west + (c / max(ncols, 1)) * (east - west)
        return detections

    def process_aoi(
        self,
        bbox: list[float],
        start_date: str,
        end_date: str,
        ais_positions: list[dict] | None = None,
    ) -> list[dict]:
        """Full pipeline: search -> download -> detect -> AIS cross-ref.

        Parameters
        ----------
        bbox : list[float]
            ``[west, south, east, north]``.
        start_date, end_date : str
            ISO dates.
        ais_positions : list[dict], optional
            AIS data for dark-vessel identification.

        Returns
        -------
        list[dict]  Detected (dark) vessels.
        """
        items = self.search_imagery(bbox, start_date, end_date)
        all_dark: list[dict] = []

        for item in items:
            product_id = item.get("id", "")
            try:
                product_path = self.download_product(product_id)
            except Exception as exc:
                logger.error("Download failed for %s: %s", product_id, exc)
                continue

            try:
                raw_dets = self.detect_vessels(product_path)
            except Exception as exc:
                logger.error("Detection failed for %s: %s", product_id, exc)
                continue

            item_bbox = item.get("bbox", bbox)
            geo_dets = self._geo_reference(raw_dets, item_bbox, item)

            if ais_positions:
                dark = self.cross_reference_ais(geo_dets, ais_positions)
                all_dark.extend(dark)
            else:
                for det in geo_dets:
                    det.update({
                        "event_type": "SAR_VESSEL_DETECTED",
                        "source": "sentinel_sar",
                        "confidence": 0.60,
                        "detected_at": datetime.now(timezone.utc).isoformat(),
                    })
                    self._dark_vessel_buffer.append(det)
                all_dark.extend(geo_dets)

        return all_dark

    def close(self) -> None:
        if self._producer is not None:
            self._producer.flush()
            self._producer.close()
            self._producer = None


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    processor = SentinelSARProcessor()
    try:
        end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        start = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
        # Gulf of Guinea -- common dark-vessel zone.
        bbox = [-5.0, 0.0, 5.0, 7.0]
        processor.process_aoi(bbox, start, end)
        processor.publish_dark_vessels()
    except KeyboardInterrupt:
        logger.info("Shutting down Sentinel SAR processor")
    finally:
        processor.close()
