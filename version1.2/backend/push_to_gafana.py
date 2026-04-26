import os
from dotenv import load_dotenv
from influxdb_client_3 import InfluxDBClient3, Point

# Load environment variables
load_dotenv()
URL = os.getenv("INFLUXDB_URL", "").rstrip("/")
TOKEN = (os.getenv("INFLUXDB_TOKEN") or "").strip()
ORG = (os.getenv("INFLUXDB_ORG") or "").strip()
BUCKET = (os.getenv("INFLUXDB_BUCKET") or "").strip()

def insert_ocr_result(screen_id: str, area_id: str, type_value: str, ocr_value: str) -> None:
    """Insert one row into ip_kvm in InfluxDB.
    Tags: 画面ID, エリアID, type_value (identifiers, used for filtering/grouping)
        type_value is used to identify what type of data ocr_value contains
    Field: OCR (string)
    Timestamp: auto-generated at insert time
    """
    if not URL or not TOKEN or not ORG or not BUCKET:
        raise RuntimeError("Set INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG and INFLUXDB_BUCKET in .env")
    
    point = (
        Point("ip_kvm_v2")
        .tag("画面ID", screen_id)
        .tag("エリアID", area_id)
        .tag("type_value", type_value)
        .field("OCR", ocr_value)
    )

    with InfluxDBClient3(host = URL, token = TOKEN, org = ORG) as client:
        client.write(database = BUCKET, record = point)
        print(f"OK — wrote OCR={ocr_value} (type={type_value}) for 画面ID={screen_id}, エリアID={area_id}")

if __name__ == "__main__":
    insert_ocr_result("1", "1", "number", "0.0") # screen_id, area_id, type_value, ocr_value