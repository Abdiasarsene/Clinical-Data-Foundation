# src/ingestion/local_to_minio.py
import os
from minio import Minio
from minio.error import S3Error
from utils.config import settings
from logs.logger import log_event
from connectors.minio.create_buckets import store_raw_data_healthcare

# ====== CONFIGS ======
LOCAL_BASE_DIR = "raw_data/files"
BUCKET_NAME = "healthcare-raw-data"
MINIO_PREFIX = "raw-files"

# ====== UPLOAD DATASETS ======
def upload_directory(client: Minio) -> None:
    try:
        if not os.path.isdir(LOCAL_BASE_DIR):
            raise FileNotFoundError(f"Directory not found: {LOCAL_BASE_DIR}")

        for root, _, files in os.walk(LOCAL_BASE_DIR):
            for filename in files:
                local_path = os.path.join(root, filename)

                # Path relatif depuis collection/files
                relative_path = os.path.relpath(local_path, LOCAL_BASE_DIR)

                # Chemin final dans MinIO
                object_name = f"{MINIO_PREFIX}/{relative_path}".replace("\\", "/")

                print(f"Uploading: {local_path} -> {object_name}")

                client.fput_object(
                    bucket_name=BUCKET_NAME,
                    object_name=object_name,
                    file_path=local_path,
                )
    except Exception as e:
        log_event("error", "Import error via MinIO", str(e))
        raise e

# ====== CHECK UP ======cd 
def ensure_bucket(client: Minio) -> None:
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    log_event("info", "Bucket available")

# if __name__ == "__main__":
#     try:
#         minio_client = store_raw_data_healthcare()
#         ensure_bucket(minio_client)
#         upload_directory(minio_client)
#         print("✅ Upload completed successfully")

#     except S3Error as e:
#         print("❌ MinIO error:", e)

#     except Exception as e:
#         print("❌ Error:", e)
