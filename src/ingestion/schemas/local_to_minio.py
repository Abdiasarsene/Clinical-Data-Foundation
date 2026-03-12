# src/ingestion/local_to_minio.py
import os
import hashlib
from minio import Minio
from minio.error import S3Error
from utils.config import settings
from observability.logs.logger import log_event
from src.ingestion.connectors.minio.create_buckets import store_raw_data_healthcare

# ====== CONFIGS ======
LOCAL_BASE_DIR = "raw_data/files"
BUCKET_NAME = "healthcare-raw-data"
MINIO_PREFIX = "raw-files"

# ====== CHECK 
def _file_md5(file_path: str) -> str:
    """Calculer le hash MD5 d'un fichier local"""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

# ====== UPLOAD DIRECTORY ======
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

                # Vérifier si l'objet existe déjà
                try:
                    stat = client.stat_object(BUCKET_NAME, object_name)
                    etag = stat.etag.strip('"')
                    local_md5 = _file_md5(local_path)
                    if etag == local_md5:
                        log_event("info", f"Dataset '{object_name}' already exists with same content, skipping upload")
                        continue
                    else:
                        log_event("info", f"Dataset '{object_name}' exists but content changed, updating...")
                except S3Error:
                    # Objet n'existe pas → upload normal
                    pass

                # Upload du fichier
                client.fput_object(
                    bucket_name=BUCKET_NAME,
                    object_name=object_name,
                    file_path=local_path,
                )
                log_event("info", f"Uploaded '{object_name}' to bucket '{BUCKET_NAME}'")

    except Exception as e:
        log_event("error", "Import error via MinIO", str(e))
        raise e


def ensure_bucket(client: Minio) -> None:
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    log_event("info", "Bucket available")