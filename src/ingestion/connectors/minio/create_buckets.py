# connectors/minio/create_bucket.py
import logging
from minio import Minio
from minio.error import S3Error
from utils.config import settings

# ====== LOGGING ======
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ====== INIT CLIENT ======
client = Minio(
    settings.minio_endpoint,
    access_key=settings.minio_id,
    secret_key=settings.minio_mdp,
    secure=False
)

# ====== SETTING UP ======
bucket_name_raw_data = "healthcare-raw-data"
prefixes = ["raw-api/", "raw-files/", "raw_cdc/"]

# ====== CREATE BUCKET AND PREFIXES ======
def store_raw_data_healthcare():
    try:
        # Create bucket if not exists
        if not client.bucket_exists(bucket_name_raw_data):
            client.make_bucket(bucket_name_raw_data)
            logger.info(f"✅ Bucket créé : {bucket_name_raw_data}")
        else:
            logger.info(f"🔃 Bucket already exists : {bucket_name_raw_data}")
        
        # Create prefixes (directories)
        for prefix in prefixes:
            client.put_object(
                bucket_name_raw_data,
                prefix,
                data=b"",
                length=0
            )
            logger.info(f"✅ Prefixes created : {prefixes}")
    
    except Exception as e:
        logger.error(f"❌ Issue during creating : {str(e)}", exc_info=True)