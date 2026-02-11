# common/database/s3_client.py
import os
import logging
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

logger = logging.getLogger("s3-client")

class S3Client:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(S3Client, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        
        # Configurações do MinIO/AWS
        self.endpoint_url = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
        self.access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
        self.secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
        self.bucket_name = os.getenv("MINIO_BUCKET_NAME", "chat-files")
        self.region = "us-east-1"

        # Cliente Boto3
        self.s3 = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            config=Config(signature_version="s3v4", s3={'addressing_style': 'path'}),
            region_name=self.region
        )
        self._initialized = True

    def create_bucket_if_not_exists(self):
        try:
            self.s3.head_bucket(Bucket=self.bucket_name)
            logger.info(f"✅ Bucket '{self.bucket_name}' já existe.")
        except ClientError:
            try:
                self.s3.create_bucket(Bucket=self.bucket_name)
                logger.info(f"✅ Bucket '{self.bucket_name}' criado.")
            except Exception as e:
                logger.error(f"❌ Falha ao criar bucket: {e}")

    def generate_presigned_post(self, object_name: str, expiration=3600):
        """
        Gera URL e campos para upload direto via POST (Browser/Mobile).
        """
        try:
            response = self.s3.generate_presigned_post(
                Bucket=self.bucket_name,
                Key=object_name,
                ExpiresIn=expiration
            )
            return response
        except ClientError as e:
            logger.error(f"Erro gerando presigned URL POST: {e}")
            return None

    def generate_presigned_get(self, object_key: str, expiration=3600) -> str:
        """
        Gera URL assinada para leitura (GET).
        """
        try:
            url = self.s3.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': self.bucket_name,
                    'Key': object_key
                },
                ExpiresIn=expiration
            )
            return url
        except Exception as e:
            logger.error(f"Erro no S3 Client (GET): {e}")
            raise e

# Instância Global
s3_client = S3Client()