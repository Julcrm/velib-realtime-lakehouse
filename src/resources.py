from dagster import ConfigurableResource
import boto3
import json
from typing import Union

class MinioResource(ConfigurableResource):
    """
    Ressource pour interagir avec un stockage d'objets MinIO (compatible S3).
    """
    endpoint_url: str
    access_key: str
    secret_key: str
    bucket_name: str

    def get_client(self):
        """Crée et retourne un client boto3 S3."""
        return boto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        )

    def upload_json(self, key: str, data: Union[dict, bytes]) -> str:
        """Téléverse un objet JSON ou des octets vers MinIO."""
        client = self.get_client()
        
        if isinstance(data, dict):
            body = json.dumps(data)
        else:
            body = data 

        client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=body,
            ContentType='application/json'
        )
        return f"s3a://{self.bucket_name}/{key}"

    def read_json(self, key: str) -> dict:
        """Lit un objet JSON depuis MinIO."""
        client = self.get_client()
        response = client.get_object(Bucket=self.bucket_name, Key=key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)