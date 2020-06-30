import boto3
import pickle


class S3Processor(object):
    """
    S3 Object Manager.

    ...
    """

    def __init__(
            self,
            endpoint_url,
            aws_access_key_id,
            aws_secret_access_key,
            bucket_name,

    ):
        """
        ...

        Args:
            endpoint_url:
            aws_access_key_id:
            aws_secret_access_key:
            bucket_name:
        """
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.bucket_name = bucket_name

    def _get_s3_resource(self):
        return boto3.resource(
            service_name='s3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )

    def get_list_keys(self):
        """
        ...

        Returns:

        """
        s3 = self._get_s3_resource()
        sample_db_bucket = s3.Bucket(self.bucket_name)
        return [s3_object.key for s3_object in sample_db_bucket.objects.all()]

    def save_object_by_key(self, key, object_to_save):
        """
        ...

        Args:
            key:
            object_to_save:

        Returns:

        """
        s3 = self._get_s3_resource()
        pickle_byte_obj = pickle.dumps(object_to_save)
        s3.Object(self.bucket_name, key).put(Body=pickle_byte_obj)

    def delete_object_by_key(self, key):
        """
        ...

        Args:
            key:

        Returns:

        """
        s3 = self._get_s3_resource()
        sample_db_bucket = s3.Bucket(self.bucket_name)
        obj_list = [
            s3_object for s3_object in sample_db_bucket.objects.filter(
                Prefix=key,
            ) if s3_object.key == key
        ]
        my_obj = obj_list[0]
        my_obj.delete()

    def get_object_by_key(self, key):
        """
        ...

        Args:
            key:

        Returns:

        """
        s3 = self._get_s3_resource()
        sample_db_bucket = s3.Bucket(self.bucket_name)
        object_list = [
            s3_object for s3_object in sample_db_bucket.objects.filter(
                Prefix=key,
            ) if s3_object.key == key
        ]
        selected_object = object_list[0]
        ld_pickle_object = selected_object.get()['Body'].read()
        return pickle.loads(ld_pickle_object)

