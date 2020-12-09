import multiprocessing
import os

from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults
from botocore.exceptions import ClientError


class S3ToS3TransferOperator(BaseOperator):
    template_fields = ("src_s3_bucket", "src_s3_prefix", "dst_s3_bucket", "dst_s3_prefix")

    @apply_defaults
    def __init__(
        self,
        src_s3_bucket: str,
        src_s3_prefix: str,
        dst_s3_bucket: str,
        dst_s3_prefix: str,
        aws_conn_id="aws_default",
        clear_option="all",
        *args,
        **kwargs,
    ):
        """
        Copies all files within the bucket prefix to the destination bucket/prefix/...

        :param src_s3_bucket: Source bucket to copy files from
        :type str
        :param src_s3_prefix: Prefix to filter objects for
        :type str
        :param dst_s3_bucket: Destination bucet to move files to
        :type str
        :param dst_s3_prefix: Destination s3 prefix to write to
        :type str
        :param aws_conn_id: AWS connection ID default 'aws_default'
        :type str
        :param clear_option: Operations to perform on existing objes in target bucket.
            (default "all")
            When "all" is set, all objects are deleted in the prefix
            When "existing" is set, deletes objects with matching keys
            When None is set, objects are just copied.
        :type
            Union[str,None]
        """
        super(S3ToS3TransferOperator, self).__init__(*args, **kwargs)
        self.src_s3_bucket = src_s3_bucket
        self.src_s3_prefix = src_s3_prefix
        self.dst_s3_bucket = dst_s3_bucket
        self.dst_s3_prefix = dst_s3_prefix
        self.aws_conn_id = aws_conn_id
        self.clear_option = clear_option
        self.hook = S3Hook(aws_conn_id)

    def _get_object_keys(self):
        if self.hook.check_for_prefix(
            bucket_name=self.src_s3_bucket, prefix=self.src_s3_prefix, delimiter="/"
        ):
            keys = self.hook.list_keys(bucket_name=self.src_s3_bucket, prefix=self.src_s3_prefix,)
            return keys
        else:
            raise AirflowException(f"Could not find prefix {self.src_s3_prefix}")

    def _copy_one(self, object_key):
        try:
            if self.clear_option:
                if self.clear_option == "all":
                    self.logger.info(
                        msg=f"Deleting objects in {self.dst_s3_bucket}/{self.dst_s3_prefix}"
                    )
                    self.hook.delete_objects(bucket=self.dst_s3_bucket, keys=self.dst_s3_prefix)
                elif self.clear_option == "existing":
                    self.logger.info(msg=f"Deleting file {self.dst_s3_bucket}/{object_key}")
                    if self.hook.check_for_key(object_key, bucket_name=self.dst_s3_bucket):
                        self.hook.delete_objects(bucket=self.dst_s3_bucket, keys=self.dst_s3_prefix)
                    else:
                        self.logger.info(
                            f"No {object_key} in {self.dst_s3_bucket}/{self.dst_s3_prefix}"
                        )
                else:
                    raise ValueError(
                        f"clear_option expected ('all','existing',None), got {self.clear_option}"
                    )
            self.logger.info(
                msg=f"Copying {object_key} from {self.src_s3_bucket} "
                f"to {self.dst_s3_bucket}/{self.dst_s3_prefix}"
            )
            dst_obj_key = object_key.split(self.src_s3_prefix + "/")[-1]
            if len(dst_obj_key) > 1:
                self.hook.copy_object(
                    source_bucket_name=self.src_s3_bucket,
                    dest_bucket_name=self.dst_s3_bucket,
                    source_bucket_key=object_key,
                    dest_bucket_key=os.path.join(self.dst_s3_prefix, dst_obj_key),
                )
        except ClientError:
            raise AirflowException(
                f"Error copying object {object_key} to {self.dst_s3_bucket}/{self.dst_s3_prefix}"
            )

    def copy_objects(self, keys):
        with multiprocessing.Pool() as pool:
            pool.map(self._copy_one, keys)
        return

    def execute(self, **context):
        try:
            self.logger.info(
                f"Copying objects From {self.src_s3_bucket}/{self.src_s3_prefix} "
                f"to {self.dst_s3_bucket}/{self.dst_s3_prefix}"
            )
            obj_keys = self._get_object_keys()
            self.copy_objects(obj_keys)

        except ClientError as e:
            raise AirflowException(f"Error in transferring objects due to {e}")
