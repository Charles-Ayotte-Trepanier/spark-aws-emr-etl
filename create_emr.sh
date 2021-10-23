sudo aws emr create-cluster \
--name super_cluster_24 \
--use-default-roles \
--release-label emr-5.28.0 \
--instance-count 3 \
--applications Name=Spark  Name=Zeppelin Name=Livy Name=Hadoop Name=JupyterHub Name=Hive \
--ec2-attributes KeyName=emr_exercise,SubnetId=subnet-0c34e4a63c3630654 \
--instance-type m5.xlarge \
--profile new_name \
--region us-east-1