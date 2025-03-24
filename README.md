# S3Transition 支持转换Glacier以及GIR层到 Intelligent Tier
通过s3.properties 定义目标桶
aws.access_key=AAAA333333
aws.secret_key=BBBBB33333
aws.region=us-east-1
aws.bucket_name=tzhubucket3
aws.prefix_path= # 针对前缀的过滤
aws.storageclass=GLACIER#GLACIER_IR # GLACIER表示归档层，GIR表示Instant Retrieval层
