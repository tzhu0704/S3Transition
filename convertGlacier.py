import boto3
import time
import os
from botocore.exceptions import ClientError
import configparser
from enum import Enum
import logging
from datetime import datetime

class StorageClass(Enum):
    GLACIER = "GLACIER"
    GLACIER_IR = "GLACIER_IR"

class StorageConfig:
    def __init__(self, storage_class, convert_to_intelligent=True):
        self.storage_class = storage_class
        self.convert_to_intelligent = convert_to_intelligent

class ConversionStats:
    def __init__(self):
        self.found = 0
        self.converted = 0
        self.failed = 0
        self.skipped = 0

def setup_logging():
    """Setup logging configuration"""
    # Create logs directory if it doesn't exist
    if not os.path.exists('logs'):
        os.makedirs('logs')

    # Create a timestamp for the log file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f'logs/conversion_{timestamp}.log'

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def load_config():
    config_file='config/s3.properties'
    config = configparser.ConfigParser()
    config.read(config_file)
    
    return {
        'access_key': config.get('DEFAULT', 'aws.access_key'),
        'secret_key': config.get('DEFAULT', 'aws.secret_key'),
        'region': config.get('DEFAULT', 'aws.region'),
        'bucket_name': config.get('DEFAULT', 'aws.bucket_name'),
        'prefix_path': config.get('DEFAULT', 'aws.prefix_path'),
        'storageclass': config.get('DEFAULT', 'aws.storageclass')
    }

def get_s3_client():
    """Initialize and return S3 client with credentials"""
    try:
        config=load_config()
        s3_client = boto3.client(
            's3',
            aws_access_key_id=config['access_key'],
            aws_secret_access_key=config['secret_key'],
            region_name=config['region']
        )
        return s3_client
    except Exception as e:
        raise Exception(f"Failed to initialize S3 client: {str(e)}")

def process_glacier_objects(logger, bucket_name, storage_configs, prefix=''):
    """Process objects based on their storage class"""
    s3_client = get_s3_client()
    paginator = s3_client.get_paginator('list_objects_v2')
   
    # Initialize statistics for each storage class
    stats = {config.storage_class: ConversionStats() for config in storage_configs}
    objects_to_process = {config.storage_class: [] for config in storage_configs}
    logger.info("...........gprocess1..."+bucket_name+"..."+prefix) 
    try:
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
          
            logger.info(".....33..")
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
              
                if 'StorageClass' not in obj:
                    continue
                
                storage_class = obj['StorageClass']
                key = obj['Key']
              
                for config in storage_configs:
                    if storage_class == config.storage_class.value:
                        stats[config.storage_class].found += 1
                        objects_to_process[config.storage_class].append({
                            'key': key,
                            'convert_to_intelligent': config.convert_to_intelligent
                        })
                        logger.info(f"Found {config.storage_class.value} object: {key}")
                        break
        
        # Log summary of found objects
        for storage_class, stat in stats.items():
            logger.info(f"Found {stat.found} objects in {storage_class.value}")
        
        return objects_to_process, stats
                        
    except Exception as e:
        logger.error(f"Error listing objects: {str(e)}")
        return {}, stats

def initiate_restore_for_glacier_objects(logger, bucket_name, objects):
    """Initiate restore requests for Glacier objects"""
    s3_client = get_s3_client()
    objects_to_restore = []
    
    for obj in objects:
        key = obj['key']
        try:
            s3_client.restore_object(
                Bucket=bucket_name,
                Key=key,
                RestoreRequest={
                    'Days': 10,
                    'GlacierJobParameters': {
                        'Tier': 'Bulk'
                    }
                }
            )
            objects_to_restore.append(obj)
            logger.info(f"Initiated restore for: {key}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'RestoreAlreadyInProgress':
                logger.info(f"Restore already in progress for: {key}")
                objects_to_restore.append(obj)
            else:
                logger.error(f"Error initiating restore for {key}: {str(e)}")
                
    return objects_to_restore

def check_restore_status(logger, bucket_name, objects):
    """Check restore status for objects"""
    s3_client = get_s3_client()
    restored_objects = []
    
    for obj in objects:
        key = obj['key']
        try:
            response = s3_client.head_object(Bucket=bucket_name, Key=key)
            if 'Restore' in response and 'ongoing-request="false"' in response['Restore']:
                restored_objects.append(obj)
                logger.info(f"Restore completed for: {key}")
        except Exception as e:
            logger.error(f"Error checking restore status for {key}: {str(e)}")
            
    return restored_objects

def convert_to_intelligent_tiering(logger, bucket_name, objects, stats):
    """Convert objects to Intelligent-Tiering"""
    s3_client = get_s3_client()
    
    for obj in objects:
        if not obj['convert_to_intelligent']:
            stats.skipped += 1
            logger.info(f"Skipping conversion for: {obj['key']}")
            continue
            
        key = obj['key']
        try:
            s3_client.copy_object(
                Bucket=bucket_name,
                Key=key,
                CopySource={'Bucket': bucket_name, 'Key': key},
                StorageClass='INTELLIGENT_TIERING',
                MetadataDirective='COPY'
            )
            stats.converted += 1
            logger.info(f"Converted to Intelligent-Tiering: {key}")
        except Exception as e:
            stats.failed += 1
            logger.error(f"Error converting {key}: {str(e)}")

def create_storage_configs(config_string):
    storage_configs = []
    parts = config_string.split("#")
    
    for part in parts:
        part = part.strip()
        try:
            storage_class = StorageClass[part]
            config = StorageConfig(storage_class, convert_to_intelligent=True)
            storage_configs.append(config)
            print(f"Added config for: {storage_class.value}")
        except KeyError:
            print(f"Skipping invalid storage class: {part}")
    
    return storage_configs

def main():
    # Setup logging
    logger = setup_logging()
    logger.info("Starting conversion process")

    # Load configuration
    config = load_config()
    bucket_name = config['bucket_name']
    prefix = config['prefix_path']
    check_interval = 3600
    storageclass = config['storageclass']

    # Configure storage classes
    storage_configs = create_storage_configs(storageclass)
    if not storage_configs:
        logger.error("No valid storage configurations found")
        return

    try:
        logger.info("Starting object processing...")
        logger.info("Starting object processing111...") 
        objects_by_storage_class, stats = process_glacier_objects(logger, bucket_name, storage_configs, prefix)
        
        # Process Glacier IR objects (direct conversion)
        glacier_ir_objects = objects_by_storage_class.get(StorageClass.GLACIER_IR, [])
        if glacier_ir_objects:
            logger.info("\nProcessing Glacier IR objects...")
            convert_to_intelligent_tiering(logger, bucket_name, glacier_ir_objects, 
                                        stats[StorageClass.GLACIER_IR])
        
        # Process Glacier objects (requires restore)
        glacier_objects = objects_by_storage_class.get(StorageClass.GLACIER, [])
        if glacier_objects:
            logger.info("\nProcessing Glacier objects...")
            objects_to_restore = initiate_restore_for_glacier_objects(logger, bucket_name, glacier_objects)
            
            while objects_to_restore:
                logger.info(f"\nChecking restore status for {len(objects_to_restore)} objects...")
                restored_objects = check_restore_status(logger, bucket_name, objects_to_restore)
                
                objects_to_restore = [obj for obj in objects_to_restore if obj not in restored_objects]
                
                if objects_to_restore:
                    logger.info(f"Waiting for {len(objects_to_restore)} objects to complete restore...")
                    time.sleep(check_interval)
                
                if restored_objects:
                    convert_to_intelligent_tiering(logger, bucket_name, restored_objects, 
                                                stats[StorageClass.GLACIER])
        
        # Log final statistics
        logger.info("\nConversion Statistics:")
        for storage_class, stat in stats.items():
            logger.info(f"\n{storage_class.value} Statistics:")
            logger.info(f"Total objects found: {stat.found}")
            logger.info(f"Successfully converted: {stat.converted}")
            logger.info(f"Failed conversions: {stat.failed}")
            logger.info(f"Skipped conversions: {stat.skipped}")
        
        logger.info("\nProcess completed!")
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")

if __name__ == "__main__":
    main()
