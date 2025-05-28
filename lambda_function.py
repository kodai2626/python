import boto3
import os
import time
import logging
from datetime import datetime, timedelta, timezone
from botocore.exceptions import ClientError
from tenacity import retry, stop_after_attempt, wait_exponential

# ロガーの設定
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client('dynamodb')
s3_client = boto3.client('s3')

SOURCE_TABLE_NAME = os.environ['SOURCE_TABLE_NAME']
S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
S3_PREFIX = os.environ.get('S3_PREFIX', '')
MAX_RETRIES = 3
WAIT_TIME = 30  # 秒

@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=1, min=4, max=10))
def wait_for_table_status(table_name, expected_status):
    """
    テーブルのステータスが期待値になるまで待機する関数
    
    Args:
        table_name (str): 監視対象のテーブル名
        expected_status (str): 期待するステータス（'ACTIVE'や'DELETED'など）
    
    Returns:
        dict: テーブルの詳細情報
    """
    while True:
        try:
            # テーブルの現在のステータスを取得
            response = dynamodb_client.describe_table(TableName=table_name)
            status = response['Table']['TableStatus']
            logger.info(f"Table {table_name} status: {status}")
            
            if status == expected_status:
                return response
            elif status in ['CREATING', 'RESTORING']:
                time.sleep(WAIT_TIME)  # 処理中は待機
            else:
                raise Exception(f"Unexpected table status: {status}")
        except dynamodb_client.exceptions.ResourceNotFoundException:
            if expected_status == 'DELETED':
                return None
            logger.info(f"Table {table_name} not found yet, waiting...")
            time.sleep(WAIT_TIME)
        except Exception as e:
            logger.error(f"Error describing table {table_name}: {e}")
            raise

@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=1, min=4, max=10))
def wait_for_export_completion(export_arn):
    """
    エクスポートジョブの完了を待機する関数
    
    Args:
        export_arn (str): エクスポートジョブのARN
    
    Returns:
        dict: エクスポートジョブの詳細情報
    """
    while True:
        try:
            # エクスポートジョブの現在のステータスを取得
            export_desc = dynamodb_client.describe_export(ExportArn=export_arn)
            status = export_desc['ExportDescription']['ExportStatus']
            logger.info(f"Export status: {status}")
            
            if status == 'COMPLETED':
                return export_desc
            elif status == 'IN_PROGRESS':
                time.sleep(WAIT_TIME)  # 処理中は待機
            elif status in ['FAILED', 'CANCELLED']:
                raise Exception(f"Export failed or cancelled: {export_desc['ExportDescription'].get('FailureMessage', 'No failure message')}")
            else:
                raise Exception(f"Unexpected export status: {status}")
        except Exception as e:
            logger.error(f"Error checking export status: {e}")
            raise

def cleanup_temp_table(table_name):
    """
    一時テーブルを削除する関数
    
    Args:
        table_name (str): 削除対象のテーブル名
    """
    try:
        # テーブルの存在確認
        dynamodb_client.describe_table(TableName=table_name)
        logger.info(f"Attempting to delete temporary table {table_name}...")
        # テーブルの削除
        dynamodb_client.delete_table(TableName=table_name)
        # 削除完了を待機
        wait_for_table_status(table_name, 'DELETED')
        logger.info(f"Temporary table {table_name} deleted successfully.")
    except dynamodb_client.exceptions.ResourceNotFoundException:
        logger.info(f"Table {table_name} does not exist, no cleanup needed.")
    except Exception as e:
        logger.error(f"Error during cleanup of table {table_name}: {e}")
        raise

def lambda_handler(event, context):
    """
    Lambda関数のメインハンドラー
    
    Args:
        event (dict): Lambda関数の入力イベント
        context (LambdaContext): Lambda関数のコンテキスト
    
    Returns:
        dict: 処理結果
    """
    try:
        # リストアポイントの計算（30日前の時点）
        now = datetime.now(timezone.utc)
        restore_datetime = now - timedelta(days=30)
        # 一時テーブル名の生成（タイムスタンプ付き）
        temp_table_name = f"{SOURCE_TABLE_NAME}-restored-{int(time.time())}"
        
        logger.info(f"Target restore datetime: {restore_datetime}")
        logger.info(f"Temporary table name: {temp_table_name}")

        # PITRを使用してテーブルをリストア
        logger.info(f"Starting PITR restore for table {SOURCE_TABLE_NAME} to {temp_table_name}...")
        response_restore = dynamodb_client.restore_table_to_point_in_time(
            SourceTableName=SOURCE_TABLE_NAME,
            TargetTableName=temp_table_name,
            RestoreDateTime=restore_datetime,
            UseLatestRestorableTime=False
        )
        logger.info(f"Restore initiated: {response_restore}")

        # リストア完了を待機
        table_description = wait_for_table_status(temp_table_name, 'ACTIVE')
        logger.info(f"Table {temp_table_name} restored successfully.")

        # S3へのエクスポート準備
        export_time = datetime.now(timezone.utc)
        # S3のパスを生成（年/月/日/時分秒の形式）
        s3_full_prefix = os.path.join(S3_PREFIX, temp_table_name, export_time.strftime('%Y/%m/%d/%H%M%S'))
        logger.info(f"Starting export to s3://{S3_BUCKET_NAME}/{s3_full_prefix}...")

        # テーブルをS3にエクスポート
        response_export = dynamodb_client.export_table_to_point_in_time(
            TableArn=table_description['Table']['TableArn'],
            S3Bucket=S3_BUCKET_NAME,
            S3Prefix=s3_full_prefix,
            ExportFormat='DYNAMODB_JSON'
        )
        export_arn = response_export['ExportDescription']['ExportArn']
        logger.info(f"Export initiated: {export_arn}")

        # エクスポート完了を待機
        export_desc = wait_for_export_completion(export_arn)
        logger.info(f"Export completed successfully. Manifest file: s3://{S3_BUCKET_NAME}/{s3_full_prefix}/AWSDynamoDB/{export_arn.split('/')[-1]}/manifest-summary.json")

        # 一時テーブルの削除
        cleanup_temp_table(temp_table_name)

        # 成功レスポンスを返却
        return {
            'statusCode': 200,
            'body': f"Process completed successfully. Exported to s3://{S3_BUCKET_NAME}/{s3_full_prefix}"
        }

    except Exception as e:
        # エラー発生時の処理
        logger.error(f"Error in lambda_handler: {e}")
        # エラー発生時も一時テーブルのクリーンアップを試行
        try:
            cleanup_temp_table(temp_table_name)
        except Exception as cleanup_e:
            logger.error(f"Cleanup failed: {cleanup_e}")
        raise  # エラーを上位に伝播 