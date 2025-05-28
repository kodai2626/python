import boto3
import os
import time
import logging
from datetime import datetime, timedelta, timezone
from botocore.exceptions import ClientError

# ロガーの設定
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWSサービスクライアントの初期化
dynamodb_client = boto3.client('dynamodb')
s3_client = boto3.client('s3')

# 環境変数から設定値を取得
SOURCE_TABLE_NAME = os.environ['SOURCE_TABLE_NAME']
S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
MAX_RETRIES = 3
WAIT_TIME = 30  # 秒

def retry_with_backoff(func, *args, **kwargs):
    """
    指数バックオフを使用したリトライロジック
    
    Args:
        func: 実行する関数
        *args: 関数の引数
        **kwargs: 関数のキーワード引数
    
    Returns:
        関数の実行結果
    """
    for attempt in range(MAX_RETRIES):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt == MAX_RETRIES - 1:  # 最後の試行で失敗した場合
                raise
            wait_time = min(WAIT_TIME * (2 ** attempt), 300)  # 最大5分まで
            logger.info(f"[TEST] Retry attempt {attempt + 1}/{MAX_RETRIES} after {wait_time} seconds...")
            time.sleep(wait_time)

def wait_for_table_status(table_name, expected_status):
    """
    テーブルのステータスが期待値になるまで待機する関数（検証用）
    
    Args:
        table_name (str): 監視対象のテーブル名
        expected_status (str): 期待するステータス（'ACTIVE'や'DELETED'など）
    
    Returns:
        dict: テーブルの詳細情報
    """
    while True:
        try:
            response = dynamodb_client.describe_table(TableName=table_name)
            status = response['Table']['TableStatus']
            logger.info(f"[TEST] Table {table_name} status: {status}")
            
            if status == expected_status:
                return response
            elif status in ['CREATING', 'RESTORING']:
                logger.info(f"[TEST] Waiting for table {table_name} to become {expected_status}...")
                time.sleep(WAIT_TIME)
            else:
                raise Exception(f"Unexpected table status: {status}")
        except dynamodb_client.exceptions.ResourceNotFoundException:
            if expected_status == 'DELETED':
                return None
            logger.info(f"[TEST] Table {table_name} not found yet, waiting...")
            time.sleep(WAIT_TIME)
        except Exception as e:
            logger.error(f"[TEST] Error describing table {table_name}: {e}")
            raise

def wait_for_export_completion(export_arn):
    """
    エクスポートジョブの完了を待機する関数（検証用）
    
    Args:
        export_arn (str): エクスポートジョブのARN
    
    Returns:
        dict: エクスポートジョブの詳細情報
    """
    while True:
        try:
            export_desc = dynamodb_client.describe_export(ExportArn=export_arn)
            status = export_desc['ExportDescription']['ExportStatus']
            logger.info(f"[TEST] Export status: {status}")
            
            if status == 'COMPLETED':
                return export_desc
            elif status == 'IN_PROGRESS':
                logger.info(f"[TEST] Export in progress, waiting...")
                time.sleep(WAIT_TIME)
            elif status in ['FAILED', 'CANCELLED']:
                raise Exception(f"Export failed or cancelled: {export_desc['ExportDescription'].get('FailureMessage', 'No failure message')}")
            else:
                raise Exception(f"Unexpected export status: {status}")
        except Exception as e:
            logger.error(f"[TEST] Error checking export status: {e}")
            raise

def cleanup_temp_table(table_name):
    """
    一時テーブルを削除する関数（検証用）
    
    Args:
        table_name (str): 削除対象のテーブル名
    """
    try:
        dynamodb_client.describe_table(TableName=table_name)
        logger.info(f"[TEST] Attempting to delete temporary table {table_name}...")
        dynamodb_client.delete_table(TableName=table_name)
        wait_for_table_status(table_name, 'DELETED')
        logger.info(f"[TEST] Temporary table {table_name} deleted successfully.")
    except dynamodb_client.exceptions.ResourceNotFoundException:
        logger.info(f"[TEST] Table {table_name} does not exist, no cleanup needed.")
    except Exception as e:
        logger.error(f"[TEST] Error during cleanup of table {table_name}: {e}")
        raise

def lambda_handler(event, context):
    """
    検証用Lambda関数のメインハンドラー
    
    Args:
        event (dict): Lambda関数の入力イベント
        context (LambdaContext): Lambda関数のコンテキスト
    
    Returns:
        dict: 処理結果
    """
    try:
        # リストアポイントの計算（1時間前の時点）
        now = datetime.now(timezone.utc)
        restore_datetime = now - timedelta(hours=1)  # 1時間前に変更
        temp_table_name = f"{SOURCE_TABLE_NAME}-test-restored-{int(time.time())}"  # テスト用のプレフィックスを追加
        
        logger.info(f"[TEST] Target restore datetime: {restore_datetime}")
        logger.info(f"[TEST] Temporary table name: {temp_table_name}")

        # PITRを使用してテーブルをリストア
        logger.info(f"[TEST] Starting PITR restore for table {SOURCE_TABLE_NAME} to {temp_table_name}...")
        response_restore = retry_with_backoff(
            dynamodb_client.restore_table_to_point_in_time,
            SourceTableName=SOURCE_TABLE_NAME,
            TargetTableName=temp_table_name,
            RestoreDateTime=restore_datetime,
            UseLatestRestorableTime=False
        )
        logger.info(f"[TEST] Restore initiated: {response_restore}")

        # リストア完了を待機
        table_description = wait_for_table_status(temp_table_name, 'ACTIVE')
        logger.info(f"[TEST] Table {temp_table_name} restored successfully.")

        # S3へのエクスポート準備
        export_time = datetime.now(timezone.utc)
        s3_prefix = f"test/{temp_table_name}/{export_time.strftime('%Y/%m/%d/%H%M%S')}"  # テスト用のパスを追加
        logger.info(f"[TEST] Starting export to s3://{S3_BUCKET_NAME}/{s3_prefix}...")

        # テーブルをS3にエクスポート（PITRを使用しない方法）
        response_export = retry_with_backoff(
            dynamodb_client.export_table_to_s3,
            TableArn=table_description['Table']['TableArn'],
            S3Bucket=S3_BUCKET_NAME,
            S3Prefix=s3_prefix,
            ExportFormat='DYNAMODB_JSON'
        )
        export_arn = response_export['ExportDescription']['ExportArn']
        logger.info(f"[TEST] Export initiated: {export_arn}")

        # エクスポート完了を待機
        export_desc = wait_for_export_completion(export_arn)
        logger.info(f"[TEST] Export completed successfully. Manifest file: s3://{S3_BUCKET_NAME}/{s3_prefix}/AWSDynamoDB/{export_arn.split('/')[-1]}/manifest-summary.json")

        # 一時テーブルの削除
        cleanup_temp_table(temp_table_name)

        # 成功レスポンスを返却
        return {
            'statusCode': 200,
            'body': f"Test process completed successfully. Exported to s3://{S3_BUCKET_NAME}/{s3_prefix}"
        }

    except Exception as e:
        # エラー発生時の処理
        logger.error(f"[TEST] Error in lambda_handler: {e}")
        # エラー発生時も一時テーブルのクリーンアップを試行
        try:
            cleanup_temp_table(temp_table_name)
        except Exception as cleanup_e:
            logger.error(f"[TEST] Cleanup failed: {cleanup_e}")
        raise  # エラーを上位に伝播 