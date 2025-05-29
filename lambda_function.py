import boto3
import os
import logging
from datetime import datetime, timedelta
import json
from dateutil import tz

# ロガーの設定
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 環境変数から設定を取得
TABLE_NAME = os.environ.get('TABLE_NAME')
BUCKET_NAME = os.environ.get('BUCKET_NAME')

# 必須の環境変数チェック
if not TABLE_NAME or not BUCKET_NAME:
    raise ValueError("環境変数 TABLE_NAME と BUCKET_NAME は必須です")

# AWSクライアントの初期化
dynamodb = boto3.client('dynamodb')
s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # 1時間前の日時を計算（JST）
        jst = tz.gettz('Asia/Tokyo')
        now = datetime.now(jst)
        one_hour_ago = now - timedelta(hours=1)
        
        # タイムスタンプをISO形式に変換
        export_time = one_hour_ago.isoformat()
        
        logger.info(f"1時間前の時点（{export_time}）のバックアップをエクスポート開始: テーブル {TABLE_NAME}")
        
        # DynamoDBのエクスポートを開始
        response = dynamodb.export_table_to_point_in_time(
            TableArn=f'arn:aws:dynamodb:{boto3.session.Session().region_name}:{boto3.client("sts").get_caller_identity()["Account"]}:table/{TABLE_NAME}',
            S3Bucket=BUCKET_NAME,
            ExportTime=export_time,
            ExportFormat='DYNAMODB_JSON'
        )
        
        # エクスポートの状態を確認
        export_arn = response['ExportDescription']['ExportArn']
        logger.info(f"エクスポート開始成功。Export ARN: {export_arn}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': '1時間前の時点のバックアップエクスポートを開始しました',
                'exportArn': export_arn,
                'exportTime': export_time,
                's3Location': f's3://{BUCKET_NAME}'
            })
        }
        
    except Exception as e:
        error_message = f"エクスポート中にエラーが発生しました: {str(e)}"
        logger.error(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': error_message
            })
        } 