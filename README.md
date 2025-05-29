# DynamoDB PITR バックアップエクスポート

このLambda関数は、DynamoDBのPITR（Point-in-Time Recovery）を使用して30日前のバックアップをS3に自動エクスポートします。

## 前提条件

- DynamoDBテーブルでPITRが有効になっていること
- 適切なIAMロールが設定されていること
- S3バケットが存在すること

## 設定方法

1. Lambda関数の環境変数を設定：
   - `TABLE_NAME`: DynamoDBのテーブル名
   - `BUCKET_NAME`: S3バケット名
   - `PREFIX`: S3のプレフィックス（デフォルト: 'dynamodb-backups/'）

2. IAMロールに以下の権限を追加：
   ```json
   {
       "Version": "2012-10-17",
       "Statement": [
           {
               "Effect": "Allow",
               "Action": [
                   "dynamodb:ExportTableToPointInTime",
                   "dynamodb:DescribeExport"
               ],
               "Resource": "arn:aws:dynamodb:*:*:table/*"
           },
           {
               "Effect": "Allow",
               "Action": [
                   "s3:PutObject",
                   "s3:GetObject",
                   "s3:ListBucket"
               ],
               "Resource": [
                   "arn:aws:s3:::your-bucket-name",
                   "arn:aws:s3:::your-bucket-name/*"
               ]
           }
       ]
   }
   ```

## 使用方法

1. Lambda関数をデプロイ
2. EventBridge（CloudWatch Events）でスケジュールを設定（例：毎日実行）
3. エクスポートされたデータは指定したS3バケットのプレフィックス配下に保存されます

## 注意事項

- PITRが有効になっているDynamoDBテーブルのみ使用可能です
- エクスポートには時間がかかる場合があります
- 大量のデータがある場合は、Lambda関数のタイムアウト設定を適切に調整してください
