#!/bin/bash

# MySQLのDockerコンテナの設定
CONTAINER_NAME="scalardb-mysql"
MYSQL_ROOT_PASSWORD="mysql"
MYSQL_PORT="3306"
MYSQL_DATABASE="scalardb"

echo "MySQLのDockerコンテナを起動します..."

# すでに同じ名前のコンテナが実行中か確認
RUNNING=$(docker ps -q -f name=${CONTAINER_NAME})
if [ ! -z "$RUNNING" ]; then
    echo "コンテナ '${CONTAINER_NAME}' は既に実行中です。"
    echo "既存のコンテナを使用します。"
else
    # 停止中のコンテナが存在するか確認
    EXISTS=$(docker ps -aq -f status=exited -f name=${CONTAINER_NAME})
    if [ ! -z "$EXISTS" ]; then
        echo "停止中のコンテナを再起動します..."
        docker start ${CONTAINER_NAME}
    else
        # 新しくコンテナを作成して起動
        echo "MySQLのコンテナを新規作成して起動します..."
        docker run --name ${CONTAINER_NAME} \
            -e MYSQL_ROOT_PASSWORD=${MYSQL_ROOT_PASSWORD} \
            -e MYSQL_DATABASE=${MYSQL_DATABASE} \
            -p ${MYSQL_PORT}:3306 \
            -d mysql:8.0
    fi
fi

# コンテナが起動するまで待機
echo "MySQLの起動を待機中..."
sleep 5

# 接続テスト（mysql CLIが必要）
echo "接続テストを実行中..."
MAX_TRIES=10
TRIES=0
while true; do
    docker exec ${CONTAINER_NAME} mysql -u root -p${MYSQL_ROOT_PASSWORD} -e "SELECT 1" >/dev/null 2>&1
    if [ $? -eq 0 ]; then
        break
    fi
    TRIES=$((TRIES + 1))
    if [ $TRIES -ge $MAX_TRIES ]; then
        echo "MySQLへの接続に失敗しました。"
        exit 1
    fi
    echo "MySQLが起動中です。再試行します..."
    sleep 3
done

echo "MySQL接続確認完了!"
echo "MySQLサーバーが起動しました。"
echo "-----------------------------------"
echo "ホスト: localhost"
echo "ポート: ${MYSQL_PORT}"
echo "ユーザー名: root"
echo "パスワード: ${MYSQL_ROOT_PASSWORD}"
echo "データベース: ${MYSQL_DATABASE}"
echo "-----------------------------------"
echo ""
echo "ScalarDBの接続設定は既に scalardb.properties ファイルに設定済みです。"
echo "必要に応じて以下のコマンドでスキーマをロードしてください："
echo "java -jar scalardb-schema-loader-<VERSION>.jar --config scalardb.properties -f ycsb-schema.json --coordinator"
