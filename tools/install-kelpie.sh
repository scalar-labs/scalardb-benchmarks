#!/bin/bash

# Kelpieのバージョンを指定（最新バージョンまたは特定のバージョンに設定）
KELPIE_VERSION="1.2.4"
KELPIE_DOWNLOAD_URL="https://github.com/scalar-labs/kelpie/releases/download/${KELPIE_VERSION}/kelpie-${KELPIE_VERSION}.zip"
INSTALL_DIR="./kelpie"

echo "Kelpie v${KELPIE_VERSION}をインストールします..."

# インストールディレクトリの作成
mkdir -p ${INSTALL_DIR}

# Kelpieのダウンロード
echo "Kelpieをダウンロード中: ${KELPIE_DOWNLOAD_URL}"
curl -L ${KELPIE_DOWNLOAD_URL} -o kelpie.zip

# 解凍
echo "ファイルを解凍中..."
if [ "$(uname)" == "Darwin" ]; then
    # macOSの場合
    unzip -q kelpie.zip
else
    # Linuxの場合
    unzip -q kelpie.zip
fi

# 解凍したファイルをINSTALL_DIRに移動
mv kelpie-${KELPIE_VERSION}/* ${INSTALL_DIR}/
rmdir kelpie-${KELPIE_VERSION}

# ダウンロードしたアーカイブの削除
rm kelpie.zip

# 実行権限の付与
chmod +x ${INSTALL_DIR}/bin/kelpie

echo "Kelpie v${KELPIE_VERSION}のインストールが完了しました！"
echo "以下のコマンドでベンチマークを実行できます："
echo "./kelpie/bin/kelpie --config ycsb-multi-user-benchmark-config.toml"
