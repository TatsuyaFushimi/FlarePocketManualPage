#!/bin/bash
set -e
cd "$(dirname "$0")"

# Anthropic API キーの確認
if [ -z "$ANTHROPIC_API_KEY" ]; then
  if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
  else
    echo "❌ ANTHROPIC_API_KEY が設定されていません"
    echo "   .env ファイルに ANTHROPIC_API_KEY=sk-ant-... を記載するか"
    echo "   export ANTHROPIC_API_KEY=sk-ant-... を実行してください"
    exit 1
  fi
fi

# 依存パッケージのインストール確認
if ! python3 -c "import fastapi" 2>/dev/null; then
  echo "📦 パッケージをインストール中..."
  pip3 install -r requirements_web.txt -q
fi

echo "🚀 FlarePocket 起動中..."
echo "   ブラウザで http://localhost:8000 を開いてください"
echo ""
python3 -m uvicorn web.main:app --host 0.0.0.0 --port 8000 --reload
