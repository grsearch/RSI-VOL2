#!/bin/bash
set -e

echo "=== SOL RSI Monitor 部署 ==="

# Node.js 检查
if ! command -v node &>/dev/null; then
  echo "安装 Node.js 20..."
  curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
  sudo apt-get install -y nodejs
fi
echo "Node $(node -v) / npm $(npm -v)"

# 依赖安装
npm install --omit=dev

# 日志目录
mkdir -p logs

# .env 检查
if [ ! -f .env ]; then
  cp .env.example .env
  echo ""
  echo "⚠️  请先填写 .env 配置后再启动："
  echo "   nano .env"
  exit 0
fi

# systemd 服务
SERVICE_NAME="sol-rsi-monitor"
WORK_DIR=$(pwd)
NODE_BIN=$(which node)

sudo tee /etc/systemd/system/${SERVICE_NAME}.service > /dev/null <<EOF
[Unit]
Description=SOL RSI Monitor
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=${WORK_DIR}
ExecStart=${NODE_BIN} src/index.js
Restart=always
RestartSec=5
Environment=NODE_ENV=production

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable  ${SERVICE_NAME}
sudo systemctl restart ${SERVICE_NAME}

echo ""
echo "✅ 部署完成！"
echo "   状态: sudo systemctl status ${SERVICE_NAME}"
echo "   日志: journalctl -u ${SERVICE_NAME} -f"
echo "   面板: http://$(curl -s ifconfig.me 2>/dev/null || echo YOUR_IP):${PORT:-3001}"
