#!/bin/bash

# 阿里云盘Web管理界面启动脚本

echo "正在启动阿里云盘Web管理界面..."

# 检查是否已登录
if [ ! -f "$HOME/.config/aliyunpan/aliyunpan_config.json" ]; then
    echo "错误: 未找到配置文件，请先使用命令行登录"
    echo "运行命令: ./aliyunpan login"
    exit 1
fi

# 设置环境变量
export ALIYUNPAN_CONFIG_DIR="$HOME/.config/aliyunpan"

# 切换到项目根目录
cd "$(dirname "$0")/.."

# 运行Web服务器
echo "启动Web服务器..."
go run web/main.go 