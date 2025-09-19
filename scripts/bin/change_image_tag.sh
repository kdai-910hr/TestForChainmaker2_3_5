#!/bin/bash

INPUT_TAG=$1
DEFAULT_TAG="chainmakerofficial/chainmaker-vm-engine:v2.3.5"

if [ -z "$INPUT_TAG" ]; then
  echo "[INFO] 未提供参数，跳过 VM_GO_IMAGE_NAME 修改。"
  echo "用法：./changes_images_name.sh <new_image_tag>"
  echo "示例：./changes_images_name.sh chainmakerofficial/chainmaker-vm-engine:v2.3.5"
  echo "特殊用法：./changes_images_name.sh default     # 使用默认镜像 $DEFAULT_TAG"
  exit 0
fi

if [ "$INPUT_TAG" = "default" ]; then
  NEW_TAG="$DEFAULT_TAG"
else
  NEW_TAG="$INPUT_TAG"
fi

echo "[INFO] 修改 start.sh 中的 VM_GO_IMAGE_NAME 为: $NEW_TAG"

if sed --version >/dev/null 2>&1; then
  # GNU sed (Linux)
  sed -i "s#^\(VM_GO_IMAGE_NAME=\).*#\1\"$NEW_TAG\"#" start.sh
else
  # BSD sed (macOS)
  sed -i "" "s#^\(VM_GO_IMAGE_NAME=\).*#\1\"$NEW_TAG\"#" start.sh
fi

echo "[INFO] 修改完成"
