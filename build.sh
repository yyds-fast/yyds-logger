#!/usr/bin/env bash

set -euo pipefail

python -m pytest --cov=yyds_logger --cov-report=term-missing
ruff check .
mypy yyds_logger

# 清理旧的构建产物，防止重复上传引发 PyPI 400 报错
rm -rf build dist yyds_logger.egg-info

python -m build
python -m twine check dist/*

python -m twine upload dist/*
