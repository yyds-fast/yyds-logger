#!/usr/bin/env python
# -*- coding:utf-8 -*-

# Author: gm.zhibo.wang
# E-mail: gm.zhibo.wang@gmail.com
# Date  :
# Desc  :

from setuptools import setup, find_packages
from codecs import open
import os


about = {}
here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, "yyds_logger", "__version__.py"), "r", "utf-8") as f:
    exec(f.read(), about)

with open(os.path.join(here, "README.md"), "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name=about["__title__"],
    version=about["__version__"],
    author=about["__author__"],
    author_email=about["__author_email__"],
    description=about["__description__"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    url=about["__url__"],
    license=about.get("__license__", "MIT"),
    packages=find_packages(),
    include_package_data=True,
    python_requires='>=3.8',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
    ],
    install_requires=[
    ],
    extras_require={
        "test": [
            "pytest>=8",
            "pytest-cov>=5",
        ],
        "lint": [
            "ruff>=0.6",
            "mypy>=1.10",
        ],
        # 行级耗时分析（time_it(line_by_line=True) 更精确，缺失时自动回退到内置 tracer）
        "profile": [
            "line_profiler>=5.0.2",
        ],
        # 系统监控与健康检查
        "monitoring": [
            "psutil>=7.2.2",
        ],
        # 一次性安装全部可选依赖
        "all": [
            "line_profiler>=5.0.2",
            "psutil>=7.2.2",
        ],
    },
    project_urls={
        "Bug Reports": "https://github.com/yyds-fast/yyds_logger/issues",
        "Source": "https://github.com/yyds-fast/yyds_logger",
    },
)
