#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
import os

# README.md को लॉन्ग डिस्क्रिप्शन के तौर पर पढ़ें
try:
    with open("README.md", "r", encoding="utf-8") as fh:
        long_description = fh.read()
except FileNotFoundError:
    long_description = "Telegram CLI - Advanced Multi-Account File Manager"

# requirements.txt से डिपेंडेंसी लोड करें
def load_requirements():
    requirements = []
    try:
        with open("requirements.txt", "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and not line.startswith("-"):
                    requirements.append(line)
    except FileNotFoundError:
        # अगर requirements.txt न मिले तो डिफॉल्ट डिपेंडेंसी
        requirements = [
            "telethon>=1.34.0,<2.0.0",
            "click>=8.1.0,<9.0.0",
            "cryptography>=41.0.0,<42.0.0",
            "pyyaml>=6.0,<7.0",
            "aiofiles>=23.0.0,<24.0.0",
            "tqdm>=4.65.0,<5.0.0",
            "sqlalchemy>=2.0.0,<3.0.0",
            "humanize>=4.8.0,<5.0.0",
            "croniter>=2.0.0,<3.0.0",
            "python-dotenv>=1.0.0,<2.0.0",
        ]
    return requirements

setup(
    name="telegram-cli",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="Advanced Telegram File Manager with Multi-Account, Parallel Upload/Download, Encryption, and SQLite Tracking",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/telegram-cli",
    packages=find_packages(include=["telegram_cli", "telegram_cli.*"]),
    include_package_data=True,
    install_requires=load_requirements(),
    entry_points={
        "console_scripts": [
            "tg=telegram_cli.cli:main",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Operating System :: POSIX :: Linux",
        "Environment :: Console",
        "License :: OSI Approved :: MIT License",
        "Topic :: Communications :: File Sharing",
        "Topic :: Utilities",
    ],
    python_requires=">=3.8",
    zip_safe=False,
)