#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Telegram CLI – Advanced Multi‑Account File Manager
"""

__version__ = "0.1.0"
__author__ = "Your Name"
__license__ = "MIT"

# पैकेज के मुख्य घटकों को आसानी से इम्पोर्ट करने के लिए
from telegram_cli.cli import main as cli_main

# ये क्लासेस बाद में डायरेक्ट इम्पोर्ट करने के लिए उपलब्ध होंगी
# (जब तक मॉड्यूल बन न जाएं, कमेंटेड रखा है)
# from telegram_cli.core.account_manager import AccountManager
# from telegram_cli.core.uploader import Uploader
# from telegram_cli.core.downloader import Downloader
# from telegram_cli.database.db_manager import DatabaseManager

# लॉगर को इनिशियलाइज़ करने के लिए (लेकिन इसे बाद में सक्रिय करेंगे)
# import logging
# logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = [
    "__version__",
    "cli_main",
    # "AccountManager",
    # "Uploader",
    # "Downloader",
    # "DatabaseManager",
]