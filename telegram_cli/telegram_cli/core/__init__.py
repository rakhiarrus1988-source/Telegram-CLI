#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
telegram_cli.core – Core modules for Telegram-CLI

This package contains all the core functionality:
- Account management
- Session handling
- Upload/Download with parallel connections
- Encryption/Decryption
- File tracking and searching
- Batch processing
- Integrity checking
- Scheduling
- Configuration export/import
"""

# Import core classes for easy access
try:
    from telegram_cli.core.account_manager import AccountManager
    from telegram_cli.core.session_manager import SessionManager
    from telegram_cli.core.client_pool import ClientPool
    from telegram_cli.core.uploader import Uploader
    from telegram_cli.core.downloader import Downloader
    from telegram_cli.core.searcher import Searcher
    from telegram_cli.core.encryptor import Encryptor
    from telegram_cli.core.file_tracker import FileTracker
    from telegram_cli.core.batch_processor import BatchProcessor
    from telegram_cli.core.integrity_checker import IntegrityChecker
    from telegram_cli.core.scheduler import Scheduler
    from telegram_cli.core.config_exporter import ConfigExporter
except ImportError:
    # If modules not yet created, define placeholder classes
    class AccountManager:
        pass
    class SessionManager:
        pass
    class ClientPool:
        pass
    class Uploader:
        pass
    class Downloader:
        pass
    class Searcher:
        pass
    class Encryptor:
        pass
    class FileTracker:
        pass
    class BatchProcessor:
        pass
    class IntegrityChecker:
        pass
    class Scheduler:
        pass
    class ConfigExporter:
        pass

__all__ = [
    "AccountManager",
    "SessionManager",
    "ClientPool",
    "Uploader",
    "Downloader",
    "Searcher",
    "Encryptor",
    "FileTracker",
    "BatchProcessor",
    "IntegrityChecker",
    "Scheduler",
    "ConfigExporter",
]

__version__ = "0.1.0"
__author__ = "Your Name"