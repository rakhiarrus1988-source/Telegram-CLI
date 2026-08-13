#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
telegram_cli/core/account_manager.py – Manage multiple Telegram accounts

Handles:
- Adding new accounts with API credentials and session paths
- Removing accounts
- Listing all configured accounts
- Getting a specific account by phone
- Rotating accounts (round-robin) for load balancing
- Persistent storage in YAML config file
"""

import os
import yaml
from pathlib import Path
from typing import List, Dict, Optional, Any
from datetime import datetime

# Import config manager
try:
    from telegram_cli.utils.config import Config
except ImportError:
    # Fallback if config module not yet created
    class Config:
        def __init__(self):
            self.config_path = Path("data/config/settings.yaml")
            self.config = self._load_default()
        
        def _load_default(self):
            return {"accounts": []}
        
        def load(self):
            if self.config_path.exists():
                with open(self.config_path, 'r') as f:
                    self.config = yaml.safe_load(f) or {"accounts": []}
            return self.config
        
        def save(self):
            self.config_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.config_path, 'w') as f:
                yaml.dump(self.config, f, default_flow_style=False)
        
        def get(self, key, default=None):
            return self.config.get(key, default)
        
        def set(self, key, value):
            self.config[key] = value

# Import logger
try:
    from telegram_cli.utils.logger import get_logger
    logger = get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.NullHandler())


class AccountManager:
    """
    Manages multiple Telegram accounts with persistent storage.
    Accounts are stored in data/config/settings.yaml
    """
    
    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize the account manager.
        
        Args:
            config_path: Path to config file (default: data/config/settings.yaml)
        """
        self.config = Config()
        self.config_path = config_path or Path("data/config/settings.yaml")
        self.accounts_key = "accounts"
        self._current_index = 0  # For round-robin rotation
        self.load()
    
    def load(self) -> None:
        """Load accounts from config file."""
        try:
            self.config.load()
            self.accounts = self.config.get(self.accounts_key, [])
            logger.info(f"Loaded {len(self.accounts)} accounts")
        except Exception as e:
            logger.error(f"Failed to load accounts: {e}")
            self.accounts = []
    
    def save(self) -> None:
        """Save accounts to config file."""
        try:
            self.config.set(self.accounts_key, self.accounts)
            self.config.save()
            logger.info(f"Saved {len(self.accounts)} accounts")
        except Exception as e:
            logger.error(f"Failed to save accounts: {e}")
            raise
    
    def add_account(self, api_id: int, api_hash: str, phone: str, session_path: str) -> bool:
        """
        Add a new account.
        
        Args:
            api_id: Telegram API ID
            api_hash: Telegram API Hash
            phone: Phone number with country code (e.g., +911234567890)
            session_path: Path to session file
        
        Returns:
            True if added successfully, False if account already exists
        """
        # Check if account already exists
        if self.get_account(phone) is not None:
            logger.warning(f"Account {phone} already exists")
            return False
        
        # Create account entry
        account = {
            "api_id": api_id,
            "api_hash": api_hash,
            "phone": phone,
            "session_path": session_path,
            "added_on": datetime.now().isoformat(),
            "last_used": None,
            "upload_count": 0,
            "download_count": 0,
            "total_upload_size": 0,
            "total_download_size": 0
        }
        
        self.accounts.append(account)
        self.save()
        logger.info(f"Added account: {phone}")
        return True
    
    def remove_account(self, phone: str) -> bool:
        """
        Remove an account by phone number.
        
        Args:
            phone: Phone number to remove
        
        Returns:
            True if removed, False if not found
        """
        initial_count = len(self.accounts)
        self.accounts = [acc for acc in self.accounts if acc.get("phone") != phone]
        
        if len(self.accounts) < initial_count:
            self.save()
            logger.info(f"Removed account: {phone}")
            return True
        
        logger.warning(f"Account {phone} not found")
        return False
    
    def get_all_accounts(self) -> List[Dict[str, Any]]:
        """
        Get all configured accounts.
        
        Returns:
            List of account dictionaries
        """
        return self.accounts.copy()
    
    def get_account(self, phone: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific account by phone number.
        
        Args:
            phone: Phone number to look up
        
        Returns:
            Account dict or None if not found
        """
        for acc in self.accounts:
            if acc.get("phone") == phone:
                return acc.copy()
        return None
    
    def get_account_by_session(self, session_path: str) -> Optional[Dict[str, Any]]:
        """
        Get account by session file path.
        
        Args:
            session_path: Path to session file
        
        Returns:
            Account dict or None if not found
        """
        for acc in self.accounts:
            if acc.get("session_path") == session_path:
                return acc.copy()
        return None
    
    def rotate_account(self) -> Optional[Dict[str, Any]]:
        """
        Get the next account in round-robin rotation.
        Useful for distributing load across accounts.
        
        Returns:
            Next account dict or None if no accounts
        """
        if not self.accounts:
            return None
        
        # Get account at current index
        account = self.accounts[self._current_index].copy()
        
        # Update index for next call (circular)
        self._current_index = (self._current_index + 1) % len(self.accounts)
        
        # Update last_used
        for acc in self.accounts:
            if acc.get("phone") == account.get("phone"):
                acc["last_used"] = datetime.now().isoformat()
                break
        
        # Save last_used update
        self.save()
        
        return account
    
    def get_accounts_by_phones(self, phones: List[str]) -> List[Dict[str, Any]]:
        """
        Get multiple accounts by their phone numbers.
        
        Args:
            phones: List of phone numbers
        
        Returns:
            List of account dicts (only those found)
        """
        result = []
        for phone in phones:
            acc = self.get_account(phone)
            if acc:
                result.append(acc)
        return result
    
    def update_stats(self, phone: str, upload: bool = False, download: bool = False, size: int = 0) -> bool:
        """
        Update upload/download statistics for an account.
        
        Args:
            phone: Phone number of the account
            upload: True if this is an upload operation
            download: True if this is a download operation
            size: Size in bytes (will be added to total)
        
        Returns:
            True if updated successfully
        """
        for acc in self.accounts:
            if acc.get("phone") == phone:
                if upload:
                    acc["upload_count"] = acc.get("upload_count", 0) + 1
                    acc["total_upload_size"] = acc.get("total_upload_size", 0) + size
                if download:
                    acc["download_count"] = acc.get("download_count", 0) + 1
                    acc["total_download_size"] = acc.get("total_download_size", 0) + size
                acc["last_used"] = datetime.now().isoformat()
                self.save()
                logger.debug(f"Updated stats for {phone}: upload={upload}, download={download}, size={size}")
                return True
        logger.warning(f"Account {phone} not found for stats update")
        return False
    
    def get_stats(self, phone: Optional[str] = None) -> Dict[str, Any]:
        """
        Get statistics for an account or all accounts.
        
        Args:
            phone: Optional phone number. If None, returns total stats.
        
        Returns:
            Dictionary with statistics
        """
        if phone:
            acc = self.get_account(phone)
            if acc:
                return {
                    "phone": phone,
                    "upload_count": acc.get("upload_count", 0),
                    "download_count": acc.get("download_count", 0),
                    "total_upload_size": acc.get("total_upload_size", 0),
                    "total_download_size": acc.get("total_download_size", 0),
                    "last_used": acc.get("last_used")
                }
            return {}
        
        # Total stats across all accounts
        total = {
            "total_accounts": len(self.accounts),
            "total_upload_count": 0,
            "total_download_count": 0,
            "total_upload_size": 0,
            "total_download_size": 0,
            "accounts": []
        }
        
        for acc in self.accounts:
            total["total_upload_count"] += acc.get("upload_count", 0)
            total["total_download_count"] += acc.get("download_count", 0)
            total["total_upload_size"] += acc.get("total_upload_size", 0)
            total["total_download_size"] += acc.get("total_download_size", 0)
            total["accounts"].append({
                "phone": acc.get("phone"),
                "upload_count": acc.get("upload_count", 0),
                "download_count": acc.get("download_count", 0),
                "upload_size": acc.get("total_upload_size", 0),
                "download_size": acc.get("total_download_size", 0)
            })
        
        return total
    
    def count(self) -> int:
        """Return number of configured accounts."""
        return len(self.accounts)
    
    def is_empty(self) -> bool:
        """Check if no accounts are configured."""
        return len(self.accounts) == 0
    
    def get_first_account(self) -> Optional[Dict[str, Any]]:
        """Get the first account (useful for single-account setups)."""
        if self.accounts:
            return self.accounts[0].copy()
        return None
    
    def reset_rotation(self) -> None:
        """Reset rotation index to 0."""
        self._current_index = 0
        logger.debug("Rotation index reset")
    
    def __len__(self) -> int:
        return len(self.accounts)
    
    def __repr__(self) -> str:
        return f"AccountManager(accounts={len(self.accounts)})"