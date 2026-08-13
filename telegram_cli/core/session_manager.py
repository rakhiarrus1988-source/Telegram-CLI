#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
telegram_cli/core/session_manager.py – Manage Telegram sessions

Handles:
- Creating new sessions with OTP verification
- Loading existing sessions
- Converting session formats
- Managing session files
- Multi-account session support
"""

import os
import asyncio
from pathlib import Path
from typing import Optional, Tuple, Any
from datetime import datetime
import json

# Import Telethon for Telegram client
try:
    from telethon import TelegramClient
    from telethon.errors import (
        SessionPasswordNeededError,
        PhoneCodeInvalidError,
        PhoneCodeExpiredError,
        FloodWaitError,
        RPCError
    )
    TELETHON_AVAILABLE = True
except ImportError:
    TELETHON_AVAILABLE = False
    # Placeholder for telethon classes if not installed
    class TelegramClient:
        pass

# Import config and logger
try:
    from telegram_cli.utils.config import Config
    from telegram_cli.utils.logger import get_logger
    logger = get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.NullHandler())
    class Config:
        def __init__(self):
            self.config = {}
        def get(self, key, default=None):
            return self.config.get(key, default)


class SessionManager:
    """
    Manages Telegram sessions for multiple accounts.
    Handles creation, loading, and storage of session files.
    """
    
    def __init__(self, session_dir: Optional[Path] = None):
        """
        Initialize the session manager.
        
        Args:
            session_dir: Directory to store session files (default: data/sessions/)
        """
        if session_dir is None:
            session_dir = Path("data/sessions")
        self.session_dir = Path(session_dir)
        self.session_dir.mkdir(parents=True, exist_ok=True)
        
        # Load config for default settings
        self.config = Config()
        self.api_id = self.config.get("telegram_api_id")
        self.api_hash = self.config.get("telegram_api_hash")
        
        # Active clients cache
        self._active_clients = {}
        
        logger.info(f"SessionManager initialized with session_dir: {self.session_dir}")
    
    def get_session_path(self, phone: str) -> Path:
        """
        Get the session file path for a phone number.
        
        Args:
            phone: Phone number with country code
        
        Returns:
            Path to session file
        """
        # Clean phone number for filename (remove +, spaces, etc.)
        clean_phone = phone.replace("+", "").replace(" ", "").replace("-", "")
        return self.session_dir / f"{clean_phone}.session"
    
    def session_exists(self, phone: str) -> bool:
        """
        Check if a session exists for a phone number.
        
        Args:
            phone: Phone number with country code
        
        Returns:
            True if session exists
        """
        session_path = self.get_session_path(phone)
        return session_path.exists()
    
    def get_session_file(self, session_path: Path) -> Optional[Path]:
        """
        Get the actual session file (handles .session, .session-journal, etc.)
        
        Args:
            session_path: Base session path
        
        Returns:
            Path to session file or None if not found
        """
        # Check if .session file exists
        if session_path.exists():
            return session_path
        
        # Check if .session file with different extension exists
        for ext in ['.session', '.session-journal']:
            test_path = session_path.with_suffix(ext)
            if test_path.exists():
                return test_path
        
        return None
    
    async def create_session(
        self,
        api_id: Optional[int] = None,
        api_hash: Optional[str] = None,
        phone: Optional[str] = None,
        force_new: bool = False
    ) -> Optional[TelegramClient]:
        """
        Create a new session or load existing one.
        If phone is provided and session exists, it will load it.
        If session doesn't exist, it will create a new one with OTP verification.
        
        Args:
            api_id: Telegram API ID (default: from config)
            api_hash: Telegram API Hash (default: from config)
            phone: Phone number with country code
            force_new: Force create new session even if one exists
        
        Returns:
            TelegramClient instance or None if failed
        """
        if not TELETHON_AVAILABLE:
            logger.error("Telethon not installed. Please install: pip install telethon")
            raise ImportError("Telethon is required for session management")
        
        # Use provided credentials or fallback to config
        api_id = api_id or self.api_id
        api_hash = api_hash or self.api_hash
        
        if not api_id or not api_hash:
            logger.error("API ID and API Hash are required")
            raise ValueError("API ID and API Hash must be provided or set in config")
        
        if not phone:
            logger.error("Phone number is required")
            raise ValueError("Phone number is required")
        
        # Get session path
        session_path = self.get_session_path(phone)
        
        # Check if session exists
        if not force_new and self.session_exists(phone):
            logger.info(f"Loading existing session for {phone}")
            return await self.load_session(phone, api_id, api_hash)
        
        # Create new session
        logger.info(f"Creating new session for {phone}")
        client = TelegramClient(str(session_path), api_id, api_hash)
        
        try:
            # Start the client and handle authentication
            await client.start(
                phone=lambda: phone,
                code_callback=self._get_otp_code,
                password=self._get_password
            )
            
            logger.info(f"Successfully created session for {phone}")
            logger.info(f"Session saved to: {session_path}")
            
            # Cache the client
            self._active_clients[phone] = client
            
            return client
            
        except FloodWaitError as e:
            logger.error(f"Flood wait: {e.seconds} seconds")
            raise
        except PhoneCodeInvalidError:
            logger.error("Invalid OTP code provided")
            raise
        except PhoneCodeExpiredError:
            logger.error("OTP code expired")
            raise
        except SessionPasswordNeededError:
            logger.error("2FA password required but not provided")
            raise
        except RPCError as e:
            logger.error(f"RPC Error: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to create session: {e}")
            # Clean up partial session file
            if session_path.exists():
                session_path.unlink()
            raise
    
    async def load_session(
        self,
        phone: str,
        api_id: Optional[int] = None,
        api_hash: Optional[str] = None
    ) -> Optional[TelegramClient]:
        """
        Load an existing session for a phone number.
        
        Args:
            phone: Phone number with country code
            api_id: Telegram API ID (default: from config)
            api_hash: Telegram API Hash (default: from config)
        
        Returns:
            TelegramClient instance or None if failed
        """
        if not TELETHON_AVAILABLE:
            raise ImportError("Telethon is required for session management")
        
        # Check if already cached
        if phone in self._active_clients:
            logger.info(f"Returning cached client for {phone}")
            return self._active_clients[phone]
        
        # Use provided credentials or fallback to config
        api_id = api_id or self.api_id
        api_hash = api_hash or self.api_hash
        
        if not api_id or not api_hash:
            raise ValueError("API ID and API Hash must be provided or set in config")
        
        # Check if session exists
        session_path = self.get_session_path(phone)
        if not self.session_exists(phone):
            logger.error(f"No session found for {phone}")
            return None
        
        # Load the session
        try:
            client = TelegramClient(str(session_path), api_id, api_hash)
            await client.start()
            
            # Verify connection
            me = await client.get_me()
            if me:
                logger.info(f"Successfully loaded session for {phone} (user: {me.first_name})")
                # Cache the client
                self._active_clients[phone] = client
                return client
            else:
                logger.error(f"Failed to verify session for {phone}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to load session: {e}")
            return None
    
    async def _get_otp_code(self) -> str:
        """Callback for getting OTP code from user."""
        # This will be handled by the CLI via prompt
        # The actual prompting happens in the CLI layer
        # We'll raise an exception here to let the CLI handle it
        raise NotImplementedError("OTP code input handled by CLI")
    
    async def _get_password(self) -> str:
        """Callback for getting 2FA password from user."""
        raise NotImplementedError("2FA password input handled by CLI")
    
    async def delete_session(self, phone: str) -> bool:
        """
        Delete a session file.
        
        Args:
            phone: Phone number with country code
        
        Returns:
            True if deleted successfully
        """
        session_path = self.get_session_path(phone)
        
        # Close client if cached
        if phone in self._active_clients:
            try:
                await self._active_clients[phone].disconnect()
            except:
                pass
            del self._active_clients[phone]
        
        # Delete session file
        if session_path.exists():
            try:
                session_path.unlink()
                logger.info(f"Deleted session for {phone}")
                return True
            except Exception as e:
                logger.error(f"Failed to delete session: {e}")
                return False
        
        # Also delete any journal files
        for ext in ['.session-journal', '.session.lock']:
            journal_path = session_path.with_suffix(ext)
            if journal_path.exists():
                try:
                    journal_path.unlink()
                    logger.debug(f"Deleted journal file: {journal_path}")
                except:
                    pass
        
        logger.warning(f"No session found for {phone}")
        return False
    
    async def close_all_clients(self) -> None:
        """Close all active clients."""
        for phone, client in self._active_clients.items():
            try:
                await client.disconnect()
                logger.info(f"Disconnected client for {phone}")
            except Exception as e:
                logger.error(f"Error disconnecting client {phone}: {e}")
        self._active_clients.clear()
    
    def get_active_clients(self) -> dict:
        """Get all active clients."""
        return self._active_clients.copy()
    
    def get_phone_from_session(self, session_path: Path) -> Optional[str]:
        """
        Try to extract phone number from session file.
        This is a best-effort operation.
        
        Args:
            session_path: Path to session file
        
        Returns:
            Phone number or None if not found
        """
        try:
            # Read the session file (it's a binary format, but might contain the phone)
            if session_path.exists():
                with open(session_path, 'rb') as f:
                    data = f.read()
                    # Try to decode as UTF-8 (phone might be stored as string)
                    try:
                        text = data.decode('utf-8', errors='ignore')
                        # Look for phone number pattern
                        import re
                        phone_pattern = r'\+?\d{10,15}'
                        matches = re.findall(phone_pattern, text)
                        if matches:
                            return matches[0]
                    except:
                        pass
        except:
            pass
        return None
    
    def list_all_sessions(self) -> list:
        """
        List all session files in the session directory.
        
        Returns:
            List of session file paths
        """
        sessions = []
        for file in self.session_dir.glob("*.session"):
            # Only include .session files, not journal files
            if file.is_file() and not file.name.endswith('.journal'):
                sessions.append(file)
        return sessions
    
    async def verify_session(self, client: TelegramClient) -> bool:
        """
        Verify if a session is still valid.
        
        Args:
            client: TelegramClient instance
        
        Returns:
            True if session is valid
        """
        try:
            me = await client.get_me()
            if me:
                return True
            return False
        except:
            return False
    
    def __repr__(self) -> str:
        return f"SessionManager(session_dir={self.session_dir}, active_clients={len(self._active_clients)})"