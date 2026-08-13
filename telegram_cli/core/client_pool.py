#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
telegram_cli/core/client_pool.py – Manage pool of Telegram clients

Handles:
- Creating and managing multiple Telegram clients
- Account rotation (round-robin)
- Getting clients by phone number
- Connection pooling for parallel operations
- Health checking and auto-reconnection
"""

import asyncio
import random
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
from datetime import datetime
import time

# Import Telethon
try:
    from telethon import TelegramClient
    from telethon.errors import (
        RPCError,
        FloodWaitError,
        AuthKeyError,
        UnauthorizedError
    )
    TELETHON_AVAILABLE = True
except ImportError:
    TELETHON_AVAILABLE = False
    class TelegramClient:
        pass

# Import core modules
try:
    from telegram_cli.core.account_manager import AccountManager
    from telegram_cli.core.session_manager import SessionManager
    from telegram_cli.utils.logger import get_logger
    logger = get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.NullHandler())
    
    class AccountManager:
        def __init__(self):
            self.accounts = []
        def get_all_accounts(self):
            return self.accounts
        def rotate_account(self):
            return None if not self.accounts else self.accounts[0]
    
    class SessionManager:
        def __init__(self):
            self._active_clients = {}
        async def load_session(self, phone, api_id, api_hash):
            return None


class ClientPool:
    """
    Manages a pool of Telegram clients for multiple accounts.
    Provides client rotation, connection pooling, and health checks.
    """
    
    def __init__(
        self,
        account_manager: Optional[AccountManager] = None,
        session_manager: Optional[SessionManager] = None,
        auto_connect: bool = True,
        max_clients: int = 10
    ):
        """
        Initialize the client pool.
        
        Args:
            account_manager: AccountManager instance (creates one if None)
            session_manager: SessionManager instance (creates one if None)
            auto_connect: Automatically connect all clients on init
            max_clients: Maximum number of concurrent clients
        """
        self.account_manager = account_manager or AccountManager()
        self.session_manager = session_manager or SessionManager()
        self.max_clients = max_clients
        
        # Client storage
        self._clients: Dict[str, TelegramClient] = {}  # phone -> client
        self._client_metadata: Dict[str, Dict] = {}    # phone -> metadata
        self._current_index = 0
        self._lock = asyncio.Lock()
        
        # Connection status
        self._connected = False
        self._initialized = False
        
        logger.info(f"ClientPool initialized with {len(self.account_manager.get_all_accounts())} accounts")
        
        if auto_connect:
            # Run connect in background
            asyncio.create_task(self.initialize_all())
    
    async def initialize_all(self) -> None:
        """Initialize all clients from configured accounts."""
        if self._initialized:
            return
        
        async with self._lock:
            accounts = self.account_manager.get_all_accounts()
            
            if not accounts:
                logger.warning("No accounts configured. Please add accounts first.")
                return
            
            logger.info(f"Initializing {len(accounts)} clients...")
            
            # Initialize clients sequentially to avoid rate limiting
            for acc in accounts:
                phone = acc.get('phone')
                api_id = acc.get('api_id')
                api_hash = acc.get('api_hash')
                session_path = acc.get('session_path')
                
                if not phone or not api_id or not api_hash:
                    logger.error(f"Invalid account configuration: {acc}")
                    continue
                
                try:
                    # Load or create session
                    client = await self.session_manager.load_session(
                        phone=phone,
                        api_id=api_id,
                        api_hash=api_hash
                    )
                    
                    if client:
                        self._clients[phone] = client
                        self._client_metadata[phone] = {
                            'phone': phone,
                            'api_id': api_id,
                            'connected': True,
                            'last_used': datetime.now(),
                            'session_path': session_path,
                            'healthy': True,
                            'errors': 0,
                            'last_error': None
                        }
                        logger.info(f"✅ Client initialized for {phone}")
                    else:
                        logger.error(f"❌ Failed to initialize client for {phone}")
                        
                except Exception as e:
                    logger.error(f"❌ Error initializing client for {phone}: {e}")
                    # Store failed client metadata
                    self._client_metadata[phone] = {
                        'phone': phone,
                        'api_id': api_id,
                        'connected': False,
                        'last_used': None,
                        'session_path': session_path,
                        'healthy': False,
                        'errors': 1,
                        'last_error': str(e)
                    }
            
            self._initialized = True
            self._connected = len(self._clients) > 0
            logger.info(f"ClientPool initialized with {len(self._clients)} active clients")
    
    async def get_client(self, phone: Optional[str] = None) -> Optional[TelegramClient]:
        """
        Get a client for a specific account or rotate to next.
        
        Args:
            phone: Optional phone number. If None, rotates to next account.
        
        Returns:
            TelegramClient instance or None if unavailable
        """
        if not self._initialized:
            await self.initialize_all()
        
        if not self._clients:
            logger.error("No clients available")
            return None
        
        # If phone is specified, try to get that client
        if phone:
            client = self._clients.get(phone)
            if client:
                # Check if client is still healthy
                if await self._check_client_health(client):
                    self._update_metadata(phone, {'last_used': datetime.now()})
                    return client
                else:
                    logger.warning(f"Client {phone} is not healthy, attempting to reconnect...")
                    # Try to reconnect
                    client = await self._reconnect_client(phone)
                    if client:
                        return client
            
            logger.warning(f"Client for {phone} not found or unhealthy")
            return None
        
        # Rotate to next client
        return await self.get_next_client()
    
    async def get_next_client(self) -> Optional[TelegramClient]:
        """
        Get the next client in round-robin rotation.
        
        Returns:
            Next available TelegramClient or None
        """
        if not self._clients:
            return None
        
        # Try up to 3 times to find a healthy client
        attempts = 0
        max_attempts = min(3, len(self._clients))
        
        while attempts < max_attempts:
            # Get phone at current index
            phones = list(self._clients.keys())
            if not phones:
                return None
            
            phone = phones[self._current_index % len(phones)]
            
            # Move to next index for next call
            self._current_index = (self._current_index + 1) % len(phones)
            
            # Try to get the client
            client = await self.get_client(phone)
            if client:
                return client
            
            attempts += 1
        
        logger.error("No healthy clients available after multiple attempts")
        return None
    
    async def get_all_clients(self) -> List[TelegramClient]:
        """
        Get all available clients.
        
        Returns:
            List of all healthy TelegramClient instances
        """
        if not self._initialized:
            await self.initialize_all()
        
        healthy_clients = []
        for phone, client in self._clients.items():
            if await self._check_client_health(client):
                healthy_clients.append(client)
            else:
                logger.warning(f"Client {phone} is unhealthy, skipping")
        
        return healthy_clients
    
    def get_client_phones(self) -> List[str]:
        """Get list of all client phone numbers."""
        return list(self._clients.keys())
    
    def get_client_metadata(self, phone: Optional[str] = None) -> Dict[str, Any]:
        """
        Get metadata for a specific client or all clients.
        
        Args:
            phone: Optional phone number. If None, returns all metadata.
        
        Returns:
            Client metadata dictionary
        """
        if phone:
            return self._client_metadata.get(phone, {})
        return self._client_metadata
    
    async def _check_client_health(self, client: TelegramClient) -> bool:
        """
        Check if a client is healthy and connected.
        
        Args:
            client: TelegramClient to check
        
        Returns:
            True if healthy
        """
        try:
            # Try to get the current user
            me = await client.get_me()
            if me:
                return True
            return False
        except (UnauthorizedError, AuthKeyError):
            return False
        except FloodWaitError:
            # Client is rate limited, consider it healthy but temporarily unavailable
            return False
        except Exception as e:
            logger.debug(f"Health check failed: {e}")
            return False
    
    async def _reconnect_client(self, phone: str) -> Optional[TelegramClient]:
        """
        Attempt to reconnect a client.
        
        Args:
            phone: Phone number to reconnect
        
        Returns:
            Reconnected client or None
        """
        async with self._lock:
            try:
                # Remove existing client
                if phone in self._clients:
                    try:
                        await self._clients[phone].disconnect()
                    except:
                        pass
                    del self._clients[phone]
                
                # Get account details
                accounts = self.account_manager.get_all_accounts()
                acc = next((a for a in accounts if a.get('phone') == phone), None)
                
                if not acc:
                    logger.error(f"Account {phone} not found")
                    return None
                
                # Create new client
                api_id = acc.get('api_id')
                api_hash = acc.get('api_hash')
                
                if not api_id or not api_hash:
                    logger.error(f"API credentials missing for {phone}")
                    return None
                
                client = await self.session_manager.load_session(
                    phone=phone,
                    api_id=api_id,
                    api_hash=api_hash
                )
                
                if client:
                    self._clients[phone] = client
                    self._client_metadata[phone]['connected'] = True
                    self._client_metadata[phone]['healthy'] = True
                    self._client_metadata[phone]['errors'] = 0
                    self._client_metadata[phone]['last_error'] = None
                    logger.info(f"✅ Reconnected client for {phone}")
                    return client
                else:
                    logger.error(f"❌ Failed to reconnect client for {phone}")
                    self._client_metadata[phone]['healthy'] = False
                    self._client_metadata[phone]['errors'] += 1
                    return None
                    
            except Exception as e:
                logger.error(f"Error reconnecting client {phone}: {e}")
                self._client_metadata[phone]['healthy'] = False
                self._client_metadata[phone]['last_error'] = str(e)
                self._client_metadata[phone]['errors'] += 1
                return None
    
    def _update_metadata(self, phone: str, updates: Dict) -> None:
        """Update metadata for a client."""
        if phone in self._client_metadata:
            self._client_metadata[phone].update(updates)
    
    async def refresh_clients(self) -> None:
        """Refresh all clients (reconnect if needed)."""
        logger.info("Refreshing all clients...")
        for phone in list(self._clients.keys()):
            if not await self._check_client_health(self._clients[phone]):
                logger.info(f"Refreshing client for {phone}")
                await self._reconnect_client(phone)
        logger.info("Client refresh complete")
    
    async def close_all(self) -> None:
        """Close all clients and cleanup."""
        logger.info("Closing all clients...")
        for phone, client in self._clients.items():
            try:
                await client.disconnect()
                logger.info(f"Disconnected client for {phone}")
            except Exception as e:
                logger.error(f"Error disconnecting {phone}: {e}")
        
        self._clients.clear()
        self._client_metadata.clear()
        self._connected = False
        self._initialized = False
        logger.info("All clients closed")
    
    def count(self) -> int:
        """Get number of active clients."""
        return len(self._clients)
    
    def is_connected(self) -> bool:
        """Check if pool has any connected clients."""
        return self._connected and len(self._clients) > 0
    
    async def get_healthy_count(self) -> int:
        """Get number of healthy clients."""
        count = 0
        for phone, client in self._clients.items():
            if await self._check_client_health(client):
                count += 1
        return count
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the client pool.
        
        Returns:
            Dictionary with pool statistics
        """
        total = len(self._clients)
        healthy = sum(1 for meta in self._client_metadata.values() if meta.get('healthy', False))
        errors = sum(meta.get('errors', 0) for meta in self._client_metadata.values())
        
        return {
            'total_clients': total,
            'healthy_clients': healthy,
            'unhealthy_clients': total - healthy,
            'total_errors': errors,
            'clients': self._client_metadata
        }
    
    async def __aenter__(self):
        """Context manager entry."""
        await self.initialize_all()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        await self.close_all()
    
    def __repr__(self) -> str:
        return f"ClientPool(clients={len(self._clients)}, connected={self._connected})"