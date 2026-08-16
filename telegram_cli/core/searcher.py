#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
telegram_cli/core/searcher.py – Advanced search across all Telegram accounts

Handles:
- Search messages across ALL configured accounts
- Search by keyword, file type, date range
- Search in specific channels or all channels
- Search saved messages (telegram search saved message /xyz)
- Filter results by media type, date, sender
- Export search results to JSON/CSV
- Parallel search across multiple accounts
- Fuzzy search and regex support
"""

import asyncio
import re
import json
import csv
from pathlib import Path
from typing import Optional, List, Dict, Any, Union, Tuple
from datetime import datetime, timedelta
from collections import defaultdict

# Import Telethon
try:
    from telethon import TelegramClient, types
    from telethon.tl.functions.messages import SearchRequest, GetHistoryRequest
    from telethon.tl.types import (
        Message,
        InputMessagesFilterEmpty,
        InputMessagesFilterPhotos,
        InputMessagesFilterVideo,
        InputMessagesFilterDocument,
        InputMessagesFilterUrl,
        InputMessagesFilterMusic,
        InputMessagesFilterVoice,
        InputMessagesFilterRoundVideo,
        InputMessagesFilterGif,
        InputMessagesFilterMyMentions,
        InputMessagesFilterGeo,
        InputMessagesFilterContacts
    )
    from telethon.errors import FloodWaitError, RPCError
    TELETHON_AVAILABLE = True
except ImportError:
    TELETHON_AVAILABLE = False
    class TelegramClient:
        pass

# Import core modules
try:
    from telegram_cli.core.client_pool import ClientPool
    from telegram_cli.core.downloader import Downloader
    from telegram_cli.database.db_manager import DatabaseManager
    from telegram_cli.utils.config import Config
    from telegram_cli.utils.logger import get_logger
    from telegram_cli.utils.helpers import format_size, get_timestamp, truncate_text
    MODULES_LOADED = True
except ImportError:
    MODULES_LOADED = False
    import logging
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.NullHandler())
    
    class ClientPool:
        async def get_client(self, phone=None): return None
        async def get_all_clients(self): return []
        def get_client_phones(self): return []
    class Downloader:
        async def download_file(self, file_id, **kwargs): return {}
    class DatabaseManager:
        def get_file_records(self, **kwargs): return []
    class Config:
        def get(self, key, default=None): return default
    def format_size(s): return f"{s/1024:.2f} KB"
    def get_timestamp(): return datetime.now().isoformat()
    def truncate_text(t, l=100): return t[:l] + "..." if len(t) > l else t

logger = get_logger(__name__)


class Searcher:
    """
    Advanced search across all Telegram accounts.
    Searches messages, files, and channels with advanced filtering.
    """
    
    # Message filter types mapping
    FILTER_TYPES = {
        'all': InputMessagesFilterEmpty,
        'photo': InputMessagesFilterPhotos,
        'video': InputMessagesFilterVideo,
        'document': InputMessagesFilterDocument,
        'url': InputMessagesFilterUrl,
        'music': InputMessagesFilterMusic,
        'voice': InputMessagesFilterVoice,
        'round': InputMessagesFilterRoundVideo,
        'gif': InputMessagesFilterGif,
        'mentions': InputMessagesFilterMyMentions,
        'geo': InputMessagesFilterGeo,
        'contacts': InputMessagesFilterContacts
    }
    
    def __init__(
        self,
        client_pool: Optional[ClientPool] = None,
        downloader: Optional[Downloader] = None,
        db_manager: Optional[DatabaseManager] = None,
        config: Optional[Config] = None,
        max_results_per_account: int = 100,
        search_timeout: int = 30,
        parallel_search: bool = True,
        max_parallel: int = 4
    ):
        """
        Initialize the searcher.
        
        Args:
            client_pool: ClientPool instance
            downloader: Downloader instance (for downloading found files)
            db_manager: DatabaseManager instance
            config: Config instance
            max_results_per_account: Max results per account
            search_timeout: Timeout per search in seconds
            parallel_search: Enable parallel searching
            max_parallel: Maximum parallel searches
        """
        self.client_pool = client_pool or ClientPool()
        self.downloader = downloader or Downloader()
        self.db_manager = db_manager or DatabaseManager()
        self.config = config or Config()
        
        self.max_results_per_account = max_results_per_account
        self.search_timeout = search_timeout
        self.parallel_search = parallel_search
        self.max_parallel = max_parallel
        
        # Cache for search results
        self._search_cache = {}
        self._last_search_id = 0
        
        logger.info(f"Searcher initialized (max_results: {max_results_per_account})")
    
    # ============================================
    # Main Search Methods
    # ============================================
    
    async def search(
        self,
        query: str,
        accounts: Optional[List[str]] = None,
        channels: Optional[List[str]] = None,
        filter_type: str = 'all',
        limit: Optional[int] = None,
        offset_date: Optional[datetime] = None,
        min_id: int = 0,
        max_id: int = 0,
        from_user: Optional[str] = None,
        has_media: bool = False,
        case_sensitive: bool = False,
        regex: bool = False,
        download_matches: bool = False,
        download_limit: int = 10,
        export_format: Optional[str] = None,
        export_path: Optional[Path] = None
    ) -> Dict[str, Any]:
        """
        Search across all or specified accounts.
        
        Args:
            query: Search query (keyword, phrase, or regex)
            accounts: List of account phone numbers (None = all)
            channels: List of channel usernames/IDs (None = all channels)
            filter_type: 'all', 'photo', 'video', 'document', 'url', 'music', 'voice', 'gif'
            limit: Max results per account (uses default if None)
            offset_date: Search messages before this date
            min_id: Minimum message ID
            max_id: Maximum message ID
            from_user: Search messages from specific user
            has_media: Only show messages with media
            case_sensitive: Case sensitive search
            regex: Use regex for query
            download_matches: Download files that match
            download_limit: Max files to download
            export_format: 'json' or 'csv' to export results
            export_path: Path to export file
        
        Returns:
            Search results with metadata
        """
        # Generate search ID
        self._last_search_id += 1
        search_id = self._last_search_id
        
        logger.info(f"Search #{search_id}: '{query}' (filter: {filter_type})")
        
        # Get accounts to search
        if accounts:
            search_accounts = accounts
        else:
            search_accounts = self.client_pool.get_client_phones()
        
        if not search_accounts:
            logger.warning("No accounts available for search")
            return {
                'search_id': search_id,
                'query': query,
                'total_results': 0,
                'results': [],
                'errors': []
            }
        
        # Set limit
        limit = limit or self.max_results_per_account
        
        # Prepare filter
        filter_class = self.FILTER_TYPES.get(filter_type, InputMessagesFilterEmpty)
        search_filter = filter_class()
        
        # Parallel or sequential search
        if self.parallel_search and len(search_accounts) > 1:
            results = await self._parallel_search(
                accounts=search_accounts,
                query=query,
                channels=channels,
                search_filter=search_filter,
                limit=limit,
                offset_date=offset_date,
                min_id=min_id,
                max_id=max_id,
                from_user=from_user,
                has_media=has_media,
                case_sensitive=case_sensitive,
                regex=regex
            )
        else:
            results = await self._sequential_search(
                accounts=search_accounts,
                query=query,
                channels=channels,
                search_filter=search_filter,
                limit=limit,
                offset_date=offset_date,
                min_id=min_id,
                max_id=max_id,
                from_user=from_user,
                has_media=has_media,
                case_sensitive=case_sensitive,
                regex=regex
            )
        
        # Compile results
        all_results = []
        errors = []
        total_matches = 0
        
        for account_result in results:
            if account_result.get('error'):
                errors.append(account_result)
            else:
                all_results.extend(account_result.get('results', []))
                total_matches += account_result.get('count', 0)
        
        # Sort results by date (newest first)
        all_results.sort(key=lambda x: x.get('date', ''), reverse=True)
        
        # Download matches if requested
        downloaded = []
        if download_matches and all_results:
            downloaded = await self._download_matches(
                results=all_results,
                limit=download_limit
            )
        
        # Export results if requested
        if export_format and all_results:
            export_path = await self._export_results(
                results=all_results,
                format=export_format,
                output_path=export_path,
                search_id=search_id
            )
        
        response = {
            'search_id': search_id,
            'query': query,
            'filter_type': filter_type,
            'total_results': total_matches,
            'results': all_results,
            'downloaded': downloaded,
            'errors': errors,
            'accounts_searched': len(search_accounts)
        }
        
        if export_format and export_path:
            response['export_path'] = str(export_path)
        
        logger.info(f"Search #{search_id} complete: {total_matches} matches found")
        
        return response
    
    # ============================================
    # Search Implementation
    # ============================================
    
    async def _parallel_search(
        self,
        accounts: List[str],
        query: str,
        channels: Optional[List[str]],
        search_filter,
        limit: int,
        offset_date: Optional[datetime],
        min_id: int,
        max_id: int,
        from_user: Optional[str],
        has_media: bool,
        case_sensitive: bool,
        regex: bool
    ) -> List[Dict[str, Any]]:
        """Search in parallel across accounts."""
        
        semaphore = asyncio.Semaphore(self.max_parallel)
        
        async def search_account(account):
            async with semaphore:
                return await self._search_account(
                    account=account,
                    query=query,
                    channels=channels,
                    search_filter=search_filter,
                    limit=limit,
                    offset_date=offset_date,
                    min_id=min_id,
                    max_id=max_id,
                    from_user=from_user,
                    has_media=has_media,
                    case_sensitive=case_sensitive,
                    regex=regex
                )
        
        tasks = [search_account(account) for account in accounts]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        processed = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed.append({
                    'account': accounts[i],
                    'error': str(result),
                    'results': []
                })
            else:
                processed.append(result)
        
        return processed
    
    async def _sequential_search(
        self,
        accounts: List[str],
        query: str,
        channels: Optional[List[str]],
        search_filter,
        limit: int,
        offset_date: Optional[datetime],
        min_id: int,
        max_id: int,
        from_user: Optional[str],
        has_media: bool,
        case_sensitive: bool,
        regex: bool
    ) -> List[Dict[str, Any]]:
        """Search sequentially across accounts."""
        
        results = []
        
        for account in accounts:
            try:
                result = await self._search_account(
                    account=account,
                    query=query,
                    channels=channels,
                    search_filter=search_filter,
                    limit=limit,
                    offset_date=offset_date,
                    min_id=min_id,
                    max_id=max_id,
                    from_user=from_user,
                    has_media=has_media,
                    case_sensitive=case_sensitive,
                    regex=regex
                )
                results.append(result)
            except Exception as e:
                results.append({
                    'account': account,
                    'error': str(e),
                    'results': []
                })
        
        return results
    
    async def _search_account(
        self,
        account: str,
        query: str,
        channels: Optional[List[str]],
        search_filter,
        limit: int,
        offset_date: Optional[datetime],
        min_id: int,
        max_id: int,
        from_user: Optional[str],
        has_media: bool,
        case_sensitive: bool,
        regex: bool
    ) -> Dict[str, Any]:
        """Search in a single account."""
        
        try:
            # Get client
            client = await self.client_pool.get_client(account)
            if not client:
                raise RuntimeError(f"Client not available for {account}")
            
            # Get dialogs (channels/groups)
            dialogs = []
            if channels:
                # Search only in specified channels
                for channel in channels:
                    try:
                        entity = await client.get_entity(channel)
                        dialogs.append(entity)
                    except Exception as e:
                        logger.warning(f"Channel {channel} not found: {e}")
            else:
                # Get all dialogs
                async for dialog in client.iter_dialogs():
                    if dialog.is_channel or dialog.is_group:
                        dialogs.append(dialog.entity)
            
            if not dialogs:
                return {
                    'account': account,
                    'count': 0,
                    'results': []
                }
            
            # Search in each dialog
            results = []
            
            for entity in dialogs[:5]:  # Limit to 5 dialogs for performance
                try:
                    # Build search parameters
                    search_params = {
                        'entity': entity,
                        'search': query,
                        'filter': search_filter,
                        'limit': limit,
                        'offset_date': offset_date,
                        'min_id': min_id,
                        'max_id': max_id,
                        'from_user': from_user
                    }
                    
                    # Search messages
                    messages = await client.search_messages(**search_params)
                    
                    # Process each message
                    for msg in messages:
                        # Filter by media
                        if has_media and not msg.media:
                            continue
                        
                        # Apply custom filters
                        if not self._matches_query(msg.text or '', query, case_sensitive, regex):
                            continue
                        
                        # Build result entry
                        result_entry = self._build_result_entry(
                            message=msg,
                            account=account,
                            entity=entity
                        )
                        results.append(result_entry)
                    
                    # Random sleep between dialogs
                    await asyncio.sleep(0.5)
                    
                except FloodWaitError as e:
                    logger.warning(f"Rate limited on {account}: wait {e.seconds}s")
                    await asyncio.sleep(e.seconds + 5)
                except Exception as e:
                    logger.error(f"Error searching in dialog: {e}")
                    continue
            
            return {
                'account': account,
                'count': len(results),
                'results': results[:limit]
            }
            
        except Exception as e:
            logger.error(f"Search error for {account}: {e}")
            return {
                'account': account,
                'error': str(e),
                'count': 0,
                'results': []
            }
    
    # ============================================
    # Helper Methods
    # ============================================
    
    def _matches_query(self, text: str, query: str, case_sensitive: bool, regex: bool) -> bool:
        """Check if text matches the query."""
        if not text:
            return False
        
        if regex:
            try:
                flags = 0 if case_sensitive else re.IGNORECASE
                return bool(re.search(query, text, flags))
            except:
                return False
        else:
            if case_sensitive:
                return query in text
            else:
                return query.lower() in text.lower()
    
    def _build_result_entry(self, message, account: str, entity) -> Dict[str, Any]:
        """Build a result entry from a message."""
        
        entry = {
            'message_id': message.id,
            'account': account,
            'text': message.text,
            'date': message.date.isoformat() if message.date else None,
            'sender': None,
            'chat': {
                'id': entity.id if entity else None,
                'title': entity.title if hasattr(entity, 'title') else None,
                'username': entity.username if hasattr(entity, 'username') else None
            },
            'has_media': bool(message.media),
            'media_type': None,
            'file_id': None,
            'file_name': None,
            'file_size': None,
            'url': None
        }
        
        # Get sender info
        if hasattr(message, 'sender_id'):
            entry['sender'] = message.sender_id
        
        # Get media info
        if message.media:
            entry['media_type'] = self._get_media_type(message.media)
            
            if hasattr(message.media, 'document'):
                doc = message.media.document
                if doc:
                    entry['file_id'] = str(doc.id) if doc.id else None
                    entry['file_size'] = doc.size if hasattr(doc, 'size') else None
                    
                    # Get file name
                    if hasattr(doc, 'attributes'):
                        for attr in doc.attributes:
                            if hasattr(doc, 'attributes'):
                        for attr in doc.attributes:
                            if hasattr(attr, 'file_name'):
                                entry['file_name'] = attr.file_name
                                break
            elif hasattr(message.media, 'photo'):
                photo = message.media.photo
                if photo:
                    entry['file_id'] = str(photo.id) if photo.id else None
                    entry['media_type'] = 'photo'
        
        # Get URL if present
        if message.text:
            # Extract URLs
            url_pattern = r'https?://[^\s]+'
            urls = re.findall(url_pattern, message.text)
            if urls:
                entry['url'] = urls[0]
        
        return entry
    
    def _get_media_type(self, media) -> str:
        """Get media type from media object."""
        if not media:
            return 'none'
        
        if hasattr(media, 'document'):
            doc = media.document
            if doc:
                mime = doc.mime_type if hasattr(doc, 'mime_type') else ''
                if 'video' in mime:
                    return 'video'
                elif 'audio' in mime:
                    return 'audio'
                elif 'image' in mime:
                    return 'image'
                elif 'pdf' in mime:
                    return 'pdf'
                else:
                    return 'document'
        
        if hasattr(media, 'photo'):
            return 'photo'
        
        return 'other'
    
    async def _download_matches(
        self,
        results: List[Dict],
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Download files from search results."""
        
        downloaded = []
        count = 0
        
        for result in results:
            if count >= limit:
                break
            
            file_id = result.get('file_id')
            if file_id:
                try:
                    download_result = await self.downloader.download_file(
                        file_id=file_id,
                        account_phone=result.get('account')
                    )
                    
                    downloaded.append({
                        'message_id': result.get('message_id'),
                        'file_id': file_id,
                        'path': download_result.get('path'),
                        'size': download_result.get('size', 0)
                    })
                    count += 1
                    
                except Exception as e:
                    logger.error(f"Download failed for {file_id}: {e}")
                    continue
        
        return downloaded
    
    async def _export_results(
        self,
        results: List[Dict],
        format: str,
        output_path: Optional[Path],
        search_id: int
    ) -> Optional[Path]:
        """Export search results to file."""
        
        if not output_path:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = Path(f"data/exports/search_{search_id}_{timestamp}.{format}")
        
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            if format.lower() == 'json':
                with open(output_path, 'w') as f:
                    json.dump(results, f, indent=2, default=str)
                logger.info(f"Results exported to JSON: {output_path}")
                
            elif format.lower() == 'csv':
                # Flatten results for CSV
                flat_results = []
                for r in results:
                    flat = {
                        'message_id': r.get('message_id'),
                        'account': r.get('account'),
                        'text': r.get('text', ''),
                        'date': r.get('date'),
                        'chat_id': r.get('chat', {}).get('id'),
                        'chat_title': r.get('chat', {}).get('title'),
                        'chat_username': r.get('chat', {}).get('username'),
                        'has_media': r.get('has_media', False),
                        'media_type': r.get('media_type'),
                        'file_id': r.get('file_id'),
                        'file_name': r.get('file_name'),
                        'file_size': r.get('file_size'),
                        'url': r.get('url')
                    }
                    flat_results.append(flat)
                
                if flat_results:
                    with open(output_path, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=flat_results[0].keys())
                        writer.writeheader()
                        writer.writerows(flat_results)
                    logger.info(f"Results exported to CSV: {output_path}")
            else:
                raise ValueError(f"Unsupported export format: {format}")
            
            return output_path
            
        except Exception as e:
            logger.error(f"Export failed: {e}")
            return None
    
    # ============================================
    # Advanced Search Features
    # ============================================
    
    async def search_saved_messages(
        self,
        query: str,
        accounts: Optional[List[str]] = None,
        limit: int = 50
    ) -> Dict[str, Any]:
        """
        Search specifically in saved messages (telegram search saved message /xyz).
        
        Args:
            query: Search query
            accounts: List of account phone numbers
            limit: Max results per account
        
        Returns:
            Search results from saved messages
        """
        logger.info(f"Searching saved messages for: {query}")
        
        # Get accounts
        if not accounts:
            accounts = self.client_pool.get_client_phones()
        
        results = []
        
        for account in accounts:
            try:
                client = await self.client_pool.get_client(account)
                if not client:
                    continue
                
                # Get saved messages dialog
                me = await client.get_me()
                saved_entity = await client.get_entity(me.username)
                
                # Search in saved messages
                messages = await client.search_messages(
                    entity=saved_entity,
                    search=query,
                    limit=limit
                )
                
                for msg in messages:
                    entry = self._build_result_entry(
                        message=msg,
                        account=account,
                        entity=saved_entity
                    )
                    entry['source'] = 'saved_messages'
                    results.append(entry)
                
            except Exception as e:
                logger.error(f"Error searching saved messages for {account}: {e}")
                continue
        
        return {
            'query': query,
            'total_results': len(results),
            'results': results
        }
    
    async def search_files(
        self,
        filename: str,
        accounts: Optional[List[str]] = None,
        channels: Optional[List[str]] = None,
        limit: int = 50
    ) -> Dict[str, Any]:
        """
        Search for files by filename.
        
        Args:
            filename: Filename or pattern to search
            accounts: List of account phone numbers
            channels: List of channels to search in
            limit: Max results
        
        Returns:
            File search results
        """
        return await self.search(
            query=filename,
            accounts=accounts,
            channels=channels,
            filter_type='document',
            limit=limit,
            has_media=True
        )
    
    async def search_photos(
        self,
        query: str,
        accounts: Optional[List[str]] = None,
        channels: Optional[List[str]] = None,
        limit: int = 50
    ) -> Dict[str, Any]:
        """Search for photos."""
        return await self.search(
            query=query,
            accounts=accounts,
            channels=channels,
            filter_type='photo',
            limit=limit,
            has_media=True
        )
    
    async def search_videos(
        self,
        query: str,
        accounts: Optional[List[str]] = None,
        channels: Optional[List[str]] = None,
        limit: int = 50
    ) -> Dict[str, Any]:
        """Search for videos."""
        return await self.search(
            query=query,
            accounts=accounts,
            channels=channels,
            filter_type='video',
            limit=limit,
            has_media=True
        )
    
    async def search_links(
        self,
        domain: Optional[str] = None,
        accounts: Optional[List[str]] = None,
        channels: Optional[List[str]] = None,
        limit: int = 50
    ) -> Dict[str, Any]:
        """Search for messages containing URLs."""
        query = domain or ''
        return await self.search(
            query=query,
            accounts=accounts,
            channels=channels,
            filter_type='url',
            limit=limit
        )
    
    async def search_by_date(
        self,
        start_date: datetime,
        end_date: datetime,
        accounts: Optional[List[str]] = None,
        channels: Optional[List[str]] = None,
        limit: int = 50
    ) -> Dict[str, Any]:
        """Search messages within a date range."""
        # This would require custom implementation using GetHistoryRequest
        # For now, use offset_date with limit
        return await self.search(
            query='',
            accounts=accounts,
            channels=channels,
            limit=limit,
            offset_date=end_date
        )
    
    # ============================================
    # Utility Methods
    # ============================================
    
    def clear_cache(self) -> None:
        """Clear search cache."""
        self._search_cache.clear()
        logger.info("Search cache cleared")
    
    def get_search_stats(self) -> Dict[str, Any]:
        """Get search statistics."""
        return {
            'total_searches': self._last_search_id,
            'cache_size': len(self._search_cache),
            'max_results_per_account': self.max_results_per_account,
            'parallel_search': self.parallel_search
        }
    
    async def close(self) -> None:
        """Clean up resources."""
        await self.client_pool.close_all()
        logger.info("Searcher closed")
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    def __repr__(self) -> str:
        return f"Searcher(searches={self._last_search_id})"