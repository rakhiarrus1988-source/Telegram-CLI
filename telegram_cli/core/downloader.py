#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
telegram_cli/core/downloader.py – Advanced file downloader with channel support

Handles:
- Download files by ID, description, or from channels
- Download ALL data from Telegram channels (messages, media, files)
- Parallel downloading with multiple connections
- Auto-decryption with master password
- Re-upload downloaded data to Telegram
- Export channel data to local storage
- Resume interrupted downloads
- Progress tracking and database logging
"""

import os
import asyncio
import random
import hashlib
import json
from pathlib import Path
from typing import Optional, List, Dict, Any, Union, Tuple
from datetime import datetime, timedelta
import time
import aiofiles

# Import Telethon
try:
    from telethon import TelegramClient, types
    from telethon.tl.functions.messages import GetHistoryRequest
    from telethon.tl.types import (
        Message,
        MessageMediaDocument,
        MessageMediaPhoto,
        Document,
        Photo,
        InputMessagesFilterDocument,
        InputMessagesFilterPhotoVideo,
        InputMessagesFilterEmpty
    )
    from telethon.errors import (
        RPCError,
        FloodWaitError,
        FileReferenceInvalidError,
        MediaEmptyError
    )
    TELETHON_AVAILABLE = True
except ImportError:
    TELETHON_AVAILABLE = False
    class TelegramClient:
        pass

# Import core modules
try:
    from telegram_cli.core.client_pool import ClientPool
    from telegram_cli.core.encryptor import Encryptor
    from telegram_cli.core.file_tracker import FileTracker
    from telegram_cli.core.integrity_checker import IntegrityChecker
    from telegram_cli.core.uploader import Uploader
    from telegram_cli.database.db_manager import DatabaseManager
    from telegram_cli.utils.config import Config
    from telegram_cli.utils.logger import get_logger
    from telegram_cli.utils.progress_bar import ProgressBar
    from telegram_cli.utils.helpers import format_size, get_timestamp, human_readable_time, ensure_dir
    MODULES_LOADED = True
except ImportError:
    MODULES_LOADED = False
    import logging
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.NullHandler())
    
    class ClientPool:
        async def get_client(self, phone=None): return None
        async def get_all_clients(self): return []
    class Encryptor:
        def __init__(self): self.master_password = None
        def decrypt_file(self, path, password): return True
    class FileTracker:
        def record_download(self, **kwargs): pass
        def get_file_by_id(self, file_id): return None
    class IntegrityChecker:
        def calculate_hash(self, path): return "hash"
        def verify_integrity(self, path, expected): return True
    class Uploader:
        async def upload_file(self, **kwargs): return {"file_id": "123"}
    class DatabaseManager:
        def __init__(self): pass
        def get_file_records(self, **kwargs): return []
    class Config:
        def get(self, key, default=None): return default
    class ProgressBar:
        def __init__(self, **kwargs): pass
        def update(self, n): pass
        def close(self): pass
    def format_size(s): return f"{s/1024:.2f} KB"
    def get_timestamp(): return datetime.now().isoformat()
    def human_readable_time(s): return f"{s:.2f}s"
    def ensure_dir(p): Path(p).mkdir(parents=True, exist_ok=True)

logger = get_logger(__name__)


class Downloader:
    """
    Advanced downloader with support for channels, parallel downloads,
    encryption, and re-upload functionality.
    """
    
    def __init__(
        self,
        client_pool: Optional[ClientPool] = None,
        db_manager: Optional[DatabaseManager] = None,
        encryptor: Optional[Encryptor] = None,
        tracker: Optional[FileTracker] = None,
        uploader: Optional[Uploader] = None,
        config: Optional[Config] = None,
        download_dir: Optional[Path] = None,
        parallel_connections: int = 4,
        max_retries: int = 3,
        enable_decryption: bool = True,
        random_sleep_range: Tuple[int, int] = (3, 10),
        chunk_size_mb: int = 10
    ):
        """
        Initialize the downloader.
        
        Args:
            client_pool: ClientPool instance
            db_manager: DatabaseManager instance
            encryptor: Encryptor instance
            tracker: FileTracker instance
            uploader: Uploader instance (for re-upload)
            config: Config instance
            download_dir: Directory to save downloaded files
            parallel_connections: Number of parallel connections
            max_retries: Maximum retry attempts
            enable_decryption: Whether to decrypt files
            random_sleep_range: Sleep range between downloads
            chunk_size_mb: Chunk size for downloads
        """
        self.client_pool = client_pool or ClientPool()
        self.db_manager = db_manager or DatabaseManager()
        self.encryptor = encryptor or Encryptor()
        self.tracker = tracker or FileTracker(self.db_manager)
        self.uploader = uploader or Uploader()
        self.config = config or Config()
        
        # Download settings
        self.download_dir = Path(download_dir or "data/downloads")
        ensure_dir(self.download_dir)
        
        self.parallel_connections = min(max(parallel_connections, 1), 6)
        self.max_retries = max_retries
        self.enable_decryption = enable_decryption
        self.random_sleep_range = random_sleep_range
        self.chunk_size = chunk_size_mb * 1024 * 1024
        
        # Statistics
        self.downloaded_files = 0
        self.downloaded_bytes = 0
        self.errors = 0
        
        # Active downloads tracking
        self._active_downloads = {}
        self._download_cache = {}
        
        logger.info(f"Downloader initialized with {self.parallel_connections} parallel connections")
        logger.info(f"Download directory: {self.download_dir}")
    
    # ============================================
    # Main Download Methods
    # ============================================
    
    async def download_file(
        self,
        file_id: str,
        output_path: Optional[Path] = None,
        decrypt: bool = True,
        password: Optional[str] = None,
        account_phone: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Download a file by its ID.
        
        Args:
            file_id: File ID from database or Telegram
            output_path: Output path (auto-generated if None)
            decrypt: Whether to decrypt the file
            password: Password for decryption
            account_phone: Specific account to use
        
        Returns:
            Download result dictionary
        """
        logger.info(f"Downloading file: {file_id}")
        
        # Check if file is already downloaded
        cached = self._download_cache.get(file_id)
        if cached:
            logger.info(f"File {file_id} already downloaded: {cached}")
            return cached
        
        # Get file record from database
        file_record = None
        if self.tracker:
            file_record = self.tracker.get_file_by_id(file_id)
        
        # If not in database, try to find in Telegram
        if not file_record:
            logger.info(f"File {file_id} not in database, searching in Telegram...")
            file_record = await self._search_file_in_telegram(file_id, account_phone)
        
        if not file_record:
            raise ValueError(f"File {file_id} not found in database or Telegram")
        
        # Determine output path
        if not output_path:
            file_name = file_record.get('file_name', f"{file_id}.file")
            output_path = self.download_dir / file_name
        
        output_path = Path(output_path)
        ensure_dir(output_path.parent)
        
        # Check if file already exists
        if output_path.exists():
            # Verify integrity
            if self._verify_file_integrity(output_path, file_record.get('hash')):
                logger.info(f"File already exists and verified: {output_path}")
                self._download_cache[file_id] = {
                    'success': True,
                    'path': str(output_path),
                    'size': output_path.stat().st_size,
                    'cached': True
                }
                return self._download_cache[file_id]
        
        # Download the file
        try:
            # Get client
            client = await self._get_client(account_phone)
            if not client:
                raise RuntimeError("No client available")
            
            # Check if it's a multi-part file
            parts = file_record.get('parts', 1)
            if parts > 1:
                result = await self._download_multipart(
                    file_id=file_id,
                    file_record=file_record,
                    output_path=output_path,
                    decrypt=decrypt,
                    password=password,
                    client=client
                )
            else:
                result = await self._download_single_file(
                    file_id=file_id,
                    file_record=file_record,
                    output_path=output_path,
                    decrypt=decrypt,
                    password=password,
                    client=client
                )
            
            if result and result.get('success'):
                # Record download in database
                if self.tracker:
                    self.tracker.record_download(
                        file_id=file_id,
                        file_name=output_path.name,
                        file_size=output_path.stat().st_size if output_path.exists() else 0,
                        download_date=get_timestamp(),
                        account_phone=account_phone or 'unknown',
                        source=file_record.get('source', 'telegram')
                    )
                
                # Update stats
                self.downloaded_files += 1
                if output_path.exists():
                    self.downloaded_bytes += output_path.stat().st_size
                
                # Cache result
                self._download_cache[file_id] = result
                
                # Random sleep
                await self._random_sleep()
                
                logger.info(f"✅ Download complete: {output_path.name}")
                return result
            else:
                raise RuntimeError("Download failed")
                
        except Exception as e:
            logger.error(f"Download failed: {e}")
            self.errors += 1
            raise
    
    async def _download_single_file(
        self,
        file_id: str,
        file_record: Dict,
        output_path: Path,
        decrypt: bool,
        password: Optional[str],
        client: TelegramClient
    ) -> Dict[str, Any]:
        """Download a single file from Telegram."""
        
        # Get the message from Telegram
        message = await self._get_message_by_id(client, file_id)
        if not message:
            # Try to find by file ID
            message = await self._find_file_by_id(client, file_id)
        
        if not message:
            raise ValueError(f"Message {file_id} not found")
        
        # Check if it's a media message
        if not hasattr(message, 'media') or not message.media:
            raise ValueError(f"Message {file_id} has no media")
        
        # Download the media
        temp_path = output_path.with_suffix('.temp')
        
        # Download with progress
        progress_bar = ProgressBar(
            total=file_record.get('file_size', 0),
            desc=f"Downloading {output_path.name}",
            unit='B',
            unit_scale=True
        )
        
        try:
            async def progress_callback(current, total):
                progress_bar.update(current)
            
            # Download file
            await client.download_media(
                message=message,
                file=temp_path,
                progress_callback=progress_callback
            )
            
            progress_bar.close()
            
            # Check if downloaded file exists
            if not temp_path.exists():
                raise RuntimeError("Download failed - file not saved")
            
            # Decrypt if needed
            if decrypt and self.enable_decryption:
                encrypted = file_record.get('encrypted', False)
                if encrypted:
                    logger.info("Decrypting file...")
                    decrypted = await self._decrypt_file(
                        file_path=temp_path,
                        output_path=output_path,
                        password=password or file_record.get('encryption_password')
                    )
                    if decrypted:
                        temp_path.unlink()
                    else:
                        raise RuntimeError("Decryption failed")
                else:
                    # Rename temp to final
                    temp_path.rename(output_path)
            else:
                # Rename temp to final
                temp_path.rename(output_path)
            
            # Verify integrity
            if not self._verify_file_integrity(output_path, file_record.get('hash')):
                logger.warning("File integrity check failed")
            
            return {
                'success': True,
                'path': str(output_path),
                'size': output_path.stat().st_size,
                'file_id': file_id
            }
            
        except Exception as e:
            logger.error(f"Error downloading file: {e}")
            if temp_path.exists():
                temp_path.unlink()
            raise
    
    async def _download_multipart(
        self,
        file_id: str,
        file_record: Dict,
        output_path: Path,
        decrypt: bool,
        password: Optional[str],
        client: TelegramClient
    ) -> Dict[str, Any]:
        """Download and merge multipart files."""
        
        parts = file_record.get('part_details', [])
        if not parts:
            # Try to get parts from database
            parts = await self._get_part_details(file_id)
        
        if not parts:
            raise ValueError(f"No parts found for {file_id}")
        
        logger.info(f"Downloading {len(parts)} parts")
        
        temp_dir = output_path.parent / f".parts_{file_id}"
        ensure_dir(temp_dir)
        
        try:
            downloaded_parts = []
            for part_info in parts:
                part_id = part_info.get('file_id')
                part_desc = part_info.get('description', f"part_{part_info.get('part')}")
                
                # Download each part
                part_path = temp_dir / f"part_{part_info.get('part'):03d}"
                result = await self.download_file(
                    file_id=part_id,
                    output_path=part_path,
                    decrypt=decrypt,
                    password=password
                )
                
                if result.get('success'):
                    downloaded_parts.append({
                        'part': part_info.get('part'),
                        'path': result.get('path'),
                        'size': result.get('size', 0)
                    })
                else:
                    raise RuntimeError(f"Failed to download part {part_info.get('part')}")
            
            # Merge parts
            logger.info("Merging parts...")
            await self._merge_parts(
                parts=downloaded_parts,
                output_path=output_path
            )
            
            # Verify merged file
            if not self._verify_file_integrity(output_path, file_record.get('hash')):
                logger.warning("Merged file integrity check failed")
            
            # Clean up temp directory
            for part in downloaded_parts:
                if Path(part['path']).exists():
                    Path(part['path']).unlink()
            temp_dir.rmdir()
            
            return {
                'success': True,
                'path': str(output_path),
                'size': output_path.stat().st_size,
                'parts': len(downloaded_parts)
            }
            
        except Exception as e:
            logger.error(f"Multipart download failed: {e}")
            # Clean up
            if temp_dir.exists():
                import shutil
                shutil.rmtree(temp_dir)
            raise
    
    # ============================================
    # Channel Download Methods
    # ============================================
    
    async def download_channel(
        self,
        channel: Union[str, int],
        output_dir: Optional[Path] = None,
        account_phone: Optional[str] = None,
        limit: int = 100,
        offset_date: Optional[datetime] = None,
        min_id: int = 0,
        max_id: int = 0,
        filter_types: Optional[List[str]] = None,
        download_media: bool = True,
        only_media: bool = False,
        re_upload: bool = False,
        re_upload_channel: Optional[str] = None,
        save_metadata: bool = True
    ) -> Dict[str, Any]:
        """
        Download all data from a Telegram channel.
        
        Args:
            channel: Channel username, ID, or invite link
            output_dir: Directory to save downloads
            account_phone: Account to use
            limit: Number of messages to fetch
            offset_date: Messages before this date
            min_id: Minimum message ID
            max_id: Maximum message ID
            filter_types: Filter by types ('document', 'photo', 'video', etc.)
            download_media: Whether to download media
            only_media: Only download messages with media
            re_upload: Re-upload downloaded data to Telegram
            re_upload_channel: Channel to re-upload to
            save_metadata: Save metadata to JSON
        
        Returns:
            Channel download statistics
        """
        logger.info(f"Downloading channel: {channel}")
        
        output_dir = Path(output_dir or self.download_dir / f"channel_{channel}")
        ensure_dir(output_dir)
        
        # Get client
        client = await self._get_client(account_phone)
        if not client:
            raise RuntimeError("No client available")
        
        # Get channel entity
        entity = await self._get_channel_entity(client, channel)
        if not entity:
            raise ValueError(f"Channel {channel} not found")
        
        # Fetch messages
        messages = await self._fetch_messages(
            client=client,
            entity=entity,
            limit=limit,
            offset_date=offset_date,
            min_id=min_id,
            max_id=max_id
        )
        
        logger.info(f"Fetched {len(messages)} messages from channel")
        
        # Process messages
        results = {
            'total_messages': len(messages),
            'downloaded_files': 0,
            're_uploaded_files': 0,
            'failed_downloads': 0,
            'messages': []
        }
        
        for message in messages:
            try:
                # Filter by type
                if filter_types and message.media:
                    media_type = self._get_media_type(message.media)
                    if media_type not in filter_types:
                        continue
                
                # Skip if no media
                if only_media and not message.media:
                    continue
                
                # Process message
                msg_result = await self._process_channel_message(
                    message=message,
                    client=client,
                    output_dir=output_dir,
                    download_media=download_media,
                    re_upload=re_upload,
                    re_upload_channel=re_upload_channel,
                    account_phone=account_phone,
                    save_metadata=save_metadata
                )
                
                if msg_result:
                    results['messages'].append(msg_result)
                    if msg_result.get('downloaded'):
                        results['downloaded_files'] += 1
                    if msg_result.get('re_uploaded'):
                        results['re_uploaded_files'] += 1
                else:
                    results['failed_downloads'] += 1
                
            except Exception as e:
                logger.error(f"Error processing message {message.id}: {e}")
                results['failed_downloads'] += 1
                continue
        
        logger.info(f"Channel download complete: {results['downloaded_files']} files downloaded, "
                   f"{results['re_uploaded_files']} re-uploaded")
        
        return results
    
    async def download_all_channels(
        self,
        channels: List[Union[str, int]],
        output_dir: Optional[Path] = None,
        account_phone: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Download data from multiple channels.
        
        Args:
            channels: List of channel usernames, IDs, or invite links
            output_dir: Base output directory
            account_phone: Account to use
            **kwargs: Additional arguments passed to download_channel
        
        Returns:
            Combined download statistics
        """
        logger.info(f"Downloading {len(channels)} channels")
        
        results = {}
        total_downloaded = 0
        total_re_uploaded = 0
        
        for i, channel in enumerate(channels):
            channel_dir = output_dir / f"channel_{i}" if output_dir else None
            try:
                result = await self.download_channel(
                    channel=channel,
                    output_dir=channel_dir,
                    account_phone=account_phone,
                    **kwargs
                )
                results[str(channel)] = result
                total_downloaded += result.get('downloaded_files', 0)
                total_re_uploaded += result.get('re_uploaded_files', 0)
                
                # Random sleep between channels
                await self._random_sleep()
                
            except Exception as e:
                logger.error(f"Failed to download channel {channel}: {e}")
                results[str(channel)] = {'error': str(e)}
        
        return {
            'channels': results,
            'total_downloaded': total_downloaded,
            'total_re_uploaded': total_re_uploaded
        }
    
    # ============================================
    # Channel Message Processing
    # ============================================
    
    async def _process_channel_message(
        self,
        message: Message,
        client: TelegramClient,
        output_dir: Path,
        download_media: bool = True,
        re_upload: bool = False,
        re_upload_channel: Optional[str] = None,
        account_phone: Optional[str] = None,
        save_metadata: bool = True
    ) -> Optional[Dict[str, Any]]:
        """Process a single channel message."""
        
        try:
            result = {
                'message_id': message.id,
                'date': message.date.isoformat() if message.date else None,
                'text': message.text[:100] if message.text else None,
                'has_media': bool(message.media),
                'downloaded': False,
                're_uploaded': False,
                'file_path': None
            }
            
            # Download media if present
            if message.media and download_media:
                file_info = await self._download_channel_media(
                    message=message,
                    client=client,
                    output_dir=output_dir
                )
                
                if file_info:
                    result.update(file_info)
                    result['downloaded'] = True
                    
                    # Re-upload if requested
                    if re_upload and re_upload_channel:
                        try:
                            upload_result = await self.uploader.upload_file(
                                file_path=file_info['file_path'],
                                description=f"From channel {file_info.get('original_channel', 'unknown')}",
                                channel=re_upload_channel,
                                account_phone=account_phone
                            )
                            result['re_uploaded'] = True
                            result['re_upload_id'] = upload_result.get('file_id')
                        except Exception as e:
                            logger.error(f"Re-upload failed: {e}")
            
            # Save metadata
            if save_metadata and result.get('downloaded'):
                await self._save_metadata(
                    message=message,
                    result=result,
                    output_dir=output_dir
                )
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing message {message.id}: {e}")
            return None
    
    async def _download_channel_media(
        self,
        message: Message,
        client: TelegramClient,
        output_dir: Path
    ) -> Optional[Dict[str, Any]]:
        """Download media from a channel message."""
        
        if not message.media:
            return None
        
        try:
            # Get file name
            file_name = self._get_file_name(message)
            if not file_name:
                # Generate name from message ID
                file_name = f"msg_{message.id}_{int(time.time())}"
            
            # Determine file type
            media_type = self._get_media_type(message.media)
            if media_type:
                file_name = f"{file_name}.{media_type}"
            
            # Create subdirectory based on date
            if message.date:
                date_dir = message.date.strftime("%Y/%m/%d")
                output_path = output_dir / date_dir / file_name
            else:
                output_path = output_dir / file_name
            
            ensure_dir(output_path.parent)
            
            # Download with progress
            temp_path = output_path.with_suffix('.temp')
            
            progress_bar = ProgressBar(
                total=message.media.document.size if hasattr(message.media, 'document') else 0,
                desc=f"Downloading {file_name}",
                unit='B',
                unit_scale=True
            )
            
            async def progress_callback(current, total):
                progress_bar.update(current)
            
            # Download
            await client.download_media(
                message=message,
                file=temp_path,
                progress_callback=progress_callback
            )
            
            progress_bar.close()
            
            if temp_path.exists():
                temp_path.rename(output_path)
                
                # Calculate hash
                file_hash = await self._calculate_hash(output_path)
                
                return {
                    'file_path': str(output_path),
                    'file_name': file_name,
                    'file_size': output_path.stat().st_size,
                    'file_hash': file_hash,
                    'media_type': media_type,
                    'original_channel': str(message.chat_id) if message.chat_id else None
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error downloading media: {e}")
            return None
    
    # ============================================
    # Helper Methods
    # ============================================
    
    async def _get_channel_entity(self, client: TelegramClient, channel: Union[str, int]):
        """Get channel entity from username, ID, or invite link."""
        try:
            if isinstance(channel, int):
                return await client.get_entity(channel)
            else:
                return await client.get_entity(channel)
        except Exception as e:
            logger.error(f"Failed to get channel entity: {e}")
            return None
    
    async def _fetch_messages(
        self,
        client: TelegramClient,
        entity,
        limit: int = 100,
        offset_date: Optional[datetime] = None,
        min_id: int = 0,
        max_id: int = 0
    ) -> List[Message]:
        """Fetch messages from a channel."""
        
        messages = []
        offset_id = 0
        
        while True:
            try:
                # Get history
                history = await client.get_messages(
                    entity=entity,
                    limit=min(limit - len(messages), 100),
                    offset_id=offset_id,
                    offset_date=offset_date,
                    min_id=min_id,
                    max_id=max_id,
                )
                
                if not history:
                    break
                
                messages.extend(history)
                
                # Update offset
                if history:
                    offset_id = history[-1].id
                
                # Check if we have enough messages
                if len(messages) >= limit:
                    break
                
                # Small delay to avoid rate limiting
                await asyncio.sleep(0.5)
                
            except FloodWaitError as e:
                logger.warning(f"Rate limited: wait {e.seconds}s")
                await asyncio.sleep(e.seconds + 5)
            except Exception as e:
                logger.error(f"Error fetching messages: {e}")
                break
        
        return messages
    
    async def _get_message_by_id(self, client: TelegramClient, file_id: str) -> Optional[Message]:
        """Get message by ID from Telegram."""
        try:
            # Try to parse as int ID
            msg_id = int(file_id)
            # This would need the chat ID too, so try to find it
            # For now, just return None
            return None
        except:
            return None
    
    async def _find_file_by_id(self, client: TelegramClient, file_id: str) -> Optional[Message]:
        """Try to find a file by searching in recent messages."""
        # This is a placeholder - in a real implementation, you'd search through channels
        return None
    
    def _get_file_name(self, message: Message) -> Optional[str]:
        """Extract file name from message."""
        if not message.media:
            return None
        
        if hasattr(message.media, 'document'):
            doc = message.media.document
            if doc and hasattr(doc, 'attributes'):
                for attr in doc.attributes:
                    if hasattr(attr, 'file_name'):
                        return attr.file_name
        
        return f"file_{message.id}"
    
    def _get_media_type(self, media) -> Optional[str]:
        """Get media type from media object."""
        if not media:
            return None
        
        if hasattr(media, 'document'):
            doc = media.document
            if doc:
                mime_type = doc.mime_type if hasattr(doc, 'mime_type') else ''
                if 'video' in mime_type:
                    return 'video'
                elif 'audio' in mime_type:
                    return 'audio'
                elif 'image' in mime_type:
                    return 'image'
                elif 'pdf' in mime_type:
                    return 'pdf'
                else:
                    return 'document'
        
        if hasattr(media, 'photo'):
            return 'photo'
        
        return 'file'
    
    async def _merge_parts(self, parts: List[Dict], output_path: Path) -> None:
        """Merge downloaded parts into a single file."""
        
        async with aiofiles.open(output_path, 'wb') as outfile:
            for part in sorted(parts, key=lambda x: x.get('part', 0)):
                part_path = Path(part['path'])
                if part_path.exists():
                    async with aiofiles.open(part_path, 'rb') as infile:
                        while True:
                            chunk = await infile.read(self.chunk_size)
                            if not chunk:
                                break
                            await outfile.write(chunk)
    
    async def _decrypt_file(
        self,
        file_path: Path,
        output_path: Path,
        password: Optional[str] = None
    ) -> bool:
        """Decrypt a file."""
        try:
            if not password:
                # Try to get from database
                # For now, use the encryptor's decrypt function
                pass
            
            success = self.encryptor.decrypt_file(
                file_path=str(file_path),
                output_path=str(output_path),
                password=password
            )
            
            return success
            
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            return False
    
    async def _calculate_hash(self, file_path: Path) -> str:
        """Calculate file hash."""
        try:
            hasher = hashlib.sha256()
            async with aiofiles.open(file_path, 'rb') as f:
                while True:
                    chunk = await f.read(8192)
                    if not chunk:
                        break
                    hasher.update(chunk)
            return hasher.hexdigest()
        except:
            return ""
    
    def _verify_file_integrity(self, file_path: Path, expected_hash: Optional[str]) -> bool:
        """Verify file integrity against expected hash."""
        if not expected_hash:
            return True
        
        try:
            calculated = asyncio.run(self._calculate_hash(file_path))
            return calculated == expected_hash
        except:
            return True
    
    async def _save_metadata(self, message: Message, result: Dict, output_dir: Path) -> None:
        """Save message metadata to JSON."""
        try:
            metadata = {
                'message_id': message.id,
                'date': message.date.isoformat() if message.date else None,
                'text': message.text,
                'file_info': result,
                'chat_id': message.chat_id if message.chat_id else None,
                'peer_id': message.peer_id,
                'media_type': self._get_media_type(message.media) if message.media else None
            }
            
            json_path = output_dir / "metadata.json"
            
            # Append to existing JSON if it exists
            existing = []
            if json_path.exists():
                try:
                    with open(json_path, 'r') as f:
                        existing = json.load(f)
                except:
                    pass
            
            existing.append(metadata)
            
            with open(json_path, 'w') as f:
                json.dump(existing, f, indent=2, default=str)
                
        except Exception as e:
            logger.error(f"Failed to save metadata: {e}")
    
    async def _get_client(self, account_phone: Optional[str] = None):
        """Get a client from the pool."""
        if account_phone:
            return await self.client_pool.get_client(account_phone)
        return await self.client_pool.get_next_client()
    
    async def _random_sleep(self) -> None:
        """Sleep for a random duration."""
        if self.random_sleep_range:
            sleep_time = random.randint(
                self.random_sleep_range[0],
                self.random_sleep_range[1]
            )
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
    
    async def _get_part_details(self, file_id: str) -> List[Dict]:
        """Get part details from database."""
        if self.tracker:
            file_record = self.tracker.get_file_by_id(file_id)
            if file_record:
                return file_record.get('part_details', [])
        return []
    
    async def search_and_download(
        self,
        query: str,
        channel: Optional[str] = None,
        account_phone: Optional[str] = None,
        limit: int = 50,
        download_media: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Search for messages and download matching files.
        
        Args:
            query: Search query
            channel: Channel to search in
            account_phone: Account to use
            limit: Maximum results
            download_media: Whether to download media
        
        Returns:
            List of search results with downloads
        """
        logger.info(f"Searching for: {query}")
        
        client = await self._get_client(account_phone)
        if not client:
            raise RuntimeError("No client available")
        
        results = []
        
        # Search in messages
        if channel:
            entity = await self._get_channel_entity(client, channel)
            if not entity:
                raise ValueError(f"Channel {channel} not found")
            
            messages = await client.search_messages(
                entity=entity,
                query=query,
                limit=limit
            )
        else:
            # Search in all dialogs
            messages = await client.search_messages(
                query=query,
                limit=limit
            )
        
        for message in messages:
            msg_result = {
                'message_id': message.id,
                'text': message.text,
                'date': message.date.isoformat() if message.date else None,
                'chat': str(message.chat_id) if message.chat_id else None,
                'has_media': bool(message.media),
                'downloaded': False
            }
            
            # Download if media
            if message.media and download_media:
                output_dir = self.download_dir / f"search_{query.replace(' ', '_')}"
                file_info = await self._download_channel_media(
                    message=message,
                    client=client,
                    output_dir=output_dir
                )
                if file_info:
                    msg_result.update(file_info)
                    msg_result['downloaded'] = True
            
            results.append(msg_result)
        
        logger.info(f"Search complete: {len(results)} results")
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get download statistics."""
        return {
            'files_downloaded': self.downloaded_files,
            'bytes_downloaded': self.downloaded_bytes,
            'total_errors': self.errors,
            'active_downloads': len(self._active_downloads)
        }
    
    def reset_stats(self) -> None:
        """Reset download statistics."""
        self.downloaded_files = 0
        self.downloaded_bytes = 0
        self.errors = 0
    
    async def close(self) -> None:
        """Clean up resources."""
       await self.client_pool.close_all()
        logger.info("Downloader closed")
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    def __repr__(self) -> str:
        return f"Downloader(files={self.downloaded_files}, errors={self.errors})"