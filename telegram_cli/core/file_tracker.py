#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
telegram_cli/core/file_tracker.py – Track all file operations with SQLite

Handles:
- Recording file uploads with all metadata (ID, size, hash, description, parts)
- Recording file downloads
- Searching files by ID, description, name, date, tags
- Getting file statistics and history
- Tracking file parts for split files
- File version history
- Export file records to JSON/CSV
- Maintaining unique descriptions per file
"""

import json
import hashlib
from pathlib import Path
from typing import Optional, List, Dict, Any, Union, Tuple
from datetime import datetime, timedelta
from collections import defaultdict
import sqlite3
import aiosqlite

# Import core modules
try:
    from telegram_cli.database.db_manager import DatabaseManager
    from telegram_cli.utils.logger import get_logger
    from telegram_cli.utils.helpers import get_timestamp, format_size
    MODULES_LOADED = True
except ImportError:
    MODULES_LOADED = False
    import logging
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.NullHandler())
    
    class DatabaseManager:
        def __init__(self): pass
        def init_db(self): pass
        def execute_query(self, query, params=None): return []
        def execute_write(self, query, params=None): pass
        def close(self): pass
    def get_timestamp(): return datetime.now().isoformat()
    def format_size(s): return f"{s/1024:.2f} KB"

logger = get_logger(__name__)


class FileTracker:
    """
    Tracks all file operations with persistent SQLite storage.
    Every upload and download is recorded with complete metadata.
    """
    
    def __init__(
        self,
        db_manager: Optional[DatabaseManager] = None,
        db_path: Optional[Path] = None
    ):
        """
        Initialize the file tracker.
        
        Args:
            db_manager: DatabaseManager instance (creates one if None)
            db_path: Path to SQLite database file
        """
        self.db_manager = db_manager or DatabaseManager(db_path)
        self._cache = {}
        self._description_cache = set()
        
        # Initialize database
        self._init_tables()
        
        # Load description cache for uniqueness checking
        self._load_descriptions()
        
        logger.info("FileTracker initialized")
    
    def _init_tables(self) -> None:
        """Initialize database tables for file tracking."""
        
        # File records table
        create_file_records = """
        CREATE TABLE IF NOT EXISTS file_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id TEXT UNIQUE NOT NULL,
            file_name TEXT NOT NULL,
            file_size INTEGER NOT NULL,
            description TEXT UNIQUE,
            hash TEXT,
            account_phone TEXT,
            channel TEXT,
            upload_date TEXT,
            download_date TEXT,
            parts INTEGER DEFAULT 1,
            part_details TEXT,  -- JSON array of part info
            encrypted BOOLEAN DEFAULT 0,
            encryption_info TEXT,  -- JSON
            tags TEXT,  -- JSON array
            metadata TEXT,  -- JSON
            ip TEXT,
            download_count INTEGER DEFAULT 0,
            last_accessed TEXT,
            version INTEGER DEFAULT 1,
            parent_id TEXT,  -- For file versions
            status TEXT DEFAULT 'active',  -- active, deleted, archived
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        # File parts table (for tracking individual parts)
        create_file_parts = """
        CREATE TABLE IF NOT EXISTS file_parts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id TEXT NOT NULL,
            part_number INTEGER NOT NULL,
            part_file_id TEXT NOT NULL,
            part_size INTEGER,
            part_hash TEXT,
            upload_date TEXT,
            FOREIGN KEY (file_id) REFERENCES file_records(file_id)
        )
        """
        
        # Downloads table
        create_downloads = """
        CREATE TABLE IF NOT EXISTS downloads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id TEXT NOT NULL,
            account_phone TEXT,
            download_date TEXT NOT NULL,
            file_path TEXT,
            file_size INTEGER,
            success BOOLEAN DEFAULT 1,
            error_message TEXT,
            FOREIGN KEY (file_id) REFERENCES file_records(file_id)
        )
        """
        
        # File tags table (for efficient tag searching)
        create_tags = """
        CREATE TABLE IF NOT EXISTS file_tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id TEXT NOT NULL,
            tag TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (file_id) REFERENCES file_records(file_id),
            UNIQUE(file_id, tag)
        )
        """
        
        # Indexes for performance
        create_indexes = """
        CREATE INDEX IF NOT EXISTS idx_file_records_file_id ON file_records(file_id);
        CREATE INDEX IF NOT EXISTS idx_file_records_description ON file_records(description);
        CREATE INDEX IF NOT EXISTS idx_file_records_account ON file_records(account_phone);
        CREATE INDEX IF NOT EXISTS idx_file_records_date ON file_records(upload_date);
        CREATE INDEX IF NOT EXISTS idx_file_parts_file_id ON file_parts(file_id);
        CREATE INDEX IF NOT EXISTS idx_file_tags_file_id ON file_tags(file_id);
        CREATE INDEX IF NOT EXISTS idx_file_tags_tag ON file_tags(tag);
        CREATE INDEX IF NOT EXISTS idx_downloads_file_id ON downloads(file_id);
        """
        
        # Execute all queries
        try:
            self.db_manager.execute_write(create_file_records)
            self.db_manager.execute_write(create_file_parts)
            self.db_manager.execute_write(create_downloads)
            self.db_manager.execute_write(create_tags)
            self.db_manager.execute_write(create_indexes)
            logger.info("File tracker tables initialized")
        except Exception as e:
            logger.error(f"Failed to initialize tables: {e}")
            raise
    
    def _load_descriptions(self) -> None:
        """Load all existing descriptions for uniqueness checking."""
        try:
            query = "SELECT description FROM file_records WHERE description IS NOT NULL"
            rows = self.db_manager.execute_query(query)
            self._description_cache = {row[0] for row in rows if row[0]}
            logger.debug(f"Loaded {len(self._description_cache)} descriptions")
        except Exception as e:
            logger.error(f"Failed to load descriptions: {e}")
            self._description_cache = set()
    
    def _is_description_unique(self, description: str) -> bool:
        """Check if a description is unique."""
        if not description:
            return True
        return description not in self._description_cache
    
    def _add_description_to_cache(self, description: str) -> None:
        """Add a description to the cache."""
        if description:
            self._description_cache.add(description)
    
    def _remove_description_from_cache(self, description: str) -> None:
        """Remove a description from the cache."""
        if description in self._description_cache:
            self._description_cache.remove(description)
    
    # ============================================
    # Record Uploads
    # ============================================
    
    def record_upload(
        self,
        file_id: str,
        file_name: str,
        file_size: int,
        description: Optional[str] = None,
        hash: Optional[str] = None,
        account_phone: Optional[str] = None,
        channel: Optional[str] = None,
        parts: int = 1,
        part_details: Optional[List[Dict]] = None,
        encrypted: bool = False,
        encryption_info: Optional[Dict] = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict] = None,
        ip: Optional[str] = None,
        parent_id: Optional[str] = None,
        version: int = 1
    ) -> bool:
        """
        Record a file upload in the database.
        
        Args:
            file_id: Unique file ID (from Telegram or generated)
            file_name: Original file name
            file_size: File size in bytes
            description: User-provided description (must be unique)
            hash: File hash (SHA256)
            account_phone: Account used for upload
            channel: Channel where file was uploaded
            parts: Number of parts (1 if not split)
            part_details: List of part information
            encrypted: Whether file is encrypted
            encryption_info: Encryption details (algorithm, salt, etc.)
            tags: List of tags
            metadata: Additional metadata
            ip: User IP address
            parent_id: Parent file ID (for versions)
            version: File version number
        
        Returns:
            True if recorded successfully
        """
        # Validate description uniqueness
        if description and not self._is_description_unique(description):
            logger.warning(f"Description '{description}' already exists, generating unique one")
            description = f"{description}_{int(datetime.now().timestamp())}"
        
        # Prepare JSON fields
        part_details_json = json.dumps(part_details) if part_details else None
        encryption_info_json = json.dumps(encryption_info) if encryption_info else None
        tags_json = json.dumps(tags) if tags else None
        metadata_json = json.dumps(metadata) if metadata else None
        
        # Current timestamp
        timestamp = get_timestamp()
        
        # Check if file already exists
        existing = self.get_file_by_id(file_id)
        if existing:
            logger.warning(f"File {file_id} already exists, updating instead")
            return self.update_file_record(
                file_id=file_id,
                file_name=file_name,
                file_size=file_size,
                description=description,
                hash=hash,
                account_phone=account_phone,
                channel=channel,
                parts=parts,
                part_details=part_details,
                encrypted=encrypted,
                encryption_info=encryption_info,
                tags=tags,
                metadata=metadata,
                ip=ip
            )
        
        # Insert file record
        query = """
        INSERT INTO file_records (
            file_id, file_name, file_size, description, hash,
            account_phone, channel, upload_date, parts, part_details,
            encrypted, encryption_info, tags, metadata, ip,
            parent_id, version, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        params = (
            file_id, file_name, file_size, description, hash,
            account_phone, channel, timestamp, parts, part_details_json,
            1 if encrypted else 0, encryption_info_json, tags_json, metadata_json, ip,
            parent_id, version, timestamp, timestamp
        )
        
        try:
            self.db_manager.execute_write(query, params)
            self._add_description_to_cache(description)
            
            # Insert parts if any
            if part_details:
                self._insert_parts(file_id, part_details)
            
            # Insert tags if any
            if tags:
                self._insert_tags(file_id, tags)
            
            logger.info(f"Recorded upload: {file_name} (ID: {file_id}, Size: {format_size(file_size)})")
            return True
            
        except sqlite3.IntegrityError as e:
            logger.error(f"Failed to record upload (integrity error): {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to record upload: {e}")
            return False
    
    def _insert_parts(self, file_id: str, part_details: List[Dict]) -> None:
        """Insert part details for a file."""
        for part in part_details:
            query = """
            INSERT INTO file_parts (
                file_id, part_number, part_file_id, part_size, part_hash, upload_date
            ) VALUES (?, ?, ?, ?, ?, ?)
            """
            params = (
                file_id,
                part.get('part', 0),
                part.get('file_id', ''),
                part.get('size', 0),
                part.get('hash', ''),
                get_timestamp()
            )
            self.db_manager.execute_write(query, params)
    
    def _insert_tags(self, file_id: str, tags: List[str]) -> None:
        """Insert tags for a file."""
        for tag in tags:
            query = "INSERT OR IGNORE INTO file_tags (file_id, tag) VALUES (?, ?)"
            self.db_manager.execute_write(query, (file_id, tag.lower().strip()))
    
    # ============================================
    # Record Downloads
    # ============================================
    
    def record_download(
        self,
        file_id: str,
        file_name: Optional[str] = None,
        file_size: Optional[int] = None,
        account_phone: Optional[str] = None,
        file_path: Optional[str] = None,
        success: bool = True,
        error_message: Optional[str] = None
    ) -> bool:
        """
        Record a file download.
        
        Args:
            file_id: File ID
            file_name: Downloaded file name
            file_size: Downloaded file size
            account_phone: Account used for download
            file_path: Local file path
            success: Whether download succeeded
            error_message: Error message if failed
        
        Returns:
            True if recorded successfully
        """
        query = """
        INSERT INTO downloads (
            file_id, account_phone, download_date, file_path,
            file_size, success, error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        
        params = (
            file_id,
            account_phone,
            get_timestamp(),
            file_path,
            file_size or 0,
            1 if success else 0,
            error_message
        )
        
        try:
            self.db_manager.execute_write(query, params)
            
            # Update download count in file_records
            update_query = """
            UPDATE file_records 
            SET download_count = download_count + 1,
                last_accessed = ?,
                updated_at = ?
            WHERE file_id = ?
            """
            timestamp = get_timestamp()
            self.db_manager.execute_write(update_query, (timestamp, timestamp, file_id))
            
            # Update file name if provided
            if file_name:
                name_query = "UPDATE file_records SET file_name = ? WHERE file_id = ?"
                self.db_manager.execute_write(name_query, (file_name, file_id))
            
            logger.info(f"Recorded download for: {file_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to record download: {e}")
            return False
    
    # ============================================
    # File Retrieval Methods
    # ============================================
    
    def get_file_by_id(self, file_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a file record by its ID.
        
        Args:
            file_id: File ID to look up
        
        Returns:
            File record dict or None if not found
        """
        # Check cache
        if file_id in self._cache:
            return self._cache[file_id].copy()
        
        query = "SELECT * FROM file_records WHERE file_id = ?"
        row = self.db_manager.execute_query(query, (file_id,))
        
        if not row:
            return None
        
        record = self._row_to_dict(row[0])
        self._cache[file_id] = record
        return record.copy()
    
    def get_file_by_description(self, description: str) -> Optional[Dict[str, Any]]:
        """
        Get a file record by its description.
        
        Args:
            description: File description
        
        Returns:
            File record dict or None if not found
        """
        query = "SELECT * FROM file_records WHERE description = ?"
        row = self.db_manager.execute_query(query, (description,))
        
        if not row:
            return None
        
        return self._row_to_dict(row[0])
    
    def search_files(
        self,
        query: Optional[str] = None,
        description: Optional[str] = None,
        file_name: Optional[str] = None,
        account_phone: Optional[str] = None,
        channel: Optional[str] = None,
        tags: Optional[List[str]] = None,
        min_size: Optional[int] = None,
        max_size: Optional[int] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        encrypted: Optional[bool] = None,
        status: str = 'active',
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Search file records with multiple filters.
        
        Args:
            query: Search in file_name, description, tags
            description: Exact description match
            file_name: Search in file_name (partial match)
            account_phone: Filter by account
            channel: Filter by channel
            tags: Filter by tags (AND condition)
            min_size: Minimum file size in bytes
            max_size: Maximum file size in bytes
            start_date: Files uploaded after this date
            end_date: Files uploaded before this date
            encrypted: Filter by encryption status
            status: File status (active, deleted, archived)
            limit: Max results
            offset: Result offset for pagination
        
        Returns:
            List of file record dicts
        """
        conditions = []
        params = []
        
        if status:
            conditions.append("status = ?")
            params.append(status)
        
        if query:
            conditions.append("(file_name LIKE ? OR description LIKE ?)")
            params.extend([f"%{query}%", f"%{query}%"])
        
        if description:
            conditions.append("description = ?")
            params.append(description)
        
        if file_name:
            conditions.append("file_name LIKE ?")
            params.append(f"%{file_name}%")
        
        if account_phone:
            conditions.append("account_phone = ?")
            params.append(account_phone)
        
        if channel:
            conditions.append("channel LIKE ?")
            params.append(f"%{channel}%")
        
        if min_size is not None:
            conditions.append("file_size >= ?")
            params.append(min_size)
        
        if max_size is not None:
            conditions.append("file_size <= ?")
            params.append(max_size)
        
        if start_date:
            conditions.append("upload_date >= ?")
            params.append(start_date.isoformat())
        
        if end_date:
            conditions.append("upload_date <= ?")
            params.append(end_date.isoformat())
        
        if encrypted is not None:
            conditions.append("encrypted = ?")
            params.append(1 if encrypted else 0)
        
        # Build query
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        query_str = f"""
            SELECT * FROM file_records 
            WHERE {where_clause}
            ORDER BY upload_date DESC
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])
        
        rows = self.db_manager.execute_query(query_str, tuple(params))
        
        results = []
        for row in rows:
            record = self._row_to_dict(row)
            
            # Filter by tags (post-query filtering for AND condition)
            if tags:
                record_tags = json.loads(record.get('tags', '[]'))
                if not all(tag.lower() in [t.lower() for t in record_tags] for tag in tags):
                    continue
            
            results.append(record)
        
        return results
    
    def get_files_by_account(self, account_phone: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get all files uploaded by a specific account.
        
        Args:
            account_phone: Account phone number
            limit: Max results
        
        Returns:
            List of file records
        """
        query = """
        SELECT * FROM file_records 
        WHERE account_phone = ? 
        ORDER BY upload_date DESC 
        LIMIT ?
        """
        rows = self.db_manager.execute_query(query, (account_phone, limit))
        return [self._row_to_dict(row) for row in rows]
    
    def get_files_by_channel(self, channel: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get all files in a specific channel.
        
        Args:
            channel: Channel name/ID
            limit: Max results
        
        Returns:
            List of file records
        """
        query = """
        SELECT * FROM file_records 
        WHERE channel = ? 
        ORDER BY upload_date DESC 
        LIMIT ?
        """
        rows = self.db_manager.execute_query(query, (channel, limit))
        return [self._row_to_dict(row) for row in rows]
    
    def get_files_by_tag(self, tag: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get all files with a specific tag.
        
        Args:
            tag: Tag to search
            limit: Max results
        
        Returns:
            List of file records
        """
        query = """
        SELECT fr.* FROM file_records fr
        JOIN file_tags ft ON fr.file_id = ft.file_id
        WHERE ft.tag = ?
        ORDER BY fr.upload_date DESC
        LIMIT ?
        """
        rows = self.db_manager.execute_query(query, (tag.lower().strip(), limit))
        return [self._row_to_dict(row) for row in rows]
    
    def get_file_parts(self, file_id: str) -> List[Dict[str, Any]]:
        """
        Get all parts of a split file.
        
        Args:
            file_id: Master file ID
        
        Returns:
            List of part details
        """
        query = "SELECT * FROM file_parts WHERE file_id = ? ORDER BY part_number"
        rows = self.db_manager.execute_query(query, (file_id,))
        return [dict(row) for row in rows]
    
    def get_file_downloads(self, file_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get download history for a file.
        
        Args:
            file_id: File ID
            limit: Max results
        
        Returns:
            List of download records
        """
        query = """
        SELECT * FROM downloads 
        WHERE file_id = ? 
        ORDER BY download_date DESC 
        LIMIT ?
        """
        rows = self.db_manager.execute_query(query, (file_id, limit))
        return [dict(row) for row in rows]
    
    # ============================================
    # File Update Methods
    # ============================================
    
    def update_file_record(
        self,
        file_id: str,
        file_name: Optional[str] = None,
        file_size: Optional[int] = None,
        description: Optional[str] = None,
        hash: Optional[str] = None,
        account_phone: Optional[str] = None,
        channel: Optional[str] = None,
        parts: Optional[int] = None,
        part_details: Optional[List[Dict]] = None,
        encrypted: Optional[bool] = None,
        encryption_info: Optional[Dict] = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict] = None,
        ip: Optional[str] = None,
        status: Optional[str] = None
    ) -> bool:
        """
        Update a file record.
        
        Args:
            file_id: File ID to update
            **kwargs: Fields to update
        
        Returns:
            True if updated successfully
        """
        updates = []
        params = []
        timestamp = get_timestamp()
        
        # Build update fields
        if file_name is not None:
            updates.append("file_name = ?")
            params.append(file_name)
        
        if file_size is not None:
            updates.append("file_size = ?")
            params.append(file_size)
        
        if description is not None:
            # Check uniqueness
            if not self._is_description_unique(description):
                logger.warning(f"Description '{description}' already exists")
                return False
            updates.append("description = ?")
            params.append(description)
            self._add_description_to_cache(description)
        
        if hash is not None:
            updates.append("hash = ?")
            params.append(hash)
        
        if account_phone is not None:
            updates.append("account_phone = ?")
            params.append(account_phone)
        
        if channel is not None:
            updates.append("channel = ?")
            params.append(channel)
        
        if parts is not None:
            updates.append("parts = ?")
            params.append(parts)
        
        if part_details is not None:
            updates.append("part_details = ?")
            params.append(json.dumps(part_details))
        
        if encrypted is not None:
            updates.append("encrypted = ?")
            params.append(1 if encrypted else 0)
        
        if encryption_info is not None:
            updates.append("encryption_info = ?")
            params.append(json.dumps(encryption_info))
        
        if tags is not None:
            updates.append("tags = ?")
            params.append(json.dumps(tags))
            # Update tags table
            self._update_tags(file_id, tags)
        
        if metadata is not None:
            updates.append("metadata = ?")
            params.append(json.dumps(metadata))
        
        if ip is not None:
            updates.append("ip = ?")
            params.append(ip)
        
        if status is not None:
            updates.append("status = ?")
            params.append(status)
        
        # Always update updated_at
        updates.append("updated_at = ?")
        params.append(timestamp)
        
        if not updates:
            logger.warning("No fields to update")
            return False
        
        # Add file_id to params
        params.append(file_id)
        
        # Execute update
        query = f"UPDATE file_records SET {', '.join(updates)} WHERE file_id = ?"
        
        try:
            self.db_manager.execute_write(query, tuple(params))
            
            # Clear cache
            if file_id in self._cache:
                del self._cache[file_id]
            
            logger.info(f"Updated file record: {file_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update file record: {e}")
            return False
    
    def _update_tags(self, file_id: str, tags: List[str]) -> None:
        """Update tags for a file."""
        # Remove existing tags
        delete_query = "DELETE FROM file_tags WHERE file_id = ?"
        self.db_manager.execute_write(delete_query, (file_id,))
        
        # Insert new tags
        if tags:
            self._insert_tags(file_id, tags)
    
    def delete_file_record(self, file_id: str, permanent: bool = False) -> bool:
        """
        Delete a file record.
        
        Args:
            file_id: File ID to delete
            permanent: If False, mark as deleted; if True, remove completely
        
        Returns:
            True if deleted successfully
        """
        if permanent:
            # Get description before deletion
            record = self.get_file_by_id(file_id)
            if record and record.get('description'):
                self._remove_description_from_cache(record['description'])
            
            # Delete all related records
            try:
                self.db_manager.execute_write("DELETE FROM file_parts WHERE file_id = ?", (file_id,))
                self.db_manager.execute_write("DELETE FROM file_tags WHERE file_id = ?", (file_id,))
                self.db_manager.execute_write("DELETE FROM downloads WHERE file_id = ?", (file_id,))
                self.db_manager.execute_write("DELETE FROM file_records WHERE file_id = ?", (file_id,))
                
                if file_id in self._cache:
                    del self._cache[file_id]
                
                logger.info(f"Permanently deleted file record: {file_id}")
                return True
                
            except Exception as e:
                logger.error(f"Failed to delete file record: {e}")
                return False
        else:
            # Soft delete - mark as deleted
            return self.update_file_record(file_id, status='deleted')
    
    # ============================================
    # Statistics and Reporting
    # ============================================
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get overall file statistics.
        
        Returns:
            Dictionary with statistics
        """
        stats = {
            'total_files': 0,
            'total_size': 0,
            'encrypted_files': 0,
            'total_downloads': 0,
            'accounts': {},
            'channels': {},
            'daily_uploads': {},
            'file_types': {},
            'average_size': 0
        }
        
        try:
            # Total files and size
            query = """
            SELECT 
                COUNT(*) as total_files,
                SUM(file_size) as total_size,
                SUM(CASE WHEN encrypted = 1 THEN 1 ELSE 0 END) as encrypted_count
            FROM file_records WHERE status = 'active'
            """
            row = self.db_manager.execute_query(query)
            if row:
                stats['total_files'] = row[0][0] or 0
                stats['total_size'] = row[0][1] or 0
                stats['encrypted_files'] = row[0][2] or 0
            
            # Downloads count
            query = "SELECT COUNT(*) FROM downloads WHERE success = 1"
            row = self.db_manager.execute_query(query)
            if row:
                stats['total_downloads'] = row[0][0] or 0
            
            # By account
            query = """
            SELECT account_phone, COUNT(*), SUM(file_size) 
            FROM file_records WHERE status = 'active'
            GROUP BY account_phone
            """
            rows = self.db_manager.execute_query(query)
            for row in rows:
                stats['accounts'][row[0] or 'unknown'] = {
                    'count': row[1],
                    'size': row[2] or 0
                }
            
            # By channel
            query = """
            SELECT channel, COUNT(*), SUM(file_size) 
            FROM file_records WHERE status = 'active'
            GROUP BY channel
            """
            rows = self.db_manager.execute_query(query)
            for row in rows:
                stats['channels'][row[0] or 'unknown'] = {
                    'count': row[1],
                    'size': row[2] or 0
                }
            
            # Daily uploads (last 30 days)
            query = """
            SELECT DATE(upload_date), COUNT(*), SUM(file_size)
            FROM file_records 
            WHERE status = 'active' 
              AND upload_date >= DATE('now', '-30 days')
            GROUP BY DATE(upload_date)
            ORDER BY DATE(upload_date)
            """
            rows = self.db_manager.execute_query(query)
            for row in rows:
                stats['daily_uploads'][row[0] or 'unknown'] = {
                    'count': row[1],
                    'size': row[2] or 0
                }
            
            # File types by extension
            query = """
            SELECT file_name, COUNT(*)
            FROM file_records WHERE status = 'active'
            GROUP BY file_name
            """
            rows = self.db_manager.execute_query(query)
            for row in rows:
                if row[0]:
                    ext = Path(row[0]).suffix.lower() or 'no_extension'
                    if ext not in stats['file_types']:
                        stats['file_types'][ext] = 0
                    stats['file_types'][ext] += row[1]
            
            # Average file size
            if stats['total_files'] > 0:
                stats['average_size'] = stats['total_size'] / stats['total_files']
            
        except Exception as e:
            logger.error(f"Failed to get statistics: {e}")
        
        return stats
    
    def get_account_stats(self, account_phone: str) -> Dict[str, Any]:
        """
        Get statistics for a specific account.
        
        Args:
            account_phone: Account phone number
        
        Returns:
            Account statistics
        """
        stats = {
            'account': account_phone,
            'total_files': 0,
            'total_size': 0,
            'files': []
        }
        
        try:
            query = """
            SELECT COUNT(*), SUM(file_size) 
            FROM file_records 
            WHERE account_phone = ? AND status = 'active'
            """
            row = self.db_manager.execute_query(query, (account_phone,))
            if row and row[0]:
                stats['total_files'] = row[0][0] or 0
                stats['total_size'] = row[0][1] or 0
            
            # Recent files
            files = self.get_files_by_account(account_phone, 10)
            stats['recent_files'] = files
            
        except Exception as e:
            logger.error(f"Failed to get account stats: {e}")
        
        return stats
    
    # ============================================
    # Export Methods
    # ============================================
    
    def export_to_json(
        self,
        output_path: Path,
        file_ids: Optional[List[str]] = None,
        include_parts: bool = True,
        include_downloads: bool = True
    ) -> bool:
        """
        Export file records to JSON.
        
        Args:
            output_path: Path for JSON file
            file_ids: List of file IDs to export (all if None)
            include_parts: Include part details
            include_downloads: Include download history
        
        Returns:
            True if exported successfully
        """
        try:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            export_data = {
                'export_time': get_timestamp(),
                'total_files': 0,
                'files': []
            }
            
            # Get files
            if file_ids:
                files = []
                for fid in file_ids:
                    record = self.get_file_by_id(fid)
                    if record:
                        files.append(record)
            else:
                query = "SELECT * FROM file_records WHERE status = 'active'"
                rows = self.db_manager.execute_query(query)
                files = [self._row_to_dict(row) for row in rows]
            
            export_data['total_files'] = len(files)
            
            # Add parts and downloads
            for file in files:
                if include_parts:
                    file['parts'] = self.get_file_parts(file['file_id'])
                if include_downloads:
                    file['downloads'] = self.get_file_downloads(file['file_id'], 50)
            
            export_data['files'] = files
            
            # Write to file
            with open(output_path, 'w') as f:
                json.dump(export_data, f, indent=2, default=str)
            
            logger.info(f"Exported {len(files)} files to JSON: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Export to JSON failed: {e}")
            return False
    
    def export_to_csv(
        self,
        output_path: Path,
        file_ids: Optional[List[str]] = None
    ) -> bool:
        """
        Export file records to CSV.
        
        Args:
            output_path: Path for CSV file
            file_ids: List of file IDs to export (all if None)
        
        Returns:
            True if exported successfully
        """
        try:
            import csv
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Get files
            if file_ids:
                files = []
                for fid in file_ids:
                    record = self.get_file_by_id(fid)
                    if record:
                        files.append(record)
            else:
                query = "SELECT * FROM file_records WHERE status = 'active'"
                rows = self.db_manager.execute_query(query)
                files = [self._row_to_dict(row) for row in rows]
            
            if not files:
                logger.warning("No files to export")
                return False
            
            # Flatten fields
            fieldnames = [
                'file_id', 'file_name', 'file_size', 'description', 'hash',
                'account_phone', 'channel', 'upload_date', 'parts', 'encrypted',
                'download_count', 'status'
            ]
            
            with open(output_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for file in files:
                    row = {field: file.get(field, '') for field in fieldnames}
                    writer.writerow(row)
            
            logger.info(f"Exported {len(files)} files to CSV: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Export to CSV failed: {e}")
            return False
    
    # ============================================
    # Helper Methods
    # ============================================
    
    def _row_to_dict(self, row: tuple) -> Dict[str, Any]:
        """Convert a database row to a dictionary."""
        # Get column names
        cursor = self.db_manager._get_cursor()
        column_names = [description[0] for description in cursor.description]
        
        # Map row to dict
        record = dict(zip(column_names, row))
        
        # Parse JSON fields
        for field in ['part_details', 'encryption_info', 'tags', 'metadata']:
            if field in record and record[field]:
                try:
                    record[field] = json.loads(record[field])
                except:
                    pass
        
        return record
    
    def get_all_file_ids(self) -> List[str]:
        """Get all file IDs."""
        query = "SELECT file_id FROM file_records WHERE status = 'active'"
        rows = self.db_manager.execute_query(query)
        return [row[0] for row in rows]
    
    def get_duplicate_descriptions(self) -> List[str]:
        """Find duplicate descriptions."""
        query = """
        SELECT description, COUNT(*) 
        FROM file_records 
        WHERE description IS NOT NULL
        GROUP BY description 
        HAVING COUNT(*) > 1
        """
        rows = self.db_manager.execute_query(query)
        return [row[0] for row in rows]
    
    def search_by_description(self, search_term: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Search files by description (partial match).
        
        Args:
            search_term: Description search term
            limit: Max results
        
        Returns:
            List of file records
        """
        return self.search_files(
            description=search_term,
            limit=limit
        )
    
    def get_files_by_size_range(
        self,
        min_size: int,
        max_size: int,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get files within a size range.
        
        Args:
            min_size: Minimum size in bytes
            max_size: Maximum size in bytes
            limit: Max results
        
        Returns:
            List of file records
        """
        return self.search_files(
            min_size=min_size,
            max_size=max_size,
            limit=limit
        )
    
    def get_files_by_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get files within a date range.
        
        Args:
            start_date: Start date
            end_date: End date
            limit: Max results
        
        Returns:
            List of file records
        """
        return self.search_files(
            start_date=start_date,
            end_date=end_date,
            limit=limit
        )
    
    def clear_cache(self) -> None:
        """Clear the file cache."""
        self._cache.clear()
        logger.info("File tracker cache cleared")
    
    def close(self) -> None:
        """Close the database connection."""
        self.db_manager.close()
        logger.info("File tracker closed")
    
    async def close_async(self) -> None:
        """Close the database connection asynchronously."""
        await self.db_manager.close_async()
        logger.info("File tracker closed asynchronously")
    
    def __repr__(self) -> str:
        return f"FileTracker(cache={len(self._cache)} files)"