#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
telegram_cli/core/integrity_checker.py – File integrity checking with multiple hash algorithms

Handles:
- Calculate MD5, SHA1, SHA256, SHA512 hashes
- Verify file integrity against stored hashes
- Batch integrity checking for multiple files
- Database storage for integrity records
- Export/import integrity data to/from JSON
- Verify files after upload/download (automatic)
- Check for file corruption or tampering
- Generate integrity reports
- Support for large files (streaming hash calculation)
- CRC32 checksum support
"""

import os
import hashlib
import json
import zlib
from pathlib import Path
from typing import Optional, List, Dict, Any, Union, Tuple
from datetime import datetime
import asyncio
import aiofiles

# Import core modules
try:
    from telegram_cli.database.db_manager import DatabaseManager
    from telegram_cli.utils.logger import get_logger
    from telegram_cli.utils.helpers import format_size, get_timestamp, ensure_dir
    MODULES_LOADED = True
except ImportError:
    MODULES_LOADED = False
    import logging
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.NullHandler())
    
    class DatabaseManager:
        def __init__(self, db_path=None): pass
        def init_db(self): pass
        def execute_write(self, q, p=None): pass
        def execute_query(self, q, p=None): return []
        def close(self): pass
    def format_size(s): return f"{s/1024:.2f} KB"
    def get_timestamp(): return datetime.now().isoformat()
    def ensure_dir(p): Path(p).mkdir(parents=True, exist_ok=True)

logger = get_logger(__name__)


class IntegrityChecker:
    """
    Advanced integrity checker for files with multiple hash algorithms.
    """
    
    # Supported hash algorithms
    ALGORITHMS = {
        'md5': hashlib.md5,
        'sha1': hashlib.sha1,
        'sha256': hashlib.sha256,
        'sha512': hashlib.sha512,
        'crc32': 'crc32'  # Special case
    }
    
    # Recommended algorithms for different use cases
    RECOMMENDED = {
        'fast': 'md5',
        'balanced': 'sha256',
        'secure': 'sha512',
        'check': 'crc32'  # Quick check
    }
    
    def __init__(
        self,
        db_manager: Optional[DatabaseManager] = None,
        db_path: Optional[Path] = None,
        default_algorithm: str = 'sha256',
        chunk_size: int = 1024 * 1024,  # 1MB chunks
        enable_persistence: bool = True,
        verify_after_upload: bool = True,
        verify_after_download: bool = True
    ):
        """
        Initialize the integrity checker.
        
        Args:
            db_manager: DatabaseManager instance
            db_path: Database path for persistence
            default_algorithm: Default hash algorithm
            chunk_size: Chunk size for streaming hash calculation
            enable_persistence: Store integrity records in database
            verify_after_upload: Auto-verify after upload
            verify_after_download: Auto-verify after download
        """
        self.db_manager = db_manager or DatabaseManager(db_path)
        self.default_algorithm = default_algorithm
        self.chunk_size = chunk_size
        self.enable_persistence = enable_persistence
        self.verify_after_upload = verify_after_upload
        self.verify_after_download = verify_after_download
        
        # Cache for integrity records
        self._cache = {}
        
        # Initialize database
        if self.enable_persistence:
            self._init_tables()
        
        logger.info(f"IntegrityChecker initialized (algorithm: {default_algorithm})")
    
    def _init_tables(self) -> None:
        """Initialize integrity database tables."""
        try:
            # Main integrity records table
            create_integrity = """
            CREATE TABLE IF NOT EXISTS integrity_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id TEXT NOT NULL,
                file_path TEXT NOT NULL,
                file_size INTEGER,
                algorithm TEXT NOT NULL,
                hash_value TEXT NOT NULL,
                calculated_at TEXT NOT NULL,
                verified_at TEXT,
                verification_status TEXT,  -- 'passed', 'failed', 'pending'
                last_verified TEXT,
                verification_count INTEGER DEFAULT 0,
                error_count INTEGER DEFAULT 0,
                metadata TEXT,  -- JSON
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(file_id, algorithm)
            )
            """
            
            # Verification history table
            create_history = """
            CREATE TABLE IF NOT EXISTS verification_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id TEXT NOT NULL,
                verification_date TEXT NOT NULL,
                status TEXT NOT NULL,  -- 'passed', 'failed'
                error_message TEXT,
                file_size INTEGER,
                algorithm TEXT,
                hash_value TEXT,
                metadata TEXT  -- JSON
            )
            """
            
            # Indexes
            create_indexes = """
            CREATE INDEX IF NOT EXISTS idx_integrity_file_id ON integrity_records(file_id);
            CREATE INDEX IF NOT EXISTS idx_integrity_file_path ON integrity_records(file_path);
            CREATE INDEX IF NOT EXISTS idx_integrity_algorithm ON integrity_records(algorithm);
            CREATE INDEX IF NOT EXISTS idx_history_file_id ON verification_history(file_id);
            CREATE INDEX IF NOT EXISTS idx_history_date ON verification_history(verification_date);
            """
            
            self.db_manager.execute_write(create_integrity)
            self.db_manager.execute_write(create_history)
            self.db_manager.execute_write(create_indexes)
            logger.debug("Integrity tables initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize integrity tables: {e}")
            raise
    
    # ============================================
    # Hash Calculation Methods
    # ============================================
    
    def calculate_hash(
        self,
        file_path: Union[str, Path],
        algorithm: Optional[str] = None,
        chunk_size: Optional[int] = None
    ) -> Tuple[str, int]:
        """
        Calculate hash of a file using specified algorithm.
        
        Args:
            file_path: Path to file
            algorithm: Hash algorithm (md5, sha1, sha256, sha512, crc32)
            chunk_size: Chunk size for streaming (default: 1MB)
        
        Returns:
            Tuple of (hash_string, file_size)
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        if not file_path.is_file():
            raise ValueError(f"Path is not a file: {file_path}")
        
        algorithm = algorithm or self.default_algorithm
        chunk_size = chunk_size or self.chunk_size
        
        file_size = file_path.stat().st_size
        
        # Special case for CRC32
        if algorithm == 'crc32':
            return self._calculate_crc32(file_path, chunk_size), file_size
        
        # Get hash algorithm
        hash_func = self.ALGORITHMS.get(algorithm)
        if not hash_func:
            raise ValueError(f"Unsupported algorithm: {algorithm}")
        
        # Calculate hash
        hasher = hash_func()
        
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                hasher.update(chunk)
        
        return hasher.hexdigest(), file_size
    
    async def calculate_hash_async(
        self,
        file_path: Union[str, Path],
        algorithm: Optional[str] = None,
        chunk_size: Optional[int] = None
    ) -> Tuple[str, int]:
        """
        Calculate hash of a file asynchronously.
        
        Args:
            file_path: Path to file
            algorithm: Hash algorithm
            chunk_size: Chunk size
        
        Returns:
            Tuple of (hash_string, file_size)
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        if not file_path.is_file():
            raise ValueError(f"Path is not a file: {file_path}")
        
        algorithm = algorithm or self.default_algorithm
        chunk_size = chunk_size or self.chunk_size
        
        file_size = file_path.stat().st_size
        
        # Special case for CRC32
        if algorithm == 'crc32':
            return await self._calculate_crc32_async(file_path, chunk_size), file_size
        
        # Get hash algorithm
        hash_func = self.ALGORITHMS.get(algorithm)
        if not hash_func:
            raise ValueError(f"Unsupported algorithm: {algorithm}")
        
        # Calculate hash asynchronously
        hasher = hash_func()
        
        async with aiofiles.open(file_path, 'rb') as f:
            while True:
                chunk = await f.read(chunk_size)
                if not chunk:
                    break
                hasher.update(chunk)
        
        return hasher.hexdigest(), file_size
    
    def _calculate_crc32(self, file_path: Path, chunk_size: int) -> str:
        """Calculate CRC32 checksum of a file."""
        crc = 0
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                crc = zlib.crc32(chunk, crc)
        return format(crc & 0xFFFFFFFF, '08x')
    
    async def _calculate_crc32_async(self, file_path: Path, chunk_size: int) -> str:
        """Calculate CRC32 checksum of a file asynchronously."""
        crc = 0
        async with aiofiles.open(file_path, 'rb') as f:
            while True:
                chunk = await f.read(chunk_size)
                if not chunk:
                    break
                crc = zlib.crc32(chunk, crc)
        return format(crc & 0xFFFFFFFF, '08x')
    
    def calculate_multiple_hashes(
        self,
        file_path: Union[str, Path],
        algorithms: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Calculate multiple hashes for a file.
        
        Args:
            file_path: Path to file
            algorithms: List of algorithms (uses default if None)
        
        Returns:
            Dictionary with hash results
        """
        file_path = Path(file_path)
        
        if algorithms is None:
            algorithms = ['md5', 'sha1', 'sha256']
        
        results = {
            'file_path': str(file_path),
            'file_name': file_path.name,
            'file_size': file_path.stat().st_size if file_path.exists() else 0,
            'hashes': {}
        }
        
        for algo in algorithms:
            try:
                hash_val, _ = self.calculate_hash(file_path, algo)
                results['hashes'][algo] = hash_val
            except Exception as e:
                results['hashes'][algo] = {'error': str(e)}
        
        return results
    
    # ============================================
    # Verification Methods
    # ============================================
    
    def verify_integrity(
        self,
        file_path: Union[str, Path],
        expected_hash: str,
        algorithm: Optional[str] = None,
        store_result: bool = True
    ) -> Dict[str, Any]:
        """
        Verify file integrity against an expected hash.
        
        Args:
            file_path: Path to file
            expected_hash: Expected hash value
            algorithm: Hash algorithm used
            store_result: Store verification result in database
        
        Returns:
            Verification result dictionary
        """
        file_path = Path(file_path)
        algorithm = algorithm or self.default_algorithm
        
        result = {
            'file_path': str(file_path),
            'file_name': file_path.name,
            'file_exists': file_path.exists(),
            'file_size': file_path.stat().st_size if file_path.exists() else 0,
            'algorithm': algorithm,
            'expected_hash': expected_hash,
            'calculated_hash': None,
            'status': 'failed',
            'verified_at': get_timestamp()
        }
        
        if not file_path.exists():
            result['error'] = 'File not found'
            return result
        
        try:
            # Calculate hash
            calculated_hash, file_size = self.calculate_hash(file_path, algorithm)
            result['calculated_hash'] = calculated_hash
            
            # Compare
            if calculated_hash.lower() == expected_hash.lower():
                result['status'] = 'passed'
            else:
                result['status'] = 'failed'
                result['error'] = 'Hash mismatch'
            
            # Store result
            if store_result and self.enable_persistence:
                self._store_verification_result(file_path, result)
            
            return result
            
        except Exception as e:
            result['status'] = 'error'
            result['error'] = str(e)
            return result
    
    async def verify_integrity_async(
        self,
        file_path: Union[str, Path],
        expected_hash: str,
        algorithm: Optional[str] = None,
        store_result: bool = True
    ) -> Dict[str, Any]:
        """Async version of verify_integrity."""
        file_path = Path(file_path)
        algorithm = algorithm or self.default_algorithm
        
        result = {
            'file_path': str(file_path),
            'file_name': file_path.name,
            'file_exists': file_path.exists(),
            'file_size': file_path.stat().st_size if file_path.exists() else 0,
            'algorithm': algorithm,
            'expected_hash': expected_hash,
            'calculated_hash': None,
            'status': 'failed',
            'verified_at': get_timestamp()
        }
        
        if not file_path.exists():
            result['error'] = 'File not found'
            return result
        
        try:
            # Calculate hash
            calculated_hash, file_size = await self.calculate_hash_async(file_path, algorithm)
            result['calculated_hash'] = calculated_hash
            
            # Compare
            if calculated_hash.lower() == expected_hash.lower():
                result['status'] = 'passed'
            else:
                result['status'] = 'failed'
                result['error'] = 'Hash mismatch'
            
            # Store result
            if store_result and self.enable_persistence:
                self._store_verification_result(file_path, result)
            
            return result
            
        except Exception as e:
            result['status'] = 'error'
            result['error'] = str(e)
            return result
    
    def verify_file_record(self, file_id: str) -> Dict[str, Any]:
        """
        Verify a file using its database record.
        
        Args:
            file_id: File ID from database
        
        Returns:
            Verification result
        """
        # Get file record
        if not self.db_manager:
            return {'error': 'Database not available'}
        
        query = "SELECT * FROM file_records WHERE file_id = ?"
        row = self.db_manager.execute_query(query, (file_id,))
        
        if not row:
            return {'error': f'File record {file_id} not found'}
        
        record = dict(zip(
            ['id', 'file_id', 'file_name', 'file_size', 'description', 'hash',
             'account_phone', 'channel', 'upload_date', 'download_date'],
            row[0]
        ))
        
        file_path = Path(record.get('file_name', ''))
        if not file_path.exists():
            # Try to find in download directory
            download_dir = Path("data/downloads")
            possible_paths = [
                download_dir / file_path.name,
                download_dir / file_path,
                file_path
            ]
            
            for p in possible_paths:
                if p.exists():
                    file_path = p
                    break
        
        if not file_path.exists():
            return {
                'error': f'File not found: {file_path}',
                'file_id': file_id,
                'record': record
            }
        
        # Verify
        expected_hash = record.get('hash')
        if not expected_hash:
            return {'error': 'No hash stored for this file'}
        
        result = self.verify_integrity(
            file_path=file_path,
            expected_hash=expected_hash,
            algorithm='sha256',  # Default
            store_result=True
        )
        result['file_id'] = file_id
        result['record'] = record
        
        return result
    
    def verify_all_files(self) -> Dict[str, Any]:
        """
        Verify all files in the database.
        
        Returns:
            Summary of verification results
        """
        if not self.db_manager:
            return {'error': 'Database not available'}
        
        query = "SELECT file_id, file_name, hash FROM file_records WHERE hash IS NOT NULL"
        rows = self.db_manager.execute_query(query)
        
        results = {
            'total_files': len(rows),
            'verified': 0,
            'passed': 0,
            'failed': 0,
            'errors': 0,
            'details': []
        }
        
        for row in rows:
            file_id = row[0]
            file_name = row[1]
            expected_hash = row[2]
            
            result = self.verify_file_record(file_id)
            results['details'].append(result)
            results['verified'] += 1
            
            if result.get('status') == 'passed':
                results['passed'] += 1
            elif result.get('status') == 'failed':
                results['failed'] += 1
            else:
                results['errors'] += 1
        
        return results
    
    # ============================================
    # Database Methods
    # ============================================
    
    def _store_verification_result(self, file_path: Path, result: Dict) -> None:
        """Store verification result in database."""
        try:
            # Get or create integrity record
            query = """
            SELECT id FROM integrity_records WHERE file_path = ? AND algorithm = ?
            """
            row = self.db_manager.execute_query(query, (str(file_path), result['algorithm']))
            
            if row:
                # Update existing record
                update_query = """
                UPDATE integrity_records 
                SET verification_status = ?, 
                    verified_at = ?,
                    last_verified = ?,
                    verification_count = verification_count + 1,
                    error_count = ?,
                    updated_at = ?
                WHERE id = ?
                """
                params = (
                    result['status'],
                    result['verified_at'],
                    result['verified_at'],
                    1 if result['status'] == 'failed' else 0,
                    get_timestamp(),
                    row[0][0]
                )
                self.db_manager.execute_write(update_query, params)
            else:
                # Insert new record
                insert_query = """
                INSERT INTO integrity_records (
                    file_id, file_path, file_size, algorithm, hash_value,
                    calculated_at, verified_at, verification_status,
                    last_verified, verification_count, error_count,
                    metadata, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                file_id = f"integrity_{int(datetime.now().timestamp())}_{file_path.stem}"
                params = (
                    file_id,
                    str(file_path),
                    result['file_size'],
                    result['algorithm'],
                    result['calculated_hash'],
                    get_timestamp(),
                    result['verified_at'],
                    result['status'],
                    result['verified_at'],
                    1,
                    1 if result['status'] == 'failed' else 0,
                    json.dumps({'expected_hash': result['expected_hash']}),
                    get_timestamp(),
                    get_timestamp()
                )
                self.db_manager.execute_write(insert_query, params)
            
            # Store history
            history_query = """
            INSERT INTO verification_history (
                file_id, verification_date, status, error_message,
                file_size, algorithm, hash_value, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            history_params = (
                file_id or f"integrity_{int(datetime.now().timestamp())}",
                result['verified_at'],
                result['status'],
                result.get('error'),
                result['file_size'],
                result['algorithm'],
                result['calculated_hash'],
                json.dumps({'expected_hash': result['expected_hash']})
            )
            self.db_manager.execute_write(history_query, history_params)
            
        except Exception as e:
            logger.error(f"Failed to store verification result: {e}")
    
    def get_integrity_record(
        self,
        file_path: Union[str, Path],
        algorithm: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get integrity record for a file.
        
        Args:
            file_path: Path to file
            algorithm: Hash algorithm
        
        Returns:
            Integrity record or None
        """
        file_path = str(Path(file_path))
        algorithm = algorithm or self.default_algorithm
        
        query = """
        SELECT * FROM integrity_records 
        WHERE file_path = ? AND algorithm = ?
        """
        row = self.db_manager.execute_query(query, (file_path, algorithm))
        
        if not row:
            return None
        
        record = dict(zip(
            ['id', 'file_id', 'file_path', 'file_size', 'algorithm', 'hash_value',
             'calculated_at', 'verified_at', 'verification_status', 'last_verified',
             'verification_count', 'error_count', 'metadata', 'created_at', 'updated_at'],
            row[0]
        ))
        
        if record.get('metadata'):
            try:
                record['metadata'] = json.loads(record['metadata'])
            except:
                pass
        
        return record
    
    def get_verification_history(self, file_id: str, limit: int = 10) -> List[Dict]:
        """
        Get verification history for a file.
        
        Args:
            file_id: File ID
            limit: Max results
        
        Returns:
            List of verification records
        """
        query = """
        SELECT * FROM verification_history 
        WHERE file_id = ?
        ORDER BY verification_date DESC
        LIMIT ?
        """
        rows = self.db_manager.execute_query(query, (file_id, limit))
        
        results = []
        for row in rows:
            record = dict(zip(
                ['id', 'file_id', 'verification_date', 'status', 'error_message',
                 'file_size', 'algorithm', 'hash_value', 'metadata'],
                row
            ))
            
            if record.get('metadata'):
                try:
                    record['metadata'] = json.loads(record['metadata'])
                except:
                    pass
            
            results.append(record)
        
        return results
    
    def get_integrity_stats(self) -> Dict[str, Any]:
        """
        Get integrity checking statistics.
        
        Returns:
            Statistics dictionary
        """
        stats = {
            'total_records': 0,
            'verified': 0,
            'passed': 0,
            'failed': 0,
            'pending': 0,
            'algorithms': {},
            'by_file_type': {}
        }
        
        try:
            # Total records
            query = "SELECT COUNT(*) FROM integrity_records"
            row = self.db_manager.execute_query(query)
            stats['total_records'] = row[0][0] if row else 0
            
            # Status breakdown
            query = """
            SELECT verification_status, COUNT(*) 
            FROM integrity_records 
            GROUP BY verification_status
            """
            rows = self.db_manager.execute_query(query)
            for row in rows:
                status = row[0] or 'unknown'
                stats[status] = row[1]
            
            # Algorithm usage
            query = """
            SELECT algorithm, COUNT(*) 
            FROM integrity_records 
            GROUP BY algorithm
            """
            rows = self.db_manager.execute_query(query)
            for row in rows:
                stats['algorithms'][row[0]] = row[1]
            
        except Exception as e:
            logger.error(f"Failed to get integrity stats: {e}")
        
        return stats
    
    # ============================================
    # Batch Verification Methods
    # ============================================
    
    def verify_directory(
        self,
        directory: Union[str, Path],
        algorithm: Optional[str] = None,
        recursive: bool = True,
        store_results: bool = True,
        filter_extensions: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Verify all files in a directory.
        
        Args:
            directory: Directory path
            algorithm: Hash algorithm
            recursive: Include subdirectories
            store_results: Store verification results
            filter_extensions: Only check files with these extensions
        
        Returns:
            Verification summary
        """
        directory = Path(directory)
        algorithm = algorithm or self.default_algorithm
        
        if not directory.exists():
            return {'error': f'Directory not found: {directory}'}
        
        if not directory.is_dir():
            return {'error': f'Path is not a directory: {directory}'}
        
        # Get files
        if recursive:
            files = list(directory.rglob('*'))
        else:
            files = list(directory.glob('*'))
        
        files = [f for f in files if f.is_file()]
        
        # Filter extensions
        if filter_extensions:
            files = [f for f in files if f.suffix.lower() in filter_extensions]
        
        results = {
            'directory': str(directory),
            'total_files': len(files),
            'verified': 0,
            'passed': 0,
            'failed': 0,
            'errors': 0,
            'details': []
        }
        
        for file_path in files:
            try:
                # Calculate hash (no expected hash, just calculate)
                hash_val, size = self.calculate_hash(file_path, algorithm)
                
                result = {
                    'file_path': str(file_path),
                    'file_name': file_path.name,
                    'file_size': size,
                    'algorithm': algorithm,
                    'hash': hash_val,
                    'status': 'calculated'
                }
                
                # Store if enabled
                if store_results and self.enable_persistence:
                    self._store_verification_result(file_path, {
                        'file_size': size,
                        'algorithm': algorithm,
                        'calculated_hash': hash_val,
                        'expected_hash': None,
                        'status': 'calculated',
                        'verified_at': get_timestamp()
                    })
                
                results['details'].append(result)
                results['verified'] += 1
                results['passed'] += 1
                
            except Exception as e:
                results['errors'] += 1
                results['details'].append({
                    'file_path': str(file_path),
                    'error': str(e),
                    'status': 'error'
                })
        
        return results
    
    async def verify_directory_async(
        self,
        directory: Union[str, Path],
        algorithm: Optional[str] = None,
        recursive: bool = True,
        store_results: bool = True,
        max_parallel: int = 4
    ) -> Dict[str, Any]:
        """
        Verify directory files asynchronously with parallel processing.
        
        Args:
            directory: Directory path
            algorithm: Hash algorithm
            recursive: Include subdirectories
            store_results: Store results
            max_parallel: Max parallel verifications
        
        Returns:
            Verification summary
        """
        directory = Path(directory)
        algorithm = algorithm or self.default_algorithm
        
        if not directory.exists():
            return {'error': f'Directory not found: {directory}'}
        
        if not directory.is_dir():
            return {'error': f'Path is not a directory: {directory}'}
        
        # Get files
        if recursive:
            files = list(directory.rglob('*'))
        else:
            files = list(directory.glob('*'))
        
        files = [f for f in files if f.is_file()]
        
        results = {
            'directory': str(directory),
            'total_files': len(files),
            'verified': 0,
            'passed': 0,
            'failed': 0,
            'errors': 0,
            'details': []
        }
        
        semaphore = asyncio.Semaphore(max_parallel)
        
        async def verify_one(file_path):
            async with semaphore:
                try:
                    hash_val, size = await self.calculate_hash_async(file_path, algorithm)
                    result = {
                        'file_path': str(file_path),
                        'file_name': file_path.name,
                        'file_size': size,
                        'algorithm': algorithm,
                        'hash': hash_val,
                        'status': 'calculated'
                    }
                    
                    if store_results and self.enable_persistence:
                        self._store_verification_result(file_path, {
                            'file_size': size,
                            'algorithm': algorithm,
                            'calculated_hash': hash_val,
                            'expected_hash': None,
                            'status': 'calculated',
                            'verified_at': get_timestamp()
                        })
                    
                    return result
                except Exception as e:
                    return {
                        'file_path': str(file_path),
                        'error': str(e),
                        'status': 'error'
                    }
        
        tasks = [verify_one(f) for f in files]
        task_results = await asyncio.gather(*tasks)
        
        for result in task_results:
            results['details'].append(result)
            results['verified'] += 1
            if result.get('status') == 'error':
                results['errors'] += 1
            else:
                results['passed'] += 1
        
        return results
    
    # ============================================
    # Export/Import Methods
    # ============================================
    
    def export_integrity_data(
        self,
        output_path: Path,
        file_ids: Optional[List[str]] = None
    ) -> bool:
        """
        Export integrity data to JSON.
        
        Args:
            output_path: Path for JSON file
            file_ids: List of file IDs (all if None)
        
        Returns:
            True if exported successfully
        """
        try:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Get records
            if file_ids:
                records = []
                for fid in file_ids:
                    query = "SELECT * FROM integrity_records WHERE file_id = ?"
                    rows = self.db_manager.execute_query(query, (fid,))
                    for row in rows:
                        record = dict(zip(
                            ['id', 'file_id', 'file_path', 'file_size', 'algorithm', 'hash_value',
                             'calculated_at', 'verified_at', 'verification_status', 'last_verified',
                             'verification_count', 'error_count', 'metadata', 'created_at', 'updated_at'],
                            row
                        ))
                        if record.get('metadata'):
                            try:
                                record['metadata'] = json.loads(record['metadata'])
                            except:
                                pass
                        records.append(record)
            else:
                query = "SELECT * FROM integrity_records"
                rows = self.db_manager.execute_query(query)
                records = []
                for row in rows:
                    record = dict(zip(
                        ['id', 'file_id', 'file_path', 'file_size', 'algorithm', 'hash_value',
                         'calculated_at', 'verified_at', 'verification_status', 'last_verified',
                         'verification_count', 'error_count', 'metadata', 'created_at', 'updated_at'],
                        row
                    ))
                    if record.get('metadata'):
                        try:
                            record['metadata'] = json.loads(record['metadata'])
                        except:
                            pass
                    records.append(record)
            
            # Write to file
            export_data = {
                'export_time': get_timestamp(),
                'total_records': len(records),
                'records': records
            }
            
            with open(output_path, 'w') as f:
                json.dump(export_data, f, indent=2, default=str)
            
            logger.info(f"Exported {len(records)} integrity records to: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Export failed: {e}")
            return False
    
    def import_integrity_data(self, import_path: Path) -> Dict[str, Any]:
        """
        Import integrity data from JSON.
        
        Args:
            import_path: Path to JSON file
        
        Returns:
            Import summary
        """
        try:
            import_path = Path(import_path)
            
            if not import_path.exists():
                return {'error': f'File not found: {import_path}'}
            
            with open(import_path, 'r') as f:
                data = json.load(f)
            
            records = data.get('records', [])
            imported = 0
            skipped = 0
            errors = 0
            
            for record in records:
                try:
                    # Check if already exists
                    check_query = """
                    SELECT id FROM integrity_records 
                    WHERE file_id = ? AND algorithm = ?
                    """
                    existing = self.db_manager.execute_query(
                        check_query,
                        (record.get('file_id'), record.get('algorithm'))
                    )
                    
                    if existing:
                        skipped += 1
                        continue
                    
                    # Insert record
                    insert_query = """
                    INSERT INTO integrity_records (
                        file_id, file_path, file_size, algorithm, hash_value,
                        calculated_at, verified_at, verification_status,
                        last_verified, verification_count, error_count,
                        metadata, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    params = (
                        record.get('file_id'),
                        record.get('file_path'),
                        record.get('file_size'),
                        record.get('algorithm'),
                        record.get('hash_value'),
                        record.get('calculated_at') or get_timestamp(),
                        record.get('verified_at'),
                        record.get('verification_status'),
                        record.get('last_verified'),
                        record.get('verification_count', 0),
                        record.get('error_count', 0),
                        json.dumps(record.get('metadata', {})),
                        get_timestamp(),
                        get_timestamp()
                    )
                    
                    self.db_manager.execute_write(insert_query, params)
                    imported += 1
                    
                except Exception as e:
                    errors += 1
                    logger.error(f"Failed to import record: {e}")
            
            result = {
                'total_records': len(records),
                'imported': imported,
                'skipped': skipped,
                'errors': errors
            }
            
            logger.info(f"Imported {imported} integrity records from: {import_path}")
            return result
            
        except Exception as e:
            logger.error(f"Import failed: {e}")
            return {'error': str(e)}
    
    # ============================================
    # Utility Methods
    # ============================================
    
    def generate_integrity_report(
        self,
        output_path: Optional[Path] = None
    ) -> str:
        """
        Generate an integrity report.
        
        Args:
            output_path: Path for report file
        
        Returns:
            Report as string
        """
        stats = self.get_integrity_stats()
        
        report = [
            "=" * 60,
            "INTEGRITY CHECKER REPORT",
            "=" * 60,
            f"Generated: {get_timestamp()}",
            "",
            "STATISTICS:",
            f"  Total Records: {stats.get('total_records', 0)}",
            f"  Verified: {stats.get('verified', 0)}",
            f"  Passed: {stats.get('passed', 0)}",
            f"  Failed: {stats.get('failed', 0)}",
            f"  Pending: {stats.get('pending', 0)}",
            "",
            "ALGORITHMS:"
        ]
        
        for algo, count in stats.get('algorithms', {}).items():
            report.append(f"  {algo}: {count}")
        
        report.append("")
        report.append("=" * 60)
        
        report_text = "\n".join(report)
        
        # Write to file if output_path provided
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w') as f:
                f.write(report_text)
            logger.info(f"Report saved to: {output_path}")
        
        return report_text
    
    def get_checksum(
        self,
        file_path: Union[str, Path],
        algorithm: str = 'sha256'
    ) -> str:
        """
        Get checksum for a file (shortcut).
        
        Args:
            file_path: Path to file
            algorithm: Hash algorithm
        
        Returns:
            Hash string
        """
        hash_val, _ = self.calculate_hash(file_path, algorithm)
        return hash_val
    
    def compare_files(
        self,
        file1: Union[str, Path],
        file2: Union[str, Path],
        algorithm: str = 'sha256'
    ) -> bool:
        """
        Compare two files by hash.
        
        Args:
            file1: First file path
            file2: Second file path
            algorithm: Hash algorithm
        
        Returns:
            True if files are identical
        """
        hash1, _ = self.calculate_hash(file1, algorithm)
        hash2, _ = self.calculate_hash(file2, algorithm)
        return hash1 == hash2
    
    def clear_cache(self) -> None:
        """Clear the cache."""
        self._cache.clear()
        logger.debug("Integrity cache cleared")
    
    def close(self) -> None:
        """Close database connection."""
        if self.db_manager:
            self.db_manager.close()
        logger.info("IntegrityChecker closed")
    
    async def close_async(self) -> None:
        """Close database connection asynchronously."""
        if self.db_manager:
            await self.db_manager.close_async()
        logger.info("IntegrityChecker closed asynchronously")
    
    def __repr__(self) -> str:
        return f"IntegrityChecker(algorithm={self.default_algorithm}, db={self.db_manager is not None})"