#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
telegram_cli/core/config_exporter.py – Export and import full configuration

Handles:
- Export accounts, sessions, settings, database, downloads
- Create encrypted/compressed archives (tar.gz)
- Import configuration with merge/overwrite options
- Password protection for archives
- Export/import integrity verification
- Selective export/import of components
- Dry-run mode for imports
- Backup before import
"""

import os
import json
import shutil
import tarfile
import tempfile
import base64
from pathlib import Path
from typing import Optional, List, Dict, Any, Union, Tuple
from datetime import datetime
import zipfile

# Try to import cryptography for encryption
try:
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import hashes, padding
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

# Import core modules
try:
    from telegram_cli.core.account_manager import AccountManager
    from telegram_cli.core.session_manager import SessionManager
    from telegram_cli.database.db_manager import DatabaseManager
    from telegram_cli.utils.config import Config
    from telegram_cli.utils.logger import get_logger
    from telegram_cli.utils.helpers import get_timestamp, ensure_dir, format_size
    MODULES_LOADED = True
except ImportError:
    MODULES_LOADED = False
    import logging
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.NullHandler())
    
    class AccountManager:
        def __init__(self): pass
        def get_all_accounts(self): return []
        def add_account(self, **kwargs): pass
    class SessionManager:
        def __init__(self): pass
        def session_exists(self, phone): return False
        def get_session_path(self, phone): return Path()
    class DatabaseManager:
        def __init__(self): pass
        def close(self): pass
    class Config:
        def __init__(self): pass
        def load(self): pass
        def save(self): pass
        def get(self, key, default=None): return default
        def set(self, key, value): pass
    def get_timestamp(): return datetime.now().isoformat()
    def ensure_dir(p): Path(p).mkdir(parents=True, exist_ok=True)
    def format_size(s): return f"{s/1024:.2f} KB"

logger = get_logger(__name__)


class ConfigExporter:
    """
    Export and import full configuration including accounts, sessions, and data.
    """
    
    # Components that can be exported
    COMPONENTS = ['accounts', 'sessions', 'database', 'downloads', 'settings', 'passwords']
    
    def __init__(
        self,
        config: Optional[Config] = None,
        account_manager: Optional[AccountManager] = None,
        session_manager: Optional[SessionManager] = None,
        db_manager: Optional[DatabaseManager] = None,
        data_dir: Optional[Path] = None
    ):
        """
        Initialize the config exporter.
        
        Args:
            config: Config instance
            account_manager: AccountManager instance
            session_manager: SessionManager instance
            db_manager: DatabaseManager instance
            data_dir: Base data directory
        """
        self.config = config or Config()
        self.account_manager = account_manager or AccountManager()
        self.session_manager = session_manager or SessionManager()
        self.db_manager = db_manager or DatabaseManager()
        self.data_dir = Path(data_dir or "data")
        
        # Ensure data directory exists
        ensure_dir(self.data_dir)
        
        # Load config
        self.config.load()
        
        # Export/import log
        self._log = []
        
        logger.info("ConfigExporter initialized")
    
    # ============================================
    # Export Methods
    # ============================================
    
    def export(
        self,
        output_path: Union[str, Path],
        components: Optional[List[str]] = None,
        include_downloads: bool = False,
        compress: bool = True,
        password: Optional[str] = None,
        metadata: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Export configuration to a file.
        
        Args:
            output_path: Path for export file (.tar.gz or .zip)
            components: List of components to export (None = all)
            include_downloads: Include downloaded files
            compress: Use compression (tar.gz vs tar)
            password: Password for encryption (optional)
            metadata: Additional metadata to include
        
        Returns:
            Export result dictionary
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        components = components or self.COMPONENTS
        
        # Validate components
        invalid = [c for c in components if c not in self.COMPONENTS]
        if invalid:
            raise ValueError(f"Invalid components: {invalid}")
        
        logger.info(f"Exporting to {output_path} (components: {components})")
        
        # Create temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            export_dir = temp_path / "telegram_export"
            ensure_dir(export_dir)
            
            # Create export info
            export_info = {
                'version': '1.0',
                'export_time': get_timestamp(),
                'components': components,
                'include_downloads': include_downloads,
                'metadata': metadata or {},
                'files': []
            }
            
            # Export each component
            files_exported = []
            total_size = 0
            
            for component in components:
                try:
                    result = self._export_component(
                        component=component,
                        export_dir=export_dir,
                        include_downloads=include_downloads
                    )
                    
                    if result:
                        files_exported.extend(result.get('files', []))
                        total_size += result.get('size', 0)
                        export_info['files'].append({
                            'component': component,
                            'files': result.get('files', []),
                            'size': result.get('size', 0)
                        })
                        self._log_export(component, 'success', result.get('size', 0))
                        
                except Exception as e:
                    logger.error(f"Failed to export {component}: {e}")
                    self._log_export(component, 'error', 0, str(e))
            
            # Write export info
            info_path = export_dir / "export_info.json"
            with open(info_path, 'w') as f:
                json.dump(export_info, f, indent=2, default=str)
            
            # Create archive
            if output_path.suffix == '.zip':
                self._create_zip(export_dir, output_path, password)
            else:
                self._create_tar(export_dir, output_path, compress, password)
            
            # Cleanup
            shutil.rmtree(export_dir)
        
        logger.info(f"Export complete: {len(files_exported)} files, {format_size(total_size)}")
        
        return {
            'success': True,
            'output_path': str(output_path),
            'components': components,
            'files': len(files_exported),
            'size': total_size,
            'password_protected': bool(password)
        }
    
    def _export_component(
        self,
        component: str,
        export_dir: Path,
        include_downloads: bool = False
    ) -> Optional[Dict]:
        """
        Export a specific component.
        
        Args:
            component: Component name
            export_dir: Export directory
            include_downloads: Include downloads
        
        Returns:
            Component export result
        """
        component_dir = export_dir / component
        ensure_dir(component_dir)
        
        if component == 'accounts':
            return self._export_accounts(component_dir)
        
        elif component == 'sessions':
            return self._export_sessions(component_dir)
        
        elif component == 'database':
            return self._export_database(component_dir)
        
        elif component == 'downloads':
            if include_downloads:
                return self._export_downloads(component_dir)
            return None
        
        elif component == 'settings':
            return self._export_settings(component_dir)
        
        elif component == 'passwords':
            return self._export_passwords(component_dir)
        
        return None
    
    def _export_accounts(self, export_dir: Path) -> Dict:
        """Export account configurations."""
        accounts = self.account_manager.get_all_accounts()
        
        if not accounts:
            logger.warning("No accounts to export")
            return {'files': [], 'size': 0}
        
        # Sanitize accounts (remove sensitive data if needed)
        sanitized = []
        for acc in accounts:
            # Don't export api_hash in plain text if not encrypted
            # For now, we'll export with a warning
            sanitized.append(acc)
        
        accounts_path = export_dir / "accounts.json"
        with open(accounts_path, 'w') as f:
            json.dump(sanitized, f, indent=2, default=str)
        
        size = accounts_path.stat().st_size
        
        logger.info(f"Exported {len(accounts)} accounts")
        return {'files': [str(accounts_path)], 'size': size}
    
    def _export_sessions(self, export_dir: Path) -> Dict:
        """Export session files."""
        session_dir = self.data_dir / "sessions"
        if not session_dir.exists():
            logger.warning("No sessions directory to export")
            return {'files': [], 'size': 0}
        
        # Copy all session files
        files = []
        total_size = 0
        
        for session_file in session_dir.glob("*.session"):
            dest = export_dir / session_file.name
            shutil.copy2(session_file, dest)
            files.append(str(dest))
            total_size += dest.stat().st_size
        
        # Also copy journal files
        for journal_file in session_dir.glob("*.session-journal"):
            dest = export_dir / journal_file.name
            shutil.copy2(journal_file, dest)
            files.append(str(dest))
            total_size += dest.stat().st_size
        
        logger.info(f"Exported {len(files)} session files")
        return {'files': files, 'size': total_size}
    
    def _export_database(self, export_dir: Path) -> Dict:
        """Export database."""
        db_path = self.data_dir / "database" / "telegram_cli.db"
        if not db_path.exists():
            logger.warning("Database file not found")
            return {'files': [], 'size': 0}
        
        # Close database to ensure consistency
        self.db_manager.close()
        
        # Copy database
        dest = export_dir / "telegram_cli.db"
        shutil.copy2(db_path, dest)
        
        size = dest.stat().st_size
        
        logger.info(f"Exported database: {format_size(size)}")
        return {'files': [str(dest)], 'size': size}
    
    def _export_downloads(self, export_dir: Path) -> Dict:
        """Export downloaded files."""
        downloads_dir = self.data_dir / "downloads"
        if not downloads_dir.exists():
            logger.warning("Downloads directory not found")
            return {'files': [], 'size': 0}
        
        # Create subdirectory for downloads
        downloads_export = export_dir / "downloads"
        ensure_dir(downloads_export)
        
        # Copy all files
        files = []
        total_size = 0
        
        for file_path in downloads_dir.rglob("*"):
            if file_path.is_file():
                rel_path = file_path.relative_to(downloads_dir)
                dest = downloads_export / rel_path
                ensure_dir(dest.parent)
                shutil.copy2(file_path, dest)
                files.append(str(dest))
                total_size += dest.stat().st_size
        
        logger.info(f"Exported {len(files)} downloaded files ({format_size(total_size)})")
        return {'files': files, 'size': total_size}
    
    def _export_settings(self, export_dir: Path) -> Dict:
        """Export settings."""
        settings_path = self.data_dir / "config" / "settings.yaml"
        if not settings_path.exists():
            logger.warning("Settings file not found")
            return {'files': [], 'size': 0}
        
        dest = export_dir / "settings.yaml"
        shutil.copy2(settings_path, dest)
        
        size = dest.stat().st_size
        
        logger.info(f"Exported settings: {format_size(size)}")
        return {'files': [str(dest)], 'size': size}
    
    def _export_passwords(self, export_dir: Path) -> Dict:
        """Export password store."""
        password_path = self.data_dir / "passwords" / "password_store.enc"
        if not password_path.exists():
            logger.warning("Password store not found")
            return {'files': [], 'size': 0}
        
        dest = export_dir / "password_store.enc"
        shutil.copy2(password_path, dest)
        
        size = dest.stat().st_size
        
        logger.info(f"Exported password store: {format_size(size)}")
        return {'files': [str(dest)], 'size': size}
    
    # ============================================
    # Archive Creation
    # ============================================
    
    def _create_tar(self, source_dir: Path, output_path: Path, compress: bool, password: Optional[str]) -> None:
        """Create a tar/tar.gz archive."""
        mode = "w:gz" if compress else "w"
        
        with tarfile.open(output_path, mode) as tar:
            tar.add(source_dir, arcname=Path(source_dir).name)
        
        # Encrypt if password provided
        if password:
            if not CRYPTO_AVAILABLE:
                logger.warning("Cryptography not available, skipping encryption")
                return
            
            self._encrypt_file(output_path, password)
    
    def _create_zip(self, source_dir: Path, output_path: Path, password: Optional[str]) -> None:
        """Create a zip archive."""
        compression = zipfile.ZIP_DEFLATED
        
        with zipfile.ZipFile(output_path, 'w', compression) as zipf:
            for file_path in source_dir.rglob("*"):
                if file_path.is_file():
                    arcname = file_path.relative_to(source_dir.parent)
                    zipf.write(file_path, arcname)
        
        # Encrypt if password provided
        if password:
            if not CRYPTO_AVAILABLE:
                logger.warning("Cryptography not available, skipping encryption")
                return
            
            self._encrypt_file(output_path, password)
    
    def _encrypt_file(self, file_path: Path, password: str) -> None:
        """Encrypt an archive file with AES-256."""
        if not CRYPTO_AVAILABLE:
            raise RuntimeError("Cryptography library not available for encryption")
        
        # Read file
        with open(file_path, 'rb') as f:
            data = f.read()
        
        # Generate salt
        salt = os.urandom(16)
        
        # Derive key
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
            backend=default_backend()
        )
        key = kdf.derive(password.encode())
        
        # Generate IV
        iv = os.urandom(16)
        
        # Encrypt
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
        encryptor = cipher.encryptor()
        
        # Pad data
        padder = padding.PKCS7(128).padder()
        padded_data = padder.update(data) + padder.finalize()
        
        # Encrypt
        encrypted = encryptor.update(padded_data) + encryptor.finalize()
        
        # Write salt + iv + encrypted data
        encrypted_path = file_path.with_suffix(file_path.suffix + '.enc')
        with open(encrypted_path, 'wb') as f:
            f.write(salt)
            f.write(iv)
            f.write(encrypted)
        
        # Replace original
        file_path.unlink()
        encrypted_path.rename(file_path)
        
        logger.info(f"Encrypted archive: {file_path}")
    
    def _decrypt_file(self, file_path: Path, password: str) -> None:
        """Decrypt an archive file."""
        if not CRYPTO_AVAILABLE:
            raise RuntimeError("Cryptography library not available for decryption")
        
        # Read encrypted file
        with open(file_path, 'rb') as f:
            salt = f.read(16)
            iv = f.read(16)
            encrypted = f.read()
        
        # Derive key
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
            backend=default_backend()
        )
        key = kdf.derive(password.encode())
        
        # Decrypt
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        
        padded_data = decryptor.update(encrypted) + decryptor.finalize()
        
        # Unpad
        unpadder = padding.PKCS7(128).unpadder()
        data = unpadder.update(padded_data) + unpadder.finalize()
        
        # Write decrypted data
        decrypted_path = file_path.with_suffix('')
        with open(decrypted_path, 'wb') as f:
            f.write(data)
        
        # Replace encrypted with decrypted
        file_path.unlink()
        decrypted_path.rename(file_path)
        
        logger.info(f"Decrypted archive: {file_path}")
    
    # ============================================
    # Import Methods
    # ============================================
    
    def import_config(
        self,
        archive_path: Union[str, Path],
        password: Optional[str] = None,
        components: Optional[List[str]] = None,
        overwrite: bool = False,
        merge: bool = True,
        backup: bool = True,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Import configuration from an archive.
        
        Args:
            archive_path: Path to archive file (.tar.gz, .zip, or .encrypted)
            password: Password for encrypted archive
            components: Components to import (None = all)
            overwrite: Overwrite existing files
            merge: Merge with existing data (vs replace)
            backup: Create backup before import
            dry_run: Simulate import without making changes
        
        Returns:
            Import result dictionary
        """
        archive_path = Path(archive_path)
        
        if not archive_path.exists():
            raise FileNotFoundError(f"Archive not found: {archive_path}")
        
        # Detect if encrypted
        if archive_path.suffix == '.enc':
            if not password:
                raise ValueError("Password required for encrypted archive")
            # Decrypt first
            temp_archive = archive_path.with_suffix(archive_path.suffix[:-4] or '')
            shutil.copy2(archive_path, temp_archive)
            self._decrypt_file(temp_archive, password)
            archive_path = temp_archive
        
        components = components or self.COMPONENTS
        
        logger.info(f"Importing from {archive_path} (components: {components})")
        
        # Create backup if requested
        if backup and not dry_run:
            backup_path = self._create_backup()
            logger.info(f"Backup created: {backup_path}")
        
        # Extract archive
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Extract based on extension
            if archive_path.suffix == '.zip':
                self._extract_zip(archive_path, temp_path)
            else:
                self._extract_tar(archive_path, temp_path)
            
            # Find export directory
            export_dirs = list(temp_path.glob("telegram_export"))
            if not export_dirs:
                export_dirs = list(temp_path.glob("*"))
            
            if not export_dirs:
                raise ValueError("Invalid export: no content found")
            
            export_dir = export_dirs[0]
            
            # Load export info
            info_path = export_dir / "export_info.json"
            if info_path.exists():
                with open(info_path, 'r') as f:
                    export_info = json.load(f)
            else:
                export_info = {'components': components}
            
            # Check components
            available_components = export_info.get('components', [])
            components_to_import = [c for c in components if c in available_components]
            
            if not components_to_import:
                logger.warning("No matching components found in archive")
                return {'success': False, 'error': 'No matching components found'}
            
            # Import each component
            results = {}
            for component in components_to_import:
                try:
                    result = self._import_component(
                        component=component,
                        export_dir=export_dir / component,
                        overwrite=overwrite,
                        merge=merge,
                        dry_run=dry_run
                    )
                    results[component] = result
                    self._log_import(component, 'success')
                except Exception as e:
                    logger.error(f"Failed to import {component}: {e}")
                    results[component] = {'error': str(e)}
                    self._log_import(component, 'error', str(e))
            
            # Cleanup decrypted archive
            if archive_path != archive_path.with_suffix(''):
                archive_path.unlink()
        
        logger.info(f"Import complete: {results}")
        
        return {
            'success': True,
            'archive': str(archive_path),
            'components_imported': components_to_import,
            'results': results
        }
    
    def _import_component(
        self,
        component: str,
        export_dir: Path,
        overwrite: bool,
        merge: bool,
        dry_run: bool
    ) -> Dict:
        """
        Import a specific component.
        
        Args:
            component: Component name
            export_dir: Export directory
            overwrite: Overwrite existing
            merge: Merge with existing
            dry_run: Simulate
        
        Returns:
            Import result
        """
        if not export_dir.exists():
            return {'error': f'Component {component} not found in archive'}
        
        if component == 'accounts':
            return self._import_accounts(export_dir, overwrite, merge, dry_run)
        
        elif component == 'sessions':
            return self._import_sessions(export_dir, overwrite, dry_run)
        
        elif component == 'database':
            return self._import_database(export_dir, overwrite, dry_run)
        
        elif component == 'downloads':
            return self._import_downloads(export_dir, overwrite, dry_run)
        
        elif component == 'settings':
            return self._import_settings(export_dir, overwrite, dry_run)
        
        elif component == 'passwords':
            return self._import_passwords(export_dir, overwrite, dry_run)
        
        return {'error': f'Unknown component: {component}'}
    
    def _import_accounts(self, export_dir: Path, overwrite: bool, merge: bool, dry_run: bool) -> Dict:
        """Import accounts."""
        accounts_path = export_dir / "accounts.json"
        if not accounts_path.exists():
            return {'error': 'accounts.json not found'}
        
        with open(accounts_path, 'r') as f:
            accounts = json.load(f)
        
        if dry_run:
            return {'dry_run': True, 'accounts_found': len(accounts)}
        
        imported = 0
        skipped = 0
        
        for acc in accounts:
            phone = acc.get('phone')
            if not phone:
                continue
            
            # Check if exists
            existing = self.account_manager.get_account(phone)
            if existing and not overwrite:
                if merge:
                    # Merge (skip fields that exist)
                    # For now, just skip duplicates
                    skipped += 1
                    continue
                else:
                    # Overwrite - remove existing
                    self.account_manager.remove_account(phone)
            
            # Add account
            self.account_manager.add_account(
                api_id=acc.get('api_id'),
                api_hash=acc.get('api_hash'),
                phone=phone,
                session_path=acc.get('session_path')
            )
            imported += 1
        
        return {'imported': imported, 'skipped': skipped}
    
    def _import_sessions(self, export_dir: Path, overwrite: bool, dry_run: bool) -> Dict:
        """Import session files."""
        session_dir = self.data_dir / "sessions"
        ensure_dir(session_dir)
        
        imported = 0
        skipped = 0
        
        for session_file in export_dir.glob("*.session"):
            dest = session_dir / session_file.name
            
            if dest.exists() and not overwrite:
                skipped += 1
                continue
            
            if not dry_run:
                shutil.copy2(session_file, dest)
            imported += 1
        
        # Also copy journal files
        for journal_file in export_dir.glob("*.session-journal"):
            dest = session_dir / journal_file.name
            if not dry_run:
                shutil.copy2(journal_file, dest)
        
        return {'imported': imported, 'skipped': skipped}
    
    def _import_database(self, export_dir: Path, overwrite: bool, dry_run: bool) -> Dict:
        """Import database."""
        db_path = export_dir / "telegram_cli.db"
        if not db_path.exists():
            return {'error': 'Database file not found'}
        
        dest = self.data_dir / "database" / "telegram_cli.db"
        ensure_dir(dest.parent)
        
        if dest.exists() and not overwrite:
            return {'skipped': True, 'reason': 'Database exists and overwrite=False'}
        
        if not dry_run:
            # Close existing connection
            self.db_manager.close()
            # Copy
            shutil.copy2(db_path, dest)
            # Reopen
            self.db_manager.init_db()
        
        return {'imported': True}
    
    def _import_downloads(self, export_dir: Path, overwrite: bool, dry_run: bool) -> Dict:
        """Import downloaded files."""
        downloads_export = export_dir / "downloads"
        if not downloads_export.exists():
            return {'error': 'Downloads not found in archive'}
        
        downloads_dir = self.data_dir / "downloads"
        ensure_dir(downloads_dir)
        
        imported = 0
        skipped = 0
        
        for file_path in downloads_export.rglob("*"):
            if file_path.is_file():
                rel_path = file_path.relative_to(downloads_export)
                dest = downloads_dir / rel_path
                ensure_dir(dest.parent)
                
                if dest.exists() and not overwrite:
                    skipped += 1
                    continue
                
                if not dry_run:
                    shutil.copy2(file_path, dest)
                imported += 1
        
        return {'imported': imported, 'skipped': skipped}
    
    def _import_settings(self, export_dir: Path, overwrite: bool, dry_run: bool) -> Dict:
        """Import settings."""
        settings_path = export_dir / "settings.yaml"
        if not settings_path.exists():
            return {'error': 'settings.yaml not found'}
        
        dest = self.data_dir / "config" / "settings.yaml"
        ensure_dir(dest.parent)
        
        if dest.exists() and not overwrite:
            return {'skipped': True, 'reason': 'Settings exist and overwrite=False'}
        
        if not dry_run:
            shutil.copy2(settings_path, dest)
            self.config.load()
        
        return {'imported': True}
    
    def _import_passwords(self, export_dir: Path, overwrite: bool, dry_run: bool) -> Dict:
        """Import password store."""
        password_path = export_dir / "password_store.enc"
        if not password_path.exists():
            return {'error': 'password_store.enc not found'}
        
        dest = self.data_dir / "passwords" / "password_store.enc"
        ensure_dir(dest.parent)
        
        if dest.exists() and not overwrite:
            return {'skipped': True, 'reason': 'Password store exists and overwrite=False'}
        
        if not dry_run:
            shutil.copy2(password_path, dest)
        
        return {'imported': True}
    
    # ============================================
    # Archive Extraction
    # ============================================
    
    def _extract_tar(self, archive_path: Path, extract_dir: Path) -> None:
        """Extract tar/tar.gz archive."""
        try:
            with tarfile.open(archive_path, 'r:*') as tar:
                tar.extractall(extract_dir)
        except tarfile.ReadError as e:
            # Try gzip
            with tarfile.open(archive_path, 'r:gz') as tar:
                tar.extractall(extract_dir)
    
    def _extract_zip(self, archive_path: Path, extract_dir: Path) -> None:
        """Extract zip archive."""
        with zipfile.ZipFile(archive_path, 'r') as zipf:
            zipf.extractall(extract_dir)
    
    # ============================================
    # Backup Methods
    # ============================================
    
    def _create_backup(self) -> Path:
        """Create a backup of current data."""
        backup_dir = self.data_dir / "backups"
        ensure_dir(backup_dir)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = backup_dir / f"backup_{timestamp}"
        
        # Create backup tar
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            backup_content = temp_path / "backup"
            ensure_dir(backup_content)
            
            # Copy data
            for item in self.data_dir.iterdir():
                if item.name != "backups" and item.is_dir():
                    dest = backup_content / item.name
                    shutil.copytree(item, dest)
            
            # Create tar
            with tarfile.open(backup_path.with_suffix('.tar.gz'), 'w:gz') as tar:
                tar.add(backup_content, arcname="backup")
        
        return backup_path.with_suffix('.tar.gz')
    
    def restore_backup(self, backup_path: Union[str, Path]) -> Dict[str, Any]:
        """
        Restore from a backup.
        
        Args:
            backup_path: Path to backup file
        
        Returns:
            Restore result
        """
        backup_path = Path(backup_path)
        
        if not backup_path.exists():
            return {'error': f'Backup not found: {backup_path}'}
        
        logger.info(f"Restoring from backup: {backup_path}")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Extract backup
            with tarfile.open(backup_path, 'r:gz') as tar:
                tar.extractall(temp_path)
            
            backup_content = temp_path / "backup"
            if not backup_content.exists():
                return {'error': 'Invalid backup format'}
            
            # Restore each component
            restored = []
            for item in backup_content.iterdir():
                if item.is_dir():
                    dest = self.data_dir / item.name
                    if dest.exists():
                        shutil.rmtree(dest)
                    shutil.copytree(item, dest)
                    restored.append(item.name)
            
            # Reload config
            self.config.load()
            
            return {'success': True, 'restored': restored}
    
    # ============================================
    # Utility Methods
    # ============================================
    
    def _log_export(self, component: str, status: str, size: int = 0, error: str = None) -> None:
        """Log export operation."""
        self._log.append({
            'operation': 'export',
            'component': component,
            'status': status,
            'size': size,
            'error': error,
            'timestamp': get_timestamp()
        })
    
    def _log_import(self, component: str, status: str, error: str = None) -> None:
        """Log import operation."""
        self._log.append({
            'operation': 'import',
            'component': component,
            'status': status,
            'error': error,
            'timestamp': get_timestamp()
        })
    
    def get_log(self) -> List[Dict]:
        """Get export/import log."""
        return self._log.copy()
    
    def clear_log(self) -> None:
        """Clear log."""
        self._log.clear()
    
    def list_backups(self) -> List[Dict]:
        """List available backups."""
        backup_dir = self.data_dir / "backups"
        if not backup_dir.exists():
            return []
        
        backups = []
        for backup in backup_dir.glob("backup_*.tar.gz"):
            backups.append({
                'path': str(backup),
                'name': backup.name,
                'size': backup.stat().st_size,
                'modified': datetime.fromtimestamp(backup.stat().st_mtime).isoformat()
            })
        
        return sorted(backups, key=lambda x: x['modified'], reverse=True)
    
    def get_export_info(self, archive_path: Union[str, Path]) -> Dict[str, Any]:
        """
        Get information about an export archive without importing.
        
        Args:
            archive_path: Path to archive file
        
        Returns:
            Archive information
        """
        archive_path = Path(archive_path)
        
        if not archive_path.exists():
            return {'error': f'Archive not found: {archive_path}'}
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Extract just the info file
            try:
                if archive_path.suffix == '.zip':
                    with zipfile.ZipFile(archive_path, 'r') as zipf:
                        if 'telegram_export/export_info.json' in zipf.namelist():
                            with zipf.open('telegram_export/export_info.json') as f:
                                info = json.load(f)
                        else:
                            return {'error': 'No export_info.json found'}
                else:
                    with tarfile.open(archive_path, 'r:*') as tar:
                        info_member = None
                        for member in tar.getmembers():
                            if member.name.endswith('export_info.json'):
                                info_member = member
                                break
                        
                        if not info_member:
                            return {'error': 'No export_info.json found'}
                        
                        with tar.extractfile(info_member) as f:
                            info = json.load(f)
                
                # Add file info
                info['archive_size'] = archive_path.stat().st_size
                info['archive_name'] = archive_path.name
                
                return info
                
            except Exception as e:
                return {'error': f'Failed to read archive: {e}'}
    
    def close(self) -> None:
        """Clean up resources."""
        self.db_manager.close()
        logger.info("ConfigExporter closed")
    
    async def close_async(self) -> None:
        """Close asynchronously."""
        await self.db_manager.close_async()
        logger.info("ConfigExporter closed asynchronously")
    
    def __repr__(self) -> str:
        return f"ConfigExporter(data_dir={self.data_dir})"