#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
telegram_cli/core/batch_processor.py – Advanced batch processing with queue system

Handles:
- Batch upload/download with queue management
- Parallel processing with configurable concurrency
- Priority-based job scheduling
- Retry logic with exponential backoff
- Progress tracking and status reporting
- Pause/resume/cancel operations
- Job history and statistics
- Dependencies between jobs
- Scheduled batch jobs
"""

import asyncio
import uuid
import time
import json
from pathlib import Path
from typing import Optional, List, Dict, Any, Union, Callable, Tuple
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass, field, asdict
from collections import deque
import random

# Import core modules
try:
    from telegram_cli.core.uploader import Uploader
    from telegram_cli.core.downloader import Downloader
    from telegram_cli.core.file_tracker import FileTracker
    from telegram_cli.database.db_manager import DatabaseManager
    from telegram_cli.utils.logger import get_logger
    from telegram_cli.utils.progress_bar import ProgressBar
    from telegram_cli.utils.helpers import format_size, get_timestamp, human_readable_time
    MODULES_LOADED = True
except ImportError:
    MODULES_LOADED = False
    import logging
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.NullHandler())
    
    class Uploader:
        async def upload_file(self, **kwargs): return {"file_id": "123"}
        async def upload_batch(self, **kwargs): return []
    class Downloader:
        async def download_file(self, **kwargs): return {"success": True}
    class FileTracker:
        def record_upload(self, **kwargs): pass
    class DatabaseManager:
        def execute_write(self, q, p=None): pass
        def execute_query(self, q, p=None): return []
    def format_size(s): return f"{s/1024:.2f} KB"
    def get_timestamp(): return datetime.now().isoformat()
    def human_readable_time(s): return f"{s:.2f}s"

logger = get_logger(__name__)


# ============================================
# Enums and Data Classes
# ============================================

class JobStatus(Enum):
    """Job status enum."""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"
    SKIPPED = "skipped"


class JobPriority(Enum):
    """Job priority levels."""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


class JobType(Enum):
    """Job type enum."""
    UPLOAD = "upload"
    DOWNLOAD = "download"
    SEARCH = "search"
    EXPORT = "export"
    CLEANUP = "cleanup"
    CUSTOM = "custom"


@dataclass
class Job:
    """Job data class."""
    id: str
    type: JobType
    status: JobStatus
    priority: JobPriority
    data: Dict[str, Any]
    created_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    result: Optional[Dict] = None
    progress: float = 0.0
    dependencies: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        """Convert job to dictionary."""
        return {
            'id': self.id,
            'type': self.type.value,
            'status': self.status.value,
            'priority': self.priority.value,
            'data': self.data,
            'created_at': self.created_at,
            'started_at': self.started_at,
            'completed_at': self.completed_at,
            'error': self.error,
            'retry_count': self.retry_count,
            'max_retries': self.max_retries,
            'result': self.result,
            'progress': self.progress,
            'dependencies': self.dependencies,
            'tags': self.tags,
            'metadata': self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Job':
        """Create job from dictionary."""
        return cls(
            id=data['id'],
            type=JobType(data['type']),
            status=JobStatus(data['status']),
            priority=JobPriority(data['priority']),
            data=data['data'],
            created_at=data['created_at'],
            started_at=data.get('started_at'),
            completed_at=data.get('completed_at'),
            error=data.get('error'),
            retry_count=data.get('retry_count', 0),
            max_retries=data.get('max_retries', 3),
            result=data.get('result'),
            progress=data.get('progress', 0.0),
            dependencies=data.get('dependencies', []),
            tags=data.get('tags', []),
            metadata=data.get('metadata', {})
        )


# ============================================
# Batch Processor
# ============================================

class BatchProcessor:
    """
    Advanced batch processor with queue management, parallel execution,
    retry logic, and progress tracking.
    """
    
    def __init__(
        self,
        uploader: Optional[Uploader] = None,
        downloader: Optional[Downloader] = None,
        tracker: Optional[FileTracker] = None,
        db_manager: Optional[DatabaseManager] = None,
        max_workers: int = 4,
        max_retries: int = 3,
        retry_delay_base: float = 5.0,
        retry_max_delay: float = 300.0,
        enable_persistence: bool = True,
        db_path: Optional[Path] = None
    ):
        """
        Initialize the batch processor.
        
        Args:
            uploader: Uploader instance
            downloader: Downloader instance
            tracker: FileTracker instance
            db_manager: DatabaseManager instance
            max_workers: Maximum parallel workers
            max_retries: Default max retries per job
            retry_delay_base: Base delay for exponential backoff (seconds)
            retry_max_delay: Maximum delay for retry (seconds)
            enable_persistence: Save job state to database
            db_path: Database path for persistence
        """
        self.uploader = uploader or Uploader()
        self.downloader = downloader or Downloader()
        self.tracker = tracker or FileTracker()
        self.db_manager = db_manager or DatabaseManager(db_path)
        
        self.max_workers = min(max(max_workers, 1), 10)
        self.max_retries = max_retries
        self.retry_delay_base = retry_delay_base
        self.retry_max_delay = retry_max_delay
        self.enable_persistence = enable_persistence
        
        # Queues
        self._pending_queue = deque()
        self._priority_queues = {
            JobPriority.CRITICAL: deque(),
            JobPriority.HIGH: deque(),
            JobPriority.NORMAL: deque(),
            JobPriority.LOW: deque()
        }
        
        # Job tracking
        self._jobs: Dict[str, Job] = {}
        self._active_jobs: Dict[str, Job] = {}
        self._job_history: List[Dict] = []
        self._failed_jobs: List[Job] = []
        self._completed_jobs: List[Job] = []
        
        # Control flags
        self._is_running = False
        self._is_paused = False
        self._stop_requested = False
        self._workers = []
        
        # Statistics
        self._stats = {
            'total_jobs': 0,
            'completed_jobs': 0,
            'failed_jobs': 0,
            'cancelled_jobs': 0,
            'retried_jobs': 0,
            'total_time': 0,
            'start_time': None,
            'end_time': None
        }
        
        # Callbacks
        self._on_job_start = None
        self._on_job_progress = None
        self._on_job_complete = None
        self._on_job_fail = None
        self._on_batch_complete = None
        
        # Load persisted jobs
        if self.enable_persistence:
            self._load_persisted_jobs()
        
        logger.info(f"BatchProcessor initialized (workers: {max_workers})")
    
    # ============================================
    # Job Management
    # ============================================
    
    def add_job(
        self,
        job_type: Union[JobType, str],
        data: Dict[str, Any],
        priority: Union[JobPriority, int] = JobPriority.NORMAL,
        dependencies: Optional[List[str]] = None,
        max_retries: Optional[int] = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict] = None
    ) -> str:
        """
        Add a new job to the queue.
        
        Args:
            job_type: Type of job (upload, download, etc.)
            data: Job-specific data (file path, channel, etc.)
            priority: Job priority (LOW=0, NORMAL=1, HIGH=2, CRITICAL=3)
            dependencies: List of job IDs that must complete first
            max_retries: Max retries for this job
            tags: Tags for categorization
            metadata: Additional metadata
        
        Returns:
            Job ID
        """
        # Convert string to enum
        if isinstance(job_type, str):
            job_type = JobType(job_type.lower())
        
        if isinstance(priority, int):
            priority = JobPriority(priority)
        
        job_id = self._generate_job_id()
        
        job = Job(
            id=job_id,
            type=job_type,
            status=JobStatus.PENDING,
            priority=priority,
            data=data,
            created_at=get_timestamp(),
            max_retries=max_retries or self.max_retries,
            dependencies=dependencies or [],
            tags=tags or [],
            metadata=metadata or {}
        )
        
        # Store job
        self._jobs[job_id] = job
        
        # Add to queue
        self._add_to_queue(job)
        
        # Persist if enabled
        if self.enable_persistence:
            self._save_job(job)
        
        self._stats['total_jobs'] += 1
        
        logger.info(f"Added job {job_id} ({job_type.value}) with priority {priority.name}")
        
        # Start processor if not running
        if not self._is_running and not self._stop_requested:
            asyncio.create_task(self.process())
        
        return job_id
    
    def _add_to_queue(self, job: Job) -> None:
        """Add job to appropriate priority queue."""
        # Check dependencies
        if job.dependencies:
            # Check if dependencies are met
            for dep_id in job.dependencies:
                dep_job = self._jobs.get(dep_id)
                if not dep_job or dep_job.status not in [JobStatus.COMPLETED, JobStatus.SKIPPED]:
                    # Put in pending queue to check later
                    self._pending_queue.append(job)
                    return
        
        # Add to priority queue
        self._priority_queues[job.priority].append(job)
        job.status = JobStatus.QUEUED
        
        if self.enable_persistence:
            self._update_job_status(job)
    
    def _get_next_job(self) -> Optional[Job]:
        """Get the next job from queues based on priority."""
        # Check pending jobs for dependency resolution
        pending_to_requeue = []
        while self._pending_queue:
            job = self._pending_queue.popleft()
            dependencies_met = True
            
            for dep_id in job.dependencies:
                dep_job = self._jobs.get(dep_id)
                if not dep_job or dep_job.status not in [JobStatus.COMPLETED, JobStatus.SKIPPED]:
                    dependencies_met = False
                    break
            
            if dependencies_met:
                # Add to priority queue
                self._priority_queues[job.priority].append(job)
                job.status = JobStatus.QUEUED
            else:
                # Re-add to pending
                pending_to_requeue.append(job)
        
        # Restore pending jobs
        for job in pending_to_requeue:
            self._pending_queue.append(job)
        
        # Get from priority queues
        for priority in [JobPriority.CRITICAL, JobPriority.HIGH, JobPriority.NORMAL, JobPriority.LOW]:
            if self._priority_queues[priority]:
                job = self._priority_queues[priority].popleft()
                if job.status == JobStatus.QUEUED:
                    return job
        
        return None
    
    def get_job(self, job_id: str) -> Optional[Dict]:
        """Get job details by ID."""
        job = self._jobs.get(job_id)
        if job:
            return job.to_dict()
        return None
    
    def get_jobs(
        self,
        status: Optional[JobStatus] = None,
        job_type: Optional[JobType] = None,
        tags: Optional[List[str]] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict]:
        """
        Get jobs with filters.
        
        Args:
            status: Filter by status
            job_type: Filter by job type
            tags: Filter by tags (AND condition)
            limit: Max results
            offset: Result offset
        
        Returns:
            List of job dicts
        """
        result = []
        
        for job in self._jobs.values():
            # Apply filters
            if status and job.status != status:
                continue
            
            if job_type and job.type != job_type:
                continue
            
            if tags:
                if not all(tag in job.tags for tag in tags):
                    continue
            
            result.append(job.to_dict())
        
        # Sort by creation time (newest first)
        result.sort(key=lambda x: x['created_at'], reverse=True)
        
        return result[offset:offset+limit]
    
    def cancel_job(self, job_id: str, force: bool = False) -> bool:
        """
        Cancel a job.
        
        Args:
            job_id: Job ID to cancel
            force: Force cancel even if running
        
        Returns:
            True if cancelled
        """
        job = self._jobs.get(job_id)
        if not job:
            return False
        
        # If job is running and not force, can't cancel
        if job.status == JobStatus.RUNNING and not force:
            logger.warning(f"Job {job_id} is running, use force=True to cancel")
            return False
        
        # Remove from queues
        if job.status in [JobStatus.PENDING, JobStatus.QUEUED]:
            self._remove_from_queues(job)
        
        job.status = JobStatus.CANCELLED
        job.completed_at = get_timestamp()
        
        self._stats['cancelled_jobs'] += 1
        
        if self.enable_persistence:
            self._update_job_status(job)
        
        logger.info(f"Cancelled job {job_id}")
        return True
    
    def _remove_from_queues(self, job: Job) -> None:
        """Remove a job from all queues."""
        # Remove from pending queue
        if job in self._pending_queue:
            self._pending_queue.remove(job)
        
        # Remove from priority queues
        for queue in self._priority_queues.values():
            if job in queue:
                queue.remove(job)
    
    def pause(self) -> None:
        """Pause processing."""
        self._is_paused = True
        logger.info("Batch processor paused")
    
    def resume(self) -> None:
        """Resume processing."""
        self._is_paused = False
        logger.info("Batch processor resumed")
    
    def stop(self) -> None:
        """Stop processing after current jobs."""
        self._stop_requested = True
        logger.info("Batch processor stop requested")
    
    # ============================================
    # Processing Logic
    # ============================================
    
    async def process(self) -> None:
        """Start processing jobs."""
        if self._is_running:
            return
        
        self._is_running = True
        self._stop_requested = False
        self._stats['start_time'] = get_timestamp()
        
        logger.info("Batch processor started")
        
        # Create workers
        self._workers = []
        for i in range(self.max_workers):
            worker = asyncio.create_task(self._worker_loop(i))
            self._workers.append(worker)
        
        # Wait for all workers to finish
        await asyncio.gather(*self._workers)
        
        self._is_running = False
        self._stats['end_time'] = get_timestamp()
        
        # Calculate total time
        if self._stats['start_time'] and self._stats['end_time']:
            start = datetime.fromisoformat(self._stats['start_time'])
            end = datetime.fromisoformat(self._stats['end_time'])
            self._stats['total_time'] = (end - start).total_seconds()
        
        # Trigger completion callback
        if self._on_batch_complete:
            self._on_batch_complete(self._stats)
        
        logger.info("Batch processor stopped")
    
    async def _worker_loop(self, worker_id: int) -> None:
        """Worker loop for processing jobs."""
        while not self._stop_requested:
            if self._is_paused:
                await asyncio.sleep(0.5)
                continue
            
            # Get next job
            job = self._get_next_job()
            if not job:
                # No jobs, wait a bit
                await asyncio.sleep(0.5)
                continue
            
            # Process job
            await self._process_job(job, worker_id)
        
        logger.info(f"Worker {worker_id} stopped")
    
    async def _process_job(self, job: Job, worker_id: int) -> None:
        """Process a single job."""
        try:
            job.status = JobStatus.RUNNING
            job.started_at = get_timestamp()
            self._active_jobs[job.id] = job
            
            if self.enable_persistence:
                self._update_job_status(job)
            
            logger.info(f"Worker {worker_id}: Processing job {job.id} ({job.type.value})")
            
            # Trigger start callback
            if self._on_job_start:
                self._on_job_start(job)
            
            # Execute based on job type
            result = await self._execute_job(job)
            
            # Handle result
            if result.get('success', False):
                job.status = JobStatus.COMPLETED
                job.result = result
                self._completed_jobs.append(job)
                self._stats['completed_jobs'] += 1
                logger.info(f"Worker {worker_id}: Job {job.id} completed")
            else:
                # Check if should retry
                if job.retry_count < job.max_retries:
                    job.status = JobStatus.RETRYING
                    job.retry_count += 1
                    self._stats['retried_jobs'] += 1
                    
                    # Calculate delay
                    delay = self._calculate_retry_delay(job.retry_count)
                    logger.info(f"Worker {worker_id}: Job {job.id} failed, retrying in {delay:.1f}s (attempt {job.retry_count}/{job.max_retries})")
                    
                    # Re-add to queue after delay
                    asyncio.create_task(self._requeue_after_delay(job, delay))
                    return
                else:
                    job.status = JobStatus.FAILED
                    job.error = result.get('error', 'Unknown error')
                    self._failed_jobs.append(job)
                    self._stats['failed_jobs'] += 1
                    logger.error(f"Worker {worker_id}: Job {job.id} failed permanently: {job.error}")
                    
                    # Trigger fail callback
                    if self._on_job_fail:
                        self._on_job_fail(job, job.error)
            
            job.completed_at = get_timestamp()
            job.progress = 100.0
            
            # Remove from active jobs
            if job.id in self._active_jobs:
                del self._active_jobs[job.id]
            
            # Update persistence
            if self.enable_persistence:
                self._update_job_status(job)
            
            # Trigger complete callback
            if self._on_job_complete:
                self._on_job_complete(job)
            
            # Add to history
            self._job_history.append(job.to_dict())
            
        except Exception as e:
            logger.error(f"Worker {worker_id}: Error processing job {job.id}: {e}")
            job.status = JobStatus.FAILED
            job.error = str(e)
            self._failed_jobs.append(job)
            self._stats['failed_jobs'] += 1
            
            if self.enable_persistence:
                self._update_job_status(job)
            
            # Remove from active jobs
            if job.id in self._active_jobs:
                del self._active_jobs[job.id]
    
    async def _execute_job(self, job: Job) -> Dict[str, Any]:
        """Execute a job based on its type."""
        data = job.data
        
        if job.type == JobType.UPLOAD:
            # Upload job
            file_path = data.get('file_path')
            if not file_path:
                return {'success': False, 'error': 'Missing file_path'}
            
            # If it's a list, upload multiple files
            if isinstance(file_path, list):
                return await self._execute_batch_upload(file_path, data)
            else:
                return await self._execute_single_upload(file_path, data)
        
        elif job.type == JobType.DOWNLOAD:
            # Download job
            file_id = data.get('file_id')
            if not file_id:
                return {'success': False, 'error': 'Missing file_id'}
            
            return await self._execute_single_download(file_id, data)
        
        elif job.type == JobType.SEARCH:
            # Search job
            query = data.get('query')
            if not query:
                return {'success': False, 'error': 'Missing query'}
            
            return await self._execute_search(query, data)
        
        elif job.type == JobType.EXPORT:
            # Export job
            return await self._execute_export(data)
        
        elif job.type == JobType.CLEANUP:
            # Cleanup job
            return await self._execute_cleanup(data)
        
        elif job.type == JobType.CUSTOM:
            # Custom job - execute callback if provided
            callback = data.get('callback')
            if callback and callable(callback):
                try:
                    result = await callback(data)
                    return {'success': True, 'result': result}
                except Exception as e:
                    return {'success': False, 'error': str(e)}
            else:
                return {'success': False, 'error': 'No callback provided for custom job'}
        
        else:
            return {'success': False, 'error': f'Unknown job type: {job.type}'}
    
    # ============================================
    # Job Execution Helpers
    # ============================================
    
    async def _execute_single_upload(self, file_path: Path, data: Dict) -> Dict[str, Any]:
        """Execute a single upload job."""
        try:
            result = await self.uploader.upload_file(
                file_path=file_path,
                description=data.get('description'),
                channel=data.get('channel'),
                account_phone=data.get('account_phone'),
                encrypt=data.get('encrypt', True),
                tags=data.get('tags'),
                metadata=data.get('metadata')
            )
            return {'success': True, 'result': result}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    async def _execute_batch_upload(self, file_paths: List[Path], data: Dict) -> Dict[str, Any]:
        """Execute a batch upload job."""
        try:
            results = []
            for file_path in file_paths:
                try:
                    result = await self.uploader.upload_file(
                        file_path=file_path,
                        description=data.get('description'),
                        channel=data.get('channel'),
                        account_phone=data.get('account_phone'),
                        encrypt=data.get('encrypt', True),
                        tags=data.get('tags'),
                        metadata=data.get('metadata')
                    )
                    results.append({'file': str(file_path), 'success': True, 'result': result})
                except Exception as e:
                    results.append({'file': str(file_path), 'success': False, 'error': str(e)})
            
            success_count = sum(1 for r in results if r.get('success'))
            return {
                'success': success_count > 0,
                'total': len(results),
                'success_count': success_count,
                'failed_count': len(results) - success_count,
                'results': results
            }
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    async def _execute_single_download(self, file_id: str, data: Dict) -> Dict[str, Any]:
        """Execute a single download job."""
        try:
            result = await self.downloader.download_file(
                file_id=file_id,
                output_path=data.get('output_path'),
                decrypt=data.get('decrypt', True),
                password=data.get('password'),
                account_phone=data.get('account_phone')
            )
            return {'success': True, 'result': result}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    async def _execute_search(self, query: str, data: Dict) -> Dict[str, Any]:
        """Execute a search job."""
        try:
            # Import searcher here to avoid circular imports
            from telegram_cli.core.searcher import Searcher
            searcher = Searcher()
            
            result = await searcher.search(
                query=query,
                accounts=data.get('accounts'),
                channels=data.get('channels'),
                filter_type=data.get('filter_type', 'all'),
                limit=data.get('limit', 100),
                download_matches=data.get('download_matches', False)
            )
            return {'success': True, 'result': result}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    async def _execute_export(self, data: Dict) -> Dict[str, Any]:
        """Execute an export job."""
        try:
            export_type = data.get('export_type', 'json')
            output_path = data.get('output_path')
            file_ids = data.get('file_ids')
            
            if not output_path:
                return {'success': False, 'error': 'Missing output_path'}
            
            if export_type == 'json':
                success = self.tracker.export_to_json(output_path, file_ids)
            elif export_type == 'csv':
                success = self.tracker.export_to_csv(output_path, file_ids)
            else:
                return {'success': False, 'error': f'Unknown export type: {export_type}'}
            
            return {'success': success, 'path': str(output_path)}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    async def _execute_cleanup(self, data: Dict) -> Dict[str, Any]:
        """Execute a cleanup job."""
        try:
            days = data.get('days', 30)
            cleanup_type = data.get('cleanup_type', 'temporary')
            
            # Clean temporary files older than N days
            if cleanup_type == 'temporary':
                from telegram_cli.utils.helpers import cleanup_temp_files
                cleaned = cleanup_temp_files(days)
                return {'success': True, 'cleaned': cleaned}
            
            return {'success': False, 'error': f'Unknown cleanup type: {cleanup_type}'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _calculate_retry_delay(self, retry_count: int) -> float:
        """Calculate delay for retry with exponential backoff."""
        delay = self.retry_delay_base * (2 ** (retry_count - 1))
        delay = min(delay, self.retry_max_delay)
        # Add jitter
        delay *= (0.8 + 0.4 * random.random())
        return delay
    
    async def _requeue_after_delay(self, job: Job, delay: float) -> None:
        """Re-add job to queue after a delay."""
        await asyncio.sleep(delay)
        # Reset job status to QUEUED
        job.status = JobStatus.QUEUED
        # Don't add to pending, put directly back in priority queue
        self._priority_queues[job.priority].appendleft(job)
        
        if self.enable_persistence:
            self._update_job_status(job)
        
        logger.info(f"Requeued job {job.id} after delay")
    
    # ============================================
    # Persistence
    # ============================================
    
    def _save_job(self, job: Job) -> None:
        """Save job to database."""
        try:
            query = """
            INSERT OR REPLACE INTO batch_jobs (
                job_id, job_type, status, priority, data,
                created_at, started_at, completed_at, error,
                retry_count, max_retries, result, progress,
                dependencies, tags, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            params = (
                job.id,
                job.type.value,
                job.status.value,
                job.priority.value,
                json.dumps(job.data),
                job.created_at,
                job.started_at,
                job.completed_at,
                job.error,
                job.retry_count,
                job.max_retries,
                json.dumps(job.result) if job.result else None,
                job.progress,
                json.dumps(job.dependencies),
                json.dumps(job.tags),
                json.dumps(job.metadata)
            )
            
            self.db_manager.execute_write(query, params)
        except Exception as e:
            logger.error(f"Failed to save job {job.id}: {e}")
    
    def _update_job_status(self, job: Job) -> None:
        """Update job status in database."""
        try:
            query = """
            UPDATE batch_jobs 
            SET status = ?, started_at = ?, completed_at = ?, 
                error = ?, retry_count = ?, result = ?, progress = ?
            WHERE job_id = ?
            """
            
            params = (
                job.status.value,
                job.started_at,
                job.completed_at,
                job.error,
                job.retry_count,
                json.dumps(job.result) if job.result else None,
                job.progress,
                job.id
            )
            
            self.db_manager.execute_write(query, params)
        except Exception as e:
            logger.error(f"Failed to update job {job.id}: {e}")
    
    def _load_persisted_jobs(self) -> None:
        """Load persisted jobs from database."""
        try:
            # Create table if not exists
            create_table = """
            CREATE TABLE IF NOT EXISTS batch_jobs (
                job_id TEXT PRIMARY KEY,
                job_type TEXT NOT NULL,
                status TEXT NOT NULL,
                priority INTEGER NOT NULL,
                data TEXT NOT NULL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                error TEXT,
                retry_count INTEGER DEFAULT 0,
                max_retries INTEGER DEFAULT 3,
                result TEXT,
                progress REAL DEFAULT 0,
                dependencies TEXT,
                tags TEXT,
                metadata TEXT
            )
            """
            self.db_manager.execute_write(create_table)
            
            # Load jobs that are not completed/cancelled/failed
            query = """
            SELECT * FROM batch_jobs 
            WHERE status IN ('pending', 'queued', 'running', 'retrying')
            ORDER BY created_at
            """
            rows = self.db_manager.execute_query(query)
            
            for row in rows:
                data = {
                    'id': row[0],
                    'type': JobType(row[1]),
                    'status': JobStatus(row[2]),
                    'priority': JobPriority(row[3]),
                    'data': json.loads(row[4]),
                    'created_at': row[5],
                    'started_at': row[6],
                    'completed_at': row[7],
                    'error': row[8],
                    'retry_count': row[9] or 0,
                    'max_retries': row[10] or 3,
                    'result': json.loads(row[11]) if row[11] else None,
                    'progress': row[12] or 0.0,
                    'dependencies': json.loads(row[13]) if row[13] else [],
                    'tags': json.loads(row[14]) if row[14] else [],
                    'metadata': json.loads(row[15]) if row[15] else {}
                }
                
                job = Job.from_dict(data)
                self._jobs[job.id] = job
                
                # Re-add to queue if not running
                if job.status != JobStatus.RUNNING:
                    self._add_to_queue(job)
            
            logger.info(f"Loaded {len(rows)} persisted jobs")
        except Exception as e:
            logger.error(f"Failed to load persisted jobs: {e}")
    
    # ============================================
    # Callbacks
    # ============================================
    
    def on_job_start(self, callback: Callable) -> None:
        """Set callback for job start."""
        self._on_job_start = callback
    
    def on_job_progress(self, callback: Callable) -> None:
        """Set callback for job progress."""
        self._on_job_progress = callback
    
    def on_job_complete(self, callback: Callable) -> None:
        """Set callback for job completion."""
        self._on_job_complete = callback
    
    def on_job_fail(self, callback: Callable) -> None:
        """Set callback for job failure."""
        self._on_job_fail = callback
    
    def on_batch_complete(self, callback: Callable) -> None:
        """Set callback for batch completion."""
        self._on_batch_complete = callback
    
    def update_progress(self, job_id: str, progress: float) -> None:
        """Update progress for a job."""
        job = self._jobs.get(job_id)
        if job:
            job.progress = min(max(progress, 0.0), 100.0)
            if self._on_job_progress:
                self._on_job_progress(job)
    
    # ============================================
    # Utility Methods
    # ============================================
    
    def _generate_job_id(self) -> str:
        """Generate a unique job ID."""
        return f"job_{uuid.uuid4().hex[:16]}_{int(time.time())}"
    
    def get_stats(self) -> Dict[str, Any]:
        """Get processor statistics."""
        stats = self._stats.copy()
        stats.update({
            'queue_size': sum(len(q) for q in self._priority_queues.values()) + len(self._pending_queue),
            'active_jobs': len(self._active_jobs),
            'is_running': self._is_running,
            'is_paused': self._is_paused,
            'total_jobs_in_system': len(self._jobs)
        })
        return stats
    
    def get_queue_status(self) -> Dict[str, int]:
        """Get queue sizes by priority."""
        return {
            'pending': len(self._pending_queue),
            'critical': len(self._priority_queues[JobPriority.CRITICAL]),
            'high': len(self._priority_queues[JobPriority.HIGH]),
            'normal': len(self._priority_queues[JobPriority.NORMAL]),
            'low': len(self._priority_queues[JobPriority.LOW])
        }
    
    def clear_completed(self) -> int:
        """Clear completed jobs from memory."""
        count = len(self._completed_jobs)
        self._completed_jobs.clear()
        return count
    
    def clear_failed(self) -> int:
        """Clear failed jobs from memory."""
        count = len(self._failed_jobs)
        self._failed_jobs.clear()
        return count
    
    def reset(self) -> None:
        """Reset the processor."""
        self._pending_queue.clear()
        for queue in self._priority_queues.values():
            queue.clear()
        self._jobs.clear()
        self._active_jobs.clear()
        self._job_history.clear()
        self._completed_jobs.clear()
        self._failed_jobs.clear()
        self._stats = {
            'total_jobs': 0,
            'completed_jobs': 0,
            'failed_jobs': 0,
            'cancelled_jobs': 0,
            'retried_jobs': 0,
            'total_time': 0,
            'start_time': None,
            'end_time': None
        }
        logger.info("Batch processor reset")
    
    async def close(self) -> None:
        """Clean up resources."""
        self._stop_requested = True
        if self._is_running:
            await asyncio.gather(*self._workers, return_exceptions=True)
        
        if self.enable_persistence:
            # Save any pending jobs
            for job in self._jobs.values():
                if job.status in [JobStatus.PENDING, JobStatus.QUEUED, JobStatus.RUNNING]:
                    self._save_job(job)
        
        logger.info("Batch processor closed")
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    def __repr__(self) -> str:
        return f"BatchProcessor(jobs={len(self._jobs)}, running={self._is_running})"