import time
import threading
import logging
import os
import uuid
import pathlib
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor
from .cluster import ClusterNode, OptimizedClusterManager

# Configure logging
logger = logging.getLogger('DistributedScheduler')
logger.setLevel(logging.INFO)
# Prevent adding multiple handlers if module is reloaded or function called multiple times
if not logger.handlers:
    fh = logging.FileHandler('cluster_run.log', encoding='utf-8')
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(fh)
    # Console handler for critical errors only
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    logger.addHandler(ch)

class TaskStatus:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

@dataclass
class Task:
    command: str
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: str = TaskStatus.PENDING
    node_name: Optional[str] = None
    retries: int = 0
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    error_msg: Optional[str] = None
    
    @property
    def duration(self) -> float:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        if self.start_time:
            return time.time() - self.start_time
        return 0.0

class DistributedScheduler:
    def __init__(self, nodes: List[ClusterNode], cluster_manager: OptimizedClusterManager, max_retries: int = 3, timeout: int = 0):
        self.nodes = {n.name: n for n in nodes if n.enabled}
        self.cluster_manager = cluster_manager
        self.max_retries = max_retries
        self.timeout = timeout
        
        self.tasks: List[Task] = []
        self.task_map: Dict[str, Task] = {}
        
        # Node state
        self.node_states = {
            name: {
                "running_jobs": 0,
                "failed_count": 0,
                "last_active": time.time(),
                "disabled": False
            } for name in self.nodes
        }
        
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        # Create enough threads for all potential concurrent jobs
        # Sum of max_jobs across all nodes + buffer
        total_slots = sum(n.max_jobs for n in self.nodes.values())
        self.executor = ThreadPoolExecutor(max_workers=max(10, total_slots + 5))

    def submit(self, commands: List[str]):
        """Submit a list of commands as tasks"""
        with self.lock:
            for cmd in commands:
                task = Task(command=cmd)
                self.tasks.append(task)
                self.task_map[task.id] = task
        logger.info(f"Submitted {len(commands)} tasks")

    def _get_available_node(self) -> Optional[str]:
        """Get a node with available slots"""
        candidates = []
        for name, state in self.node_states.items():
            if state['disabled']:
                continue
            node = self.nodes.get(name)
            if not node: 
                continue
            
            # Check slots
            if state['running_jobs'] < node.max_jobs:
                candidates.append((name, state['running_jobs'], node.max_jobs))
        
        if not candidates:
            return None
            
        # Strategy: Fill nodes with lowest load ratio first
        candidates.sort(key=lambda x: x[1] / max(1, x[2]))
        return candidates[0][0]

    def _run_task_on_node(self, task: Task, node_name: str):
        """Execute task on specific node"""
        if self.stop_event.is_set():
            return

        node = self.nodes[node_name]
        
        with self.lock:
            task.status = TaskStatus.RUNNING
            task.node_name = node_name
            task.start_time = time.time()
            self.node_states[node_name]['running_jobs'] += 1
        
        logger.info(f"Task {task.id} started on {node_name}: {task.command[:50]}...")
        
        try:
            ssh = self.cluster_manager._create_ssh_connection(node)
            if not ssh:
                raise Exception(f"Failed to connect to {node_name}")
            
            # Execute
            # Note: exec_command returns (stdin, stdout, stderr)
            # The command is executed asynchronously on the server
            stdin, stdout, stderr = ssh.exec_command(task.command)
            channel = stdout.channel
            
            # Wait for completion with timeout support
            while not channel.exit_status_ready():
                if self.stop_event.is_set():
                    # Try to kill if possible (optional)
                    channel.close()
                    return
                
                if self.timeout > 0 and (time.time() - task.start_time) > self.timeout:
                     channel.close()
                     raise TimeoutError(f"Task exceeded timeout of {self.timeout}s")
                
                time.sleep(0.5)
            
            exit_status = channel.recv_exit_status()
            
            if exit_status != 0:
                err = stderr.read().decode('utf-8', errors='ignore')
                raise Exception(f"Command failed with status {exit_status}: {err.strip()}")
                
            with self.lock:
                task.status = TaskStatus.COMPLETED
                task.end_time = time.time()
                self.node_states[node_name]['running_jobs'] -= 1
            logger.info(f"Task {task.id} completed on {node_name}")
            
        except Exception as e:
            logger.error(f"Task {task.id} failed on {node_name}: {str(e)}")
            with self.lock:
                self.node_states[node_name]['running_jobs'] -= 1
                
                # Retry logic
                if task.retries < self.max_retries:
                    task.retries += 1
                    task.status = TaskStatus.PENDING # Re-queue
                    task.node_name = None
                    task.error_msg = str(e)
                    logger.warning(f"Task {task.id} queued for retry ({task.retries}/{self.max_retries})")
                    
                    # Fault tolerance: If node failed connection, mark suspicious
                    if "connect" in str(e).lower() or "socket" in str(e).lower():
                        self.node_states[node_name]['failed_count'] += 1
                        if self.node_states[node_name]['failed_count'] > 3:
                            self.node_states[node_name]['disabled'] = True
                            logger.error(f"Node {node_name} disabled due to excessive failures")
                else:
                    task.status = TaskStatus.FAILED
                    task.end_time = time.time()
                    task.error_msg = str(e)

    def run(self):
        """Main scheduling loop"""
        logger.info("Scheduler started")
        try:
            while not self.stop_event.is_set():
                with self.lock:
                    pending_tasks = [t for t in self.tasks if t.status == TaskStatus.PENDING]
                    running_count = len([t for t in self.tasks if t.status == TaskStatus.RUNNING])
                    
                if not pending_tasks and running_count == 0:
                    break
                    
                if not pending_tasks:
                    time.sleep(0.5)
                    continue
                    
                # Try to schedule pending tasks
                for task in pending_tasks:
                    node_name = self._get_available_node()
                    if node_name:
                        # Double check task status in case it changed (rare race)
                        if task.status == TaskStatus.PENDING:
                             self.executor.submit(self._run_task_on_node, task, node_name)
                    else:
                        # No nodes available, wait a bit
                        break 
                
                time.sleep(0.1)
        except KeyboardInterrupt:
            logger.warning("Scheduler interrupted by user")
            self.stop()
        finally:
            self.executor.shutdown(wait=False)
            logger.info("Scheduler finished")

    def stop(self):
        self.stop_event.set()

def run_distributed(commands: List[str], args) -> bool:
    """
    Helper to run commands distributedly.
    args: parsed arguments containing 'nodes' (optional), 'timeout' (optional), etc.
    """
    config_dir = pathlib.Path.home() / ".fansetools"
    manager = OptimizedClusterManager(config_dir)
    manager._load_cluster_config()
    
    # Filter nodes if specified
    all_nodes = list(manager.nodes.values()) if hasattr(manager, 'nodes') else [] # manager.list_nodes() if available, else direct access
    if hasattr(manager, 'list_nodes'):
        all_nodes = manager.list_nodes()
    
    target_nodes = []
    
    # 1. Name based filtering
    node_names = getattr(args, 'nodes', None)
    if node_names:
        names = node_names.split(',')
        target_nodes = [n for n in all_nodes if n.name in names]
    else:
        target_nodes = all_nodes

    # 2. Capability based filtering (GZIP requires fansetools)
    if getattr(args, 'require_fansetools', False):
        # Filter nodes that have fanse_path set
        original_count = len(target_nodes)
        target_nodes = [n for n in target_nodes if n.fanse_path]
        if len(target_nodes) < original_count:
            print(f"Filtered {original_count - len(target_nodes)} nodes that do not have FANSeTools installed (required for GZIP processing).")

    if not target_nodes:
        print("Error: No nodes available for distributed execution.")
        return False

    # 3. File Transfer (Reference file, etc.)
    required_files = getattr(args, 'required_files', [])
    if required_files:
        print(f"Preparing to transfer {len(required_files)} files to {len(target_nodes)} nodes...")
        for node in target_nodes:
             try:
                 ssh = manager._create_ssh_connection(node)
                 if not ssh:
                     print(f"Skipping file transfer for {node.name}: Connection failed")
                     continue
                 
                 sftp = ssh.open_sftp()
                 for local_path, remote_path in required_files:
                     # Ensure remote dir exists
                     remote_path_obj = pathlib.Path(remote_path)
                     remote_dir = str(remote_path_obj.parent).replace('\\', '/')
                     
                     # Try creating directory
                     try:
                         # Simple check if exists
                         sftp.stat(remote_dir)
                     except FileNotFoundError:
                         # Try creating
                         if manager._is_windows_system(ssh):
                             # Windows: try standard mkdir, replace / with \
                             win_dir = remote_dir.replace('/', '\\')
                             ssh.exec_command(f'mkdir "{win_dir}"')
                         else:
                             # Linux
                             ssh.exec_command(f'mkdir -p "{remote_dir}"')
                         
                     print(f"Transferring {local_path} -> {node.name}:{remote_path}")
                     sftp.put(str(local_path), str(remote_path))
                 
                 sftp.close()
                 ssh.close()
             except Exception as e:
                 print(f"Error transferring files to {node.name}: {e}")

    print(f"Distributed Scheduler: {len(commands)} tasks, {len(target_nodes)} nodes.")
    
    scheduler = DistributedScheduler(
        target_nodes, 
        cluster_manager=manager,
        timeout=getattr(args, 'timeout', 0)
    )
    
    scheduler.submit(commands)
    scheduler.run()
    
    # Summary
    completed = [t for t in scheduler.tasks if t.status == TaskStatus.COMPLETED]
    failed = [t for t in scheduler.tasks if t.status == TaskStatus.FAILED]
    
    print(f"Execution finished. Completed: {len(completed)}, Failed: {len(failed)}")
    
    if failed:
        print("Failed tasks:")
        for t in failed:
            print(f"  - {t.command[:50]}... Error: {t.error_msg}")
        return False
        
    return True

def distribute_command(commands: List[str], args: Any) -> bool:
    """
    Adapter/Decorator for business modules to invoke distributed execution.
    Usage:
        if args.cluster:
            return distribute_command(commands, args)
    """
    return run_distributed(commands, args)
