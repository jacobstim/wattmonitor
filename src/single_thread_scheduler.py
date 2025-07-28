"""
Single-threaded scheduler for coordinated meter reading
Replaces the multi-threaded RepeatedTimer approach
"""

import time
import logging
from typing import List, Callable, Dict, Any
from datetime import datetime, timedelta


class ScheduledTask:
    """Represents a scheduled task with interval and next execution time"""
    
    def __init__(self, name: str, interval: float, func: Callable, *args, **kwargs):
        self.name = name
        self.interval = timedelta(seconds=interval)
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.last_run = datetime.now() - self.interval  # Start next run immediately
        self.next_run = self.last_run 
        
    def should_run(self) -> bool:
        """Check if this task should run now"""
        return datetime.now() >= self.next_run
        
    def execute(self):
        """Execute the task and schedule next run"""
        try:
            self.last_run = self.last_run + self.interval  # We want to avoid drift because of the few ms that datetime.now() takes, so when we run, we assume this was properly triggered. 
            self.next_run = self.last_run + self.interval  
            self.func(*self.args, **self.kwargs)
            logging.info(f"Task '{self.name}' executed successfully. Next run: {self.next_run}")
        except Exception as e:
            # Next run is always scheduled after the last run, even on failure
            logging.error(f"Task '{self.name}' failed: {e}")


class SingleThreadScheduler:
    """
    Single-threaded scheduler that runs all tasks sequentially.
    This prevents race conditions in Modbus communication.
    """
    
    def __init__(self):
        self.tasks: List[ScheduledTask] = []
        self.running = False
        self._logger = logging.getLogger(__name__)
        
    def add_task(self, name: str, interval: float, func: Callable, *args, **kwargs):
        """Add a new scheduled task"""
        task = ScheduledTask(name, interval, func, *args, **kwargs)
        self.tasks.append(task)
        self._logger.info(f"Added task '{name}' with {interval}s interval")
        
    def start(self):
        """Start the scheduler main loop"""
        self.running = True
        self._logger.info("Starting single-threaded scheduler")
        
        while self.running:
            try:
                # Check all tasks and run those that are due
                current_time = datetime.now()
                tasks_run = 0
                
                for task in self.tasks:
                    if task.should_run():
                        self._logger.debug(f"Executing task: {task.name}")
                        task.execute()
                        tasks_run += 1
                
                # Sleep for a short time to prevent busy waiting
                # Use a smaller sleep interval for more precise timing
                time.sleep(0.1)
                
            except KeyboardInterrupt:
                self._logger.info("Scheduler interrupted by user")
                break
            except Exception as e:
                self._logger.error(f"Scheduler error: {e}")
                time.sleep(1)  # Wait a bit longer on error
                
        self._logger.info("Scheduler stopped")
        
    def stop(self):
        """Stop the scheduler"""
        self.running = False
        
    def get_task_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status information for all tasks"""
        status = {}
        for task in self.tasks:
            status[task.name] = {
                'interval': task.interval,
                'next_run': task.next_run,
                'last_run': task.last_run,
                'overdue': datetime.now() > task.next_run
            }
        return status
