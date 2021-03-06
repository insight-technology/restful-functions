import asyncio
import functools
import multiprocessing
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

from .modules.function import ArgDefinition, FunctionDefinition
from .modules.process import ProcessManager
from .modules.task import TaskInfo, TaskStoreSettings, task_store_factory
from .utils.logger import get_logger


class TryForkResult:
    def __init__(
            self,
            success: bool,
            message: str,
            task_id: str):
        self.success = success
        self.message = message
        self.task_id = task_id

    def to_dict(self):
        return {
            'success': self.success,
            'message': self.message,
            'task_id': self.task_id,
        }


class FunctionManager:
    """A facade class."""

    def __init__(
            self,
            task_store_settings: TaskStoreSettings = TaskStoreSettings(),
            polling_timeout_process_interval: float = 60.0,
            debug: bool = False):

        self._task_store_settings = task_store_settings

        self._function_definitions: Dict[str, FunctionDefinition] = {}

        self._process_manager = ProcessManager()
        self._task_store = task_store_factory(task_store_settings, True, logger=get_logger('TaskStore'))

        self._polling_timeout_process_interval = polling_timeout_process_interval

        self._logger = get_logger(self.__class__.__name__, debug=debug)

    @property
    def definitions(self):
        return self._function_definitions

    def add_function(
            self,
            func: Callable,
            function_name: str,
            arg_definitions: List[ArgDefinition],
            max_concurrency: int = 0,
            description: str = '',
            timeout: int = 60 * 60 * 24):
        """Add Job to FunctionServer.

        Parameters
        ----------
        func
            A Python Function.
        arg_definitions
            Definitions of Arguments.
        function_name
            Function Name to identify function. It is not necessary to be same with func.__name__ .
        max_concurrency
            A Limitation for number of parallel execution.
            (the default is 0, which tells there is No Limitation.)
        description
            A Description for The Function. (the default is '', which [default_description])
        timeout
            Function timeout seconds for running. (the default is 86400, which means 1 day.)

        Raises
        ------
        ValueError
            For max_concurrency.

        """
        if function_name in self._function_definitions:
            self._logger.info(f'Duplicate Registration: {function_name}')

        self._function_definitions[function_name] = FunctionDefinition(
            func,
            arg_definitions,
            max_concurrency,
            description,
            function_name,
            timeout
        )

    def _job_decorator(
            self,
            func: Callable,
            task_id: str) -> Callable:

        @functools.wraps(func)
        def wrapper(job_args: Dict[str, Any]):
            try:
                logger = get_logger(task_id, logger_factory=multiprocessing.get_logger)
                task_store = task_store_factory(self._task_store_settings, False, logger=logger)
                ret = func(**job_args)
                task_store.finish_task(task_id, ret)
            except Exception as e:
                self._logger.debug(e)
                if task_store:
                    task_store.finish_task(task_id, e)

        return wrapper

    def launch_function(self, function_name: str, func_args: Dict[str, Any]) -> TryForkResult:
        func_def = self._function_definitions[function_name]

        task_id = str(uuid4())
        jobnized_func = self._job_decorator(func_def.func, task_id)

        if func_def.max_concurrency != 0 and self._task_store.get_current_count(function_name) >= func_def.max_concurrency:
            return TryForkResult(
                False,
                f'Over Max Concurrency {func_def.max_concurrency}',
                ''
            )

        self._task_store.initialize_task(task_id, func_def.function_name, func_def.timeout)
        self._process_manager.fork_process(jobnized_func, func_args, task_id)

        return TryForkResult(
            True,
            '',
            task_id
        )

    def get_task_info(self, task_id: str) -> Optional[TaskInfo]:
        return self._task_store.get_task_info(task_id)

    def get_current_number_of_execution(self, function_name: str) -> Optional[int]:
        if function_name not in self._function_definitions:
            return None
        return self._task_store.get_current_count(function_name)

    def list_task_info(self, function_name: str) -> List[TaskInfo]:
        return self._task_store.list_task_info(function_name)

    def terminate_task(self, task_id: str):
        self._process_manager.terminate_task(task_id)
        self._task_store.terminate_task(task_id)

    def terminate_function(self, function_name: str):
        self._process_manager.terminate_function(function_name)
        self._task_store.terminate_function(function_name)

    def join_processes(self):
        """Graceful shutdown."""
        self._process_manager.join_processes()

    def terminate_processes(self):
        """Terminate processes."""
        for name in self._function_definitions:
            self._task_store.terminate_function(name)
        self._process_manager.terminate_processes()

    def terminate_timeout_process_impl(self):
        for task in self._task_store.list_timeout_task_info():
            self._process_manager.terminate_task(task.task_id)
            self._task_store.terminate_timeout_task(task.task_id)

    async def terminate_timeout_processes_coro(self):
        """Terminate Timeout Processes. coroutine."""
        while True:
            self.terminate_timeout_process_impl()
            await asyncio.sleep(self._polling_timeout_process_interval)
