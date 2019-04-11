import logging
import multiprocessing as mp
from typing import Any, Callable, Dict, List, Optional


class ProcessInfo:
    __slots__ = ['task_id', 'function_name', 'process']

    def __init__(self, task_id: str, function_name: str, process: mp.Process):
        self.task_id = task_id
        self.function_name = function_name
        self.process = process


class ProcessManager:
    """プロセス管理."""

    def __init__(self, *, logger: Optional[logging.Logger] = None):
        self._process_info_list: Dict[str, ProcessInfo] = {}  # task_id, ProcessInfo

        if logger is None:
            self._logger = logging.getLogger('ProcessManager')
        else:
            self._logger = logger

    def fork_process(
            self,
            jobnized_func: Callable,
            func_args: Dict[str, Any],
            task_id: str):
        p = mp.Process(target=jobnized_func, args=(func_args,))

        p.start()
        self._process_info_list[task_id] = ProcessInfo(task_id, jobnized_func.__name__, p)

        # Clean teminated processes
        remove_task_id_list: List[str] = []
        for info in self._process_info_list.values():
            if not info.process.is_alive():
                info.process.join()
                remove_task_id_list.append(info.task_id)

        for tid in remove_task_id_list:
            del self._process_info_list[tid]

    def terminate_task(self, task_id: str) -> bool:
        """タスクIDに紐づくプロセスを終了."""
        if task_id not in self._process_info_list:
            return False

        self._process_info_list[task_id].process.terminate()
        del self._process_info_list[task_id]

        return True

    def terminate_function(self, function_name: str) -> bool:
        remove_task_id_list = [elm.task_id for elm in self._process_info_list.values() if elm.function_name == function_name]
        for tid in remove_task_id_list:
            self._process_info_list[tid].process.terminate()
            self._process_info_list[tid].process.join()
            del self._process_info_list[tid]

        return True

    def join_processes(self):
        self._logger.info('Join forked processes')

        for info in self._process_info_list.values():
            info.process.join()
        self._process_info_list = {}

    def terminate_processes(self):
        self._logger.info('Terminate forked processes')

        for info in self._process_info_list.values():
            info.process.terminate()
            info.process.join()
        self._process_info_list = {}
