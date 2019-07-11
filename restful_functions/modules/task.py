"""'Task' is a status of one function process."""

import datetime
import json
import logging
import os
import sqlite3
from enum import Enum
from tempfile import gettempdir
from typing import Any, List, Optional

from ..utils.logger import get_logger


class TaskStoreSettings:
    def __init__(
            self,
            store_type: str = 'sqlite',
            sqlite_dsn: Optional[str] = None,
            expired: int = 60 * 60 * 24):
        """Settings for Intialize TaskStore.

        Parameters
        ----------
        store_type
            'sqlite'(only)
        sqlite_dsn
            SQLite Connection String. Can't use In-Memory Database.
        expired
            Expiration Time for storing a task result.
            The time is measured in seconds.

        """
        default_sqlite_dsn = os.path.join(gettempdir(), 'restful-functions.db')

        self.type = store_type
        self.sqlite_dsn = default_sqlite_dsn if sqlite_dsn is None else sqlite_dsn
        self.expired = expired


class TaskStatus(Enum):
    RUNNING = 'RUNNING'
    FAILED = 'FAILED'
    DONE = 'DONE'


class TaskInfo:
    __slots__ = ['task_id', 'function_name', 'status', 'result']

    def __init__(self,
                 task_id: str,
                 function_name: str,
                 status: TaskStatus,
                 result: Any):
        self.task_id = task_id
        self.function_name = function_name
        self.status = status
        self.result = result

    def is_running(self):
        return self.status == TaskStatus.RUNNING

    def is_done(self):
        return self.status != TaskStatus.RUNNING

    def is_success(self):
        return self.status == TaskStatus.DONE

    def is_failed(self):
        return self.status == TaskStatus.FAILED

    def to_dict(self):
        return {
            'task_id': self.task_id,
            'function_name': self.function_name,
            'status': self.status.name,
            'result': self.result,
            }


class TaskStore:
    """Abstract TaskStore Class.

    処理状態、結果格納
    """

    def initialize_task(self, task_id: str, function_name: str):
        raise NotImplementedError

    def finish_task(self, task_id: str, result: Any):
        raise NotImplementedError

    def get_task_info(self, task_id: str) -> Optional[TaskInfo]:
        raise NotImplementedError

    def get_current_count(self, function_name: str) -> int:
        raise NotImplementedError

    def list_task_info(self, function_name: str) -> List[TaskInfo]:
        raise NotImplementedError

    def terminate_task(self, task_id: str):
        self.finish_task(task_id, Exception('Manual Termination'))

    def terminate_function(self, function_name: str):
        tasks = self.list_task_info(function_name)
        for t in tasks:
            if t.is_running():
                self.finish_task(t.task_id, Exception('Manual Termination'))


class SQLiteTaskStore(TaskStore):
    """SQLite Implementation of TaskStore.

    Use SQLite to store tasks.
    """

    _INITIALIZE_TASK_SQL = 'INSERT INTO task(task_id, function_name, status, result, expired) values(?, ?, ?, ?, ?)'
    _FINISH_TASK_SQL = 'UPDATE task SET status = ?, result = ?, expired = ? WHERE task_id = ?'
    _GET_TASK_INFO_SQL = 'SELECT * FROM task WHERE task_id = ? LIMIT 1'
    _CURRENT_COUNT_SQL = f'SELECT count(*) FROM task WHERE function_name = ? AND status = "{TaskStatus.RUNNING.name}"'
    _LIST_TASK_INFO_SQL = 'SELECT * FROM task WHERE function_name = ?'

    _DELETE_OLD_STATUS_SQL = 'DELETE FROM task WHERE expired < ?'

    def __init__(
            self,
            dsn: str,
            expired: int,
            refresh_db: bool,
            logger: logging.Logger):

        self._dsn = dsn

        if refresh_db:
            conn = sqlite3.connect(dsn, 30.0)
            cur = conn.cursor()

            cur.execute('DROP TABLE IF EXISTS task')
            cur.execute('CREATE TABLE IF NOT EXISTS task (task_id TEXT PRIMARY KEY, function_name TEXT, status TEXT, result TEXT, expired INTEGER)')
            cur.execute('CREATE INDEX expired_idx ON task(expired)')
            conn.commit()

            conn.close()

        self._expired = expired

        self._logger = logger

    def _get_db(self):
        return sqlite3.connect(self._dsn, 30.0)

    def initialize_task(self, task_id: str, function_name: str):
        current_unix_time = int(datetime.datetime
                                .now(datetime.timezone.utc)
                                .timestamp())
        expired = current_unix_time + self._expired

        conn = self._get_db()
        cur = conn.cursor()
        cur.execute(SQLiteTaskStore._INITIALIZE_TASK_SQL, (
            task_id,
            function_name,
            TaskStatus.RUNNING.name,
            json.dumps({}),
            expired,
        ))
        conn.commit()
        conn.close()

    def finish_task(self, task_id: str, result: Any):
        if isinstance(result, Exception):
            status = TaskStatus.FAILED
            result_obj = str(result)
        else:
            status = TaskStatus.DONE
            result_obj = result

        current_unix_time = int(datetime.datetime
                                .now(datetime.timezone.utc)
                                .timestamp())
        expired = current_unix_time + self._expired

        try:
            store_data = json.dumps(result_obj)
        except Exception:
            self._logger.warn('result object is not json serializable')
            store_data = json.dumps({})

        conn = self._get_db()
        cur = conn.cursor()
        cur.execute(SQLiteTaskStore._FINISH_TASK_SQL, (
            status.name,
            store_data,
            expired,
            task_id,
        ))
        conn.commit()
        conn.close()

    def get_task_info(self, task_id: str) -> Optional[TaskInfo]:
        self._refresh_tasks()

        conn = self._get_db()
        cur = conn.cursor()
        ret = cur.execute(
            SQLiteTaskStore._GET_TASK_INFO_SQL,
            (task_id,)).fetchall()
        conn.close()

        if len(ret) == 0:
            return None

        return TaskInfo(
            task_id,
            ret[0][1],
            TaskStatus[ret[0][2]],
            json.loads(ret[0][3]),
        )

    def get_current_count(self, function_name: str) -> int:
        conn = self._get_db()
        cur = conn.cursor()
        ret = cur.execute(
            SQLiteTaskStore._CURRENT_COUNT_SQL,
            (function_name,)).fetchall()
        conn.close()
        return ret[0][0]

    def list_task_info(self, function_name: str) -> List[TaskInfo]:
        self._refresh_tasks()

        conn = self._get_db()
        cur = conn.cursor()
        ret = cur.execute(
            SQLiteTaskStore._LIST_TASK_INFO_SQL,
            (function_name,)).fetchall()
        conn.close()

        info_list = []
        for row in ret:
            info_list.append(TaskInfo(
                row[0],
                row[1],
                TaskStatus[row[2]],
                json.loads(row[3]),
            ))

        return info_list

    def _refresh_tasks(self):
        current_unix_time = int(datetime.datetime
                                .now(datetime.timezone.utc)
                                .timestamp())
        conn = self._get_db()
        cur = conn.cursor()
        cur.execute(
            SQLiteTaskStore._DELETE_OLD_STATUS_SQL,
            (current_unix_time,)
        )
        conn.commit()
        conn.close()


def task_store_factory(
        settings: TaskStoreSettings,
        clear_db: bool = False,
        *,
        logger: Optional[logging.Logger] = None) -> TaskStore:
    """A Factory of TaskStore.

    Generate an Instance of TaskStore Implementation by TaskStoreSettings.

    Parameters
    ----------
    settings
        Settings for TaskStore
    clear_db
        This is set True when called on the main thread.

    """
    if logger is None:
        logger = get_logger('TaskStore')

    if settings.type == 'sqlite':
        return SQLiteTaskStore(
            dsn=settings.sqlite_dsn,
            expired=settings.expired,
            refresh_db=clear_db,
            logger=logger
        )
    else:
        raise NotImplementedError
