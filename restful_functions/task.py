import datetime
import multiprocessing as mp
import pickle
import sqlite3
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

from .job import JobDefinition, JobState
from .logger import get_logger


class TaskStoreSettings:
    def __init__(
            self,
            store_type: str = 'sqlite',
            redis_host: str = 'localhost',
            redis_port: int = 6379,
            redis_db: int = 0,
            sqlite_dsn: str = 'restful-functions.db',
            expired: int = 60 * 60 * 24):
        """
        """
        self.type = store_type
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.sqlite_dsn = sqlite_dsn
        self.expired = expired


class TaskStore:
    def register_job(self, func_name: str):
        raise NotImplementedError

    def set_status(self, task_id: str, status: str, result: Any):
        raise NotImplementedError

    def get_status(self, task_id: str) -> Optional[Dict]:
        raise NotImplementedError

    def current_count(self, func_name: str) -> int:
        raise NotImplementedError

    def count_up_if_could(self, func_name: str, max_concurrency: int) -> bool:
        """
        Count Up if func can run
        """
        raise NotImplementedError

    def decrement_count(self, func_name: str):
        raise NotImplementedError


class RedisTaskStore(TaskStore):
    def __init__(
            self,
            host: str,
            port: int,
            db: int,
            expired: int):

        import redis
        self._r = redis.StrictRedis(host=host, port=port, db=db)
        self._expired = expired
        self._logger = get_logger(self.__class__.__name__)

    def register_job(self, func_name: str):
        self._r.set(f'count_{func_name}', 0)

    def set_status(self, task_id: str, status: str, result: Any):
        self._r.set(
            task_id,
            pickle.dumps({'status': status, 'result': result}),
            ex=self._expired)

    def get_status(self, task_id: str) -> Optional[Dict]:
        val = self._r.get(task_id)
        if val is None:
            return None
        return pickle.loads(val)

    def current_count(self, func_name: str):
        return int(self._r.get(f'count_{func_name}'))

    def count_up_if_could(self, func_name: str, max_concurrency: int) -> bool:
        """
        Count Up if func can run
        """
        from redis import WatchError

        with self._r.pipeline() as pipe:
            while True:
                try:
                    key = f'count_{func_name}'
                    pipe.watch(key)

                    count = int(pipe.get(key).decode())
                    if count >= max_concurrency:
                        return False

                    pipe.multi()
                    pipe.set(key, count+1)
                    pipe.execute()
                    return True

                except WatchError as e:
                    self._logger.debug(e)
                    continue

    def decrement_count(self, func_name: str):
        self._r.decr(f'count_{func_name}')


class SQLiteTaskStore(TaskStore):
    _REGISTER_SQL = 'REPLACE INTO job(key, count) values(?, 0)'
    _SET_STATUS_SQL = 'REPLACE INTO task(key, value, expired) values(?, ?, ?)'
    _GET_STATUS_SQL = 'SELECT value FROM task WHERE key = ? LIMIT 1'
    _CURRENT_COUNT_SQL = 'SELECT count FROM job WHERE key = ? LIMIT 1'
    _INCREMENT_COUNT_SQL = 'UPDATE job SET count=count+1 WHERE key = ?'
    _DECREMENT_COUNT_SQL = 'UPDATE job SET count=count-1 WHERE key = ?'

    _DELETE_OLD_STATUS_SQL = 'DELETE FROM task WHERE expired < ?'

    def __init__(
            self,
            dsn: str,
            expired: int,
            refresh_db: bool):

        self._dsn = dsn

        if refresh_db:
            conn = sqlite3.connect(dsn, 30.0)
            cur = conn.cursor()

            cur.execute('DROP TABLE IF EXISTS job')
            cur.execute('DROP TABLE IF EXISTS task')
            cur.execute('CREATE TABLE IF NOT EXISTS job (key TEXT PRIMARY KEY, count INTEGER)')  # NOQA
            cur.execute('CREATE TABLE IF NOT EXISTS task (key TEXT PRIMARY KEY, value BLOB, expired INTEGER)')  # NOQA
            cur.execute('CREATE INDEX expired_idx ON task(expired)')
            conn.commit()

            conn.close()

        self._expired = expired

        self._logger = get_logger(self.__class__.__name__)

    def _get_db(self):
        return sqlite3.connect(self._dsn, 30.0)

    def register_job(self, func_name: str):
        conn = self._get_db()
        cur = conn.cursor()
        cur.execute(SQLiteTaskStore._REGISTER_SQL, (func_name,))
        conn.commit()

    def set_status(self, task_id: str, status: str, result: Any):
        current_unix_time = int(datetime.datetime
                                .now(datetime.timezone.utc)
                                .timestamp())
        expired = current_unix_time + self._expired

        conn = self._get_db()
        cur = conn.cursor()
        cur.execute(
            SQLiteTaskStore._SET_STATUS_SQL,
            (
                task_id,
                pickle.dumps({'status': status, 'result': result}),
                expired,
            ))
        conn.commit()
        conn.close()

    def get_status(self, task_id: str) -> Optional[Dict]:
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

        ret = cur.execute(
            SQLiteTaskStore._GET_STATUS_SQL,
            (task_id,)).fetchall()
        conn.close()

        if len(ret) == 0:
            return None
        return pickle.loads(ret[0][0])

    def current_count(self, func_name: str):
        conn = self._get_db()
        cur = conn.cursor()
        ret = cur.execute(
            SQLiteTaskStore._CURRENT_COUNT_SQL,
            (func_name,)).fetchall()
        conn.close()
        return ret[0][0]

    def count_up_if_could(self, func_name: str, max_concurrency: int) -> bool:
        """
        Count Up if func can run
        """
        conn = self._get_db()
        cur = conn.cursor()

        try:
            if self.current_count(func_name) >= max_concurrency:
                return False

            cur.execute(
                SQLiteTaskStore._INCREMENT_COUNT_SQL,
                (func_name,))
            conn.commit()

            return True

        except Exception as e:
            print(e)
            conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def decrement_count(self, func_name: str):
        conn = self._get_db()
        cur = conn.cursor()
        cur.execute(
            SQLiteTaskStore._DECREMENT_COUNT_SQL,
            (func_name,))
        conn.commit()
        conn.close()


def task_store_factory(
        settings: TaskStoreSettings,
        refresh_db: bool = True) -> TaskStore:

    if settings.type == 'redis':
        return RedisTaskStore(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            expired=settings.expired
        )
    elif settings.type == 'sqlite':
        return SQLiteTaskStore(
            dsn=settings.sqlite_dsn,
            expired=settings.expired,
            refresh_db=refresh_db
        )
    else:
        raise NotImplementedError


class TryForkResult:
    def __init__(
            self,
            successed: bool,
            message: str,
            task_id: str):
        self.successed = successed
        self.message = message
        self.task_id = task_id

    def to_dict(self):
        return {
            'successed': self.successed,
            'message': self.message,
            'task_id': self.task_id,
        }


class TaskManager:
    def __init__(self, task_store_settings: TaskStoreSettings):
        self._job_definitions = {}  # type: Dict[str, JobDefinition]

        self._processes = []  # type: ignore

        self._task_store_settings = task_store_settings

        self._task_store = task_store_factory(
            self._task_store_settings, True)

        self._logger = get_logger(self.__class__.__name__)

    @property
    def job_list_text(self) -> str:
        rows = []
        for name in self._job_definitions:
            elm = self._job_definitions[name]
            rows.append(name)
            rows.append('  URL:')
            rows.append(f'    async api: /call/{elm.endpoint}')
            rows.append(f'    block api: /call/blocking/{elm.endpoint}')
            rows.append(f'  Max Concurrency: {elm.max_concurrency}')
            rows.append('  Description:')
            rows.append(f'    {elm.description}')
            if len(elm.arg_definitions) == 0:
                rows.append('  No Args')
            else:
                rows.append('  Args')
                for arg in elm.arg_definitions:
                    rows.append(f'    {arg.name} {arg.type.name} {"Requiered" if arg.is_required else ""}')  # NOQA
                    if arg.description != '':
                        rows.append(f'      {arg.description}')
            rows.append('\n')

        return '\n'.join(rows)

    @property
    def entrypoints(self) -> List[str]:
        return [elm.endpoint for elm in self._job_definitions.values()]

    def add_job(self, func_name: str, job: JobDefinition):
        self._job_definitions[func_name] = job
        self._task_store.register_job(func_name)

    def has_job(self, func_name: str) -> bool:
        return func_name in self._job_definitions

    def get_status(self, task_id: str) -> Optional[Dict]:
        return self._task_store.get_status(task_id)

    def get_current_concurrency(self, func_name: str) -> int:
        return self._task_store.current_count(func_name)

    def get_max_concurrency(self, func_name: str) -> int:
        return self._job_definitions[func_name].max_concurrency

    def update_max_concurrency(self, func_name: str, value: int) -> int:
        if value < 0:
            raise ValueError

        self._job_definitions[func_name].max_concurrency = value
        return value

    def _job_decorator(
            self,
            func_name: str,
            task_id: str) -> Callable:
        def wrapper(job_args: Dict[str, Any]):
            task_store = task_store_factory(self._task_store_settings, False)

            try:
                task_store.set_status(task_id, JobState.RUNNING, {})
                ret = self._job_definitions[func_name].func(**job_args)
                task_store.set_status(task_id, JobState.DONE, ret)
            except Exception as e:
                self._logger.debug(e)
                task_store.set_status(task_id, JobState.FAILED, str(e))
            finally:
                task_store.decrement_count(func_name)

        return wrapper

    def fork_process(
            self,
            func_name: str,
            func_args) -> TryForkResult:

        task_id = str(uuid4())
        jobnized_func = self._job_decorator(func_name, task_id)
        p = mp.Process(target=jobnized_func, args=(func_args,))

        count_up_is_ok = self._task_store.count_up_if_could(
            func_name,
            self.get_max_concurrency(func_name))

        if not count_up_is_ok:
            return TryForkResult(
                False,
                f'Over Max Concurrency {self.get_max_concurrency(func_name)}',
                ''
            )

        p.start()
        self._processes.append(p)

        remove_list = []
        for elm in self._processes:
            if not elm.is_alive():
                elm.join()
                remove_list.append(elm)
        for elm in remove_list:
            self._processes.remove(elm)

        return TryForkResult(
                True,
                '',
                task_id)

    def join_processes(self):
        self._logger.info('Join forked processes')

        for p in self._processes:
            p.join()
        self._processes = []

    def terminate_processes(self):
        self._logger.info('Terminate forked processes')

        for p in self._processes:
            p.terminate()
        self._processes = []
