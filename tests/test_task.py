from time import sleep
from typing import Any, Dict, Optional, Type

import pytest

from restful_functions.job import (ArgDefinition, ArgType, JobDefinition,
                                   JobState)
from restful_functions.task import (RedisTaskStore, SQLiteTaskStore,
                                    TaskManager, TaskStoreSettings,
                                    task_store_factory)

from . import TEST_CONFIG


@pytest.mark.parametrize('settings_base,expected', [
    (
        {
            'store_type': 'redis',
        },
        RedisTaskStore,
    ),
    (
        {
            'store_type': 'sqlite',
        },
        SQLiteTaskStore,
    ),
    (
        {
            'store_type': 'virtualdb'
        },
        None
    ),
])
def test_task_store_factory(
        settings_base: Dict[str, Any],
        expected: Optional[Type]):
    settings = TaskStoreSettings(**settings_base)

    if expected is None:
        with pytest.raises(NotImplementedError):
            task_store_factory(settings)
    else:
        ret = task_store_factory(settings)
        assert isinstance(ret, expected)


@pytest.mark.parametrize('settings', [
    TaskStoreSettings(
        store_type='redis',
        redis_host=TEST_CONFIG.REDIS_HOST,
        redis_port=TEST_CONFIG.REDIS_PORT,
        redis_db=TEST_CONFIG.REDIS_DB,
    ),
    TaskStoreSettings(
        store_type='sqlite',
    ),
])
def test_task_store_job_operation(
        settings: TaskStoreSettings):
    JOB_NAME = 'job1'

    store = task_store_factory(settings, True)

    store.register_job(JOB_NAME)

    assert store.current_count(JOB_NAME) == 0

    ret = store.count_up_if_could(JOB_NAME, 1)
    assert ret
    assert store.current_count(JOB_NAME) == 1

    ret = store.count_up_if_could(JOB_NAME, 1)
    assert not ret
    assert store.current_count(JOB_NAME) == 1

    store.decrement_count(JOB_NAME)
    assert store.current_count(JOB_NAME) == 0

    ret = store.count_up_if_could(JOB_NAME, 1)
    assert ret
    assert store.current_count(JOB_NAME) == 1

    ret = store.count_up_if_could(JOB_NAME, 2)
    assert ret
    assert store.current_count(JOB_NAME) == 2

    ret = store.count_up_if_could(JOB_NAME, 2)
    assert not ret
    assert store.current_count(JOB_NAME) == 2


@pytest.mark.parametrize('settings', [
    TaskStoreSettings(
        store_type='redis',
        redis_host=TEST_CONFIG.REDIS_HOST,
        redis_port=TEST_CONFIG.REDIS_PORT,
        redis_db=TEST_CONFIG.REDIS_DB,
        expired=1,
    ),
    TaskStoreSettings(
        store_type='redis',
        redis_host=TEST_CONFIG.REDIS_HOST,
        redis_port=TEST_CONFIG.REDIS_PORT,
        redis_db=TEST_CONFIG.REDIS_DB,
        expired=100000,
    ),
    TaskStoreSettings(
        store_type='sqlite',
        expired=1,
    ),
    TaskStoreSettings(
        store_type='sqlite',
        expired=100000,
    ),
])
def test_task_store_task_status_operation(
        settings: TaskStoreSettings):
    TASK_ID = 'fake_id'
    TASK_RESULT = 'artifact'
    SLEEP_TIME = 2

    store = task_store_factory(settings, True)

    store.set_status(TASK_ID, JobState.DONE, TASK_RESULT)
    sleep(SLEEP_TIME)

    ret = store.get_status(TASK_ID)

    if settings.expired < SLEEP_TIME:
        assert ret is None
    else:
        assert ret['result'] == TASK_RESULT


@pytest.mark.parametrize('settings', [
    TaskStoreSettings(
        store_type='redis',
        redis_host=TEST_CONFIG.REDIS_HOST,
        redis_port=TEST_CONFIG.REDIS_PORT,
        redis_db=TEST_CONFIG.REDIS_DB,
    ),
    TaskStoreSettings(
        store_type='sqlite',
    ),
])
def test_task_manager(settings: TaskStoreSettings):
    manager = TaskManager(settings)

    def TEST_FUNC(x: int, y: int):
        return x + y

    assert not manager.has_job(TEST_FUNC.__name__)

    manager.add_job(
        JobDefinition(
            TEST_FUNC,
            1,
            [
                ArgDefinition(ArgType.INTEGER, True, '1st'),
                ArgDefinition(ArgType.INTEGER, True, '2nd'),
            ],
            TEST_FUNC.__name__,
            'test'))

    assert manager.has_job(TEST_FUNC.__name__)
    assert not manager.has_job('FAKE_FUNC')

    assert manager.get_max_concurrency(TEST_FUNC.__name__) == 1
    assert manager.get_current_concurrency(TEST_FUNC.__name__) == 0

    manager.update_max_concurrency(TEST_FUNC.__name__, 100)
    assert manager.get_max_concurrency(TEST_FUNC.__name__) == 100

    with pytest.raises(ValueError):
        manager.update_max_concurrency('FACE_FUNC', 1)
        manager.update_max_concurrency(TEST_FUNC.__name__, -1)


@pytest.mark.parametrize('settings', [
    TaskStoreSettings(
        store_type='redis',
        redis_host=TEST_CONFIG.REDIS_HOST,
        redis_port=TEST_CONFIG.REDIS_PORT,
        redis_db=TEST_CONFIG.REDIS_DB,
    ),
    TaskStoreSettings(
        store_type='sqlite',
    ),
])
def test_task_manager_fork_process(settings: TaskStoreSettings):
    manager = TaskManager(settings)

    def FAST_FUNC(x: int, y: int):
        return x + y

    manager.add_job(
        JobDefinition(
            FAST_FUNC,
            1,
            [
                ArgDefinition('x', ArgType.INTEGER, True, '1st'),
                ArgDefinition('x', ArgType.INTEGER, True, '2nd'),
            ],
            FAST_FUNC.__name__,
            'fast'
        )
    )

    # Few Args
    ret = manager.fork_process(FAST_FUNC.__name__, {})
    assert ret.successed

    status = manager.get_status(ret.task_id)
    while status is None or status['status'] == JobState.RUNNING:
        status = manager.get_status(ret.task_id)
    assert status['status'] == JobState.FAILED

    # Valid Args
    ret = manager.fork_process(FAST_FUNC.__name__, {'x': 3, 'y': 4})
    assert ret.successed

    status = manager.get_status(ret.task_id)
    while status is None or status['status'] == JobState.RUNNING:
        status = manager.get_status(ret.task_id)
    assert status['status'] == JobState.DONE
    assert status['result'] == 7


@pytest.mark.parametrize('settings', [
    TaskStoreSettings(
        store_type='redis',
        redis_host=TEST_CONFIG.REDIS_HOST,
        redis_port=TEST_CONFIG.REDIS_PORT,
        redis_db=TEST_CONFIG.REDIS_DB,
    ),
    TaskStoreSettings(
        store_type='sqlite',
    ),
])
def test_task_manager_fork_process_slow_func(settings: TaskStoreSettings):
    manager = TaskManager(settings)

    def SLOW_FUNC(x: int, y: int, wait: int):
        if wait < 1:
            raise ValueError

        sleep(wait)
        return x + y

    manager.add_job(
        JobDefinition(
            SLOW_FUNC,
            2,
            [
                ArgDefinition('x', ArgType.INTEGER, True, '1st'),
                ArgDefinition('y', ArgType.INTEGER, True, '2nd'),
                ArgDefinition('wait', ArgType.INTEGER, True, 'wait sec')
            ],
            SLOW_FUNC.__name__,
            'slow'
        )
    )

    # Invalid Arg
    ret = manager.fork_process(
        SLOW_FUNC.__name__,
        {'x': 3, 'y': 4, 'wait': -1})

    status = manager.get_status(ret.task_id)
    while status is None or status['status'] == JobState.RUNNING:
        status = manager.get_status(ret.task_id)

    assert status['status'] == JobState.FAILED

    # concurrency
    # First
    ret = manager.fork_process(
        SLOW_FUNC.__name__,
        {'x': 3, 'y': 4, 'wait': 2})

    assert ret.successed

    # Second
    ret = manager.fork_process(
        SLOW_FUNC.__name__,
        {'x': 3, 'y': 4, 'wait': 2})
    assert ret.successed

    # Third (Fail)
    ret = manager.fork_process(
        SLOW_FUNC.__name__,
        {'x': 3, 'y': 4, 'wait': 2})

    assert not ret.successed

    manager.terminate_processes()
