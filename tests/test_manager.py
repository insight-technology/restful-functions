from time import sleep

import pytest

from restful_functions.manager import FunctionManager
from restful_functions.modules.function import ArgDefinition, ArgType
from restful_functions.modules.task import TaskInfo, TaskStatus, TaskStoreSettings


def get_task_info_with_waiting_done(manager: FunctionManager, task_id: str) -> TaskInfo:
    task_info = manager.get_task_info(task_id)
    while task_info is None or task_info.is_running():
        task_info = manager.get_task_info(task_id)
    return task_info


@pytest.mark.parametrize('settings', [
    TaskStoreSettings(
        store_type='sqlite',
    ),
])
def test_task_manager(settings: TaskStoreSettings):
    manager = FunctionManager(settings)

    def TEST_FUNC(x: int, y: int):
        return x + y

    assert TEST_FUNC.__name__ not in manager.definitions

    manager.add_function(
        TEST_FUNC,
        TEST_FUNC.__name__,
        [
            ArgDefinition('a', ArgType.INTEGER, True, '1st'),
            ArgDefinition('b', ArgType.INTEGER, True, '2nd'),
        ],
        1,
        'test'
    )

    assert TEST_FUNC.__name__ in manager.definitions
    assert 'FAKE_FUNC' not in manager.definitions

    assert manager.get_current_number_of_execution(TEST_FUNC.__name__) == 0


@pytest.mark.parametrize('settings', [
    TaskStoreSettings(
        store_type='sqlite',
    ),
])
def test_task_manager_launch_function(settings: TaskStoreSettings):
    manager = FunctionManager(settings)

    def FAST_FUNC(x: int, y: int):
        return x + y

    manager.add_function(
        FAST_FUNC,
        FAST_FUNC.__name__,
        [
            ArgDefinition('x', ArgType.INTEGER, True, '1st'),
            ArgDefinition('x', ArgType.INTEGER, True, '2nd'),
        ],
        1,
        'fast'
    )

    # Few Args
    ret = manager.launch_function(FAST_FUNC.__name__, {})
    assert ret.success

    task_info = get_task_info_with_waiting_done(manager, ret.task_id)
    assert task_info.is_failed()

    # Valid Args
    ret = manager.launch_function(FAST_FUNC.__name__, {'x': 3, 'y': 4})
    assert ret.success

    task_info = get_task_info_with_waiting_done(manager, ret.task_id)
    assert task_info.is_done()
    assert task_info.result == 7


@pytest.mark.parametrize('settings', [
    TaskStoreSettings(
        store_type='sqlite',
    ),
])
def test_task_manager_launch_function_slow_func(settings: TaskStoreSettings):
    manager = FunctionManager(settings)

    def SLOW_FUNC(x: int, y: int, wait: int):
        if wait < 1:
            raise ValueError

        sleep(wait)
        return x + y

    manager.add_function(
        SLOW_FUNC,
        SLOW_FUNC.__name__,
        [
            ArgDefinition('x', ArgType.INTEGER, True, '1st'),
            ArgDefinition('y', ArgType.INTEGER, True, '2nd'),
            ArgDefinition('wait', ArgType.INTEGER, True, 'wait sec')
        ],
        2,
        'slow'
    )

    # Invalid Arg
    ret = manager.launch_function(
        SLOW_FUNC.__name__,
        {'x': 3, 'y': 4, 'wait': -1})

    task_info = get_task_info_with_waiting_done(manager, ret.task_id)
    assert task_info.is_failed()

    # concurrency
    # First
    ret = manager.launch_function(
        SLOW_FUNC.__name__,
        {'x': 3, 'y': 4, 'wait': 2})

    assert ret.success

    # Second
    ret = manager.launch_function(
        SLOW_FUNC.__name__,
        {'x': 3, 'y': 4, 'wait': 2})
    assert ret.success

    # Third (Fail)
    ret = manager.launch_function(
        SLOW_FUNC.__name__,
        {'x': 3, 'y': 4, 'wait': 2})

    assert not ret.success

    manager.terminate_processes()


@pytest.mark.parametrize('settings', [
    TaskStoreSettings(
        store_type='sqlite',
    ),
])
def test_task_manager_timeout(settings: TaskStoreSettings):
    manager = FunctionManager(settings)

    def SLOW_FUNC(x: int, y: int, wait: int):
        if wait < 1:
            raise ValueError

        sleep(wait)
        return x + y

    manager.add_function(
        SLOW_FUNC,
        SLOW_FUNC.__name__,
        [
            ArgDefinition('x', ArgType.INTEGER, True, '1st'),
            ArgDefinition('y', ArgType.INTEGER, True, '2nd'),
            ArgDefinition('wait', ArgType.INTEGER, True, 'wait sec')
        ],
        10,
        'slow',
        3  # timeout
    )

    # short waiting time
    ret = manager.launch_function(
        SLOW_FUNC.__name__,
        {'x': 3, 'y': 4, 'wait': 1})
    assert ret.success

    sleep(2)

    manager.terminate_timeout_process_impl()  # manual calling

    task_info = get_task_info_with_waiting_done(manager, ret.task_id)
    assert task_info.is_success()
    assert task_info.result == 7

    # long waiting time (timeout)
    ret = manager.launch_function(
        SLOW_FUNC.__name__,
        {'x': 3, 'y': 4, 'wait': 10.0})
    assert ret.success

    sleep(4)

    manager.terminate_timeout_process_impl()  # manual calling

    task_info = task_info = get_task_info_with_waiting_done(manager, ret.task_id)
    assert task_info.is_failed()
    assert task_info.status == TaskStatus.TIMEOUT
    assert task_info.result == 'timeout'

    manager.terminate_processes()
