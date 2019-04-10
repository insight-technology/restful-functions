from time import sleep
from typing import Any, Dict, Optional, Type

import pytest

from restful_functions.modules.task import (SQLiteTaskStore, TaskStoreSettings,
                                            task_store_factory)


@pytest.mark.parametrize('settings_base,expected', [
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
        store_type='sqlite',
    ),
])
def test_task_store_count_operation(settings: TaskStoreSettings):
    FUNC_NAME = 'job1'

    store = task_store_factory(settings, True)

    assert store.get_current_count(FUNC_NAME) == 0
    assert store.get_task_info('fake_id_1') is None

    store.initialize_task('fake_id_1', FUNC_NAME)
    assert store.get_current_count(FUNC_NAME) == 1
    assert store.get_task_info('fake_id_1').is_running()

    store.finish_task('fake_id_1', 'ret')
    assert store.get_current_count(FUNC_NAME) == 0
    assert store.get_task_info('fake_id_1').is_done()
    assert store.get_task_info('fake_id_1').is_success()

    store.initialize_task('fake_id_2', FUNC_NAME)
    store.initialize_task('fake_id_3', FUNC_NAME)
    assert store.get_current_count(FUNC_NAME) == 2

    store.finish_task('fake_id_3', 'ret')
    assert store.get_current_count(FUNC_NAME) == 1

    store.finish_task('fake_id_2', 'ret')
    assert store.get_current_count(FUNC_NAME) == 0


@pytest.mark.parametrize('settings', [
    TaskStoreSettings(
        store_type='sqlite',
        expired=1,
    ),
    TaskStoreSettings(
        store_type='sqlite',
        expired=100000,
    ),
])
def test_task_store_task_result_expiration_operation(settings: TaskStoreSettings):
    FUNC_NAME = 'fake_name'
    TASK_ID = 'fake_id'
    TASK_RESULT = 'artifact'
    SLEEP_TIME = 2

    store = task_store_factory(settings, True)

    store.initialize_task(TASK_ID, FUNC_NAME)
    store.finish_task(TASK_ID, TASK_RESULT)
    sleep(SLEEP_TIME)

    ret = store.get_task_info(TASK_ID)

    if settings.expired < SLEEP_TIME:
        assert ret is None
    else:
        assert ret is not None
        assert ret.result == TASK_RESULT


@pytest.mark.parametrize('settings', [
    TaskStoreSettings(
        store_type='sqlite',
    ),
])
def test_task_store_termination_task_operation(settings: TaskStoreSettings):
    store = task_store_factory(settings, True)

    assert store.get_current_count('func1') == 0

    assert store.get_task_info('task_id_1') is None

    store.initialize_task('task_id_1', 'func1')

    assert store.get_task_info('task_id_1').is_running()

    assert store.get_current_count('func1') == 1

    store.terminate_task('task_id_1')

    assert store.get_current_count('func1') == 0

    assert store.get_task_info('task_id_1').is_failed()

    store.initialize_task('task_id_2', 'func1')
    store.initialize_task('task_id_3', 'func1')

    assert store.get_current_count('func1') == 2

    store.terminate_task('task_id_2')
    store.terminate_task('task_id_3')

    assert store.get_current_count('func1') == 0
    assert store.get_task_info('task_id_2').is_failed()
    assert store.get_task_info('task_id_3').is_failed()


@pytest.mark.parametrize('settings', [
    TaskStoreSettings(
        store_type='sqlite',
    ),
])
def test_task_store_termination_functions_operation(settings: TaskStoreSettings):
    store = task_store_factory(settings, True)

    assert store.get_current_count('func1') == 0
    store.terminate_function('func1')
    assert store.get_current_count('func1') == 0

    store.initialize_task('task_id_1', 'func1')
    store.initialize_task('task_id_2', 'func1')
    store.initialize_task('task_id_3', 'func1')

    store.initialize_task('task_id_4', 'func2')
    store.initialize_task('task_id_5', 'func2')

    assert store.get_current_count('func1') == 3
    assert store.get_current_count('func2') == 2

    store.terminate_function('func1')

    assert store.get_current_count('func1') == 0
    assert store.get_current_count('func2') == 2

    assert store.get_task_info('task_id_1').is_failed()
    assert store.get_task_info('task_id_2').is_failed()
    assert store.get_task_info('task_id_3').is_failed()
    assert store.get_task_info('task_id_4').is_running()
    assert store.get_task_info('task_id_5').is_running()
