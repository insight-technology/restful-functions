import json
import os
from multiprocessing import Process
from time import sleep
from tempfile import gettempdir

import requests

from restful_functions import (ArgDefinition, ArgType, FunctionServer,
                               TaskStoreSettings)
from restful_functions.modules.task import TaskStatus


def post_request(url: str, json_str: str = '{}', port: int = 8888) -> requests.Response:
    return requests.post(f'http://localhost:{port}{url}', json_str, headers={'Content-Type': 'application/json'})


def get_request(url: str, port: int = 8888) -> requests.Response:
    return requests.get(f'http://localhost:{port}{url}')


def sleep_func(t: int):
    sleep(t)


def sleep_function_server_process():
    tss = TaskStoreSettings()
    tss.sqlite_dsn = os.path.join(gettempdir(), 'sleep_function_server_process.db')

    server = FunctionServer(shutdown_mode='terminate', register_sys_signals=True, task_store_settings=tss)
    server.add_function(sleep_func, [ArgDefinition('t', ArgType.INTEGER, True, 'Sleep Time')], 10)

    def terminate(sig, handler):
        try:
            # FIXME: don't stop immediately
            server._app.stop()
            server.exit_with_terminate()
        except Exception:
            pass

    server.start()


def test_launch_function():
    p = Process(target=sleep_function_server_process)
    p.start()

    try:
        while True:
            try:
                get_request('/api/list/data')
            except Exception:
                sleep(1)
                continue
            break

        res = get_request('/function/running-count/sleep_func')
        assert res.status_code == 200
        assert res.json() == 0

        for i in range(10):
            res = post_request('/sleep_func', json.dumps({'t': 5}))
            assert res.status_code == 200
            assert res.json()['success']

            res = get_request(f'/task/info/{res.json()["task_id"]}')
            assert res.status_code == 200
            assert res.json()['status'] == 'RUNNING'

            res = get_request('/function/running-count/sleep_func')
            assert res.status_code == 200
            assert res.json() == i+1

        res = post_request('/sleep_func', json.dumps({'t': 5}))
        assert res.status_code == 200
        assert not res.json()['success']

        res = get_request('/function/running-count/sleep_func')
        assert res.status_code == 200
        assert res.json() == 10

    finally:
        p.terminate()


def status_check_func():
    sleep(2)
    return 42


def status_check_func_fail():
    sleep(2)
    raise Exception('Test Fail')


def server_process_for_test_task_status():
    tss = TaskStoreSettings()
    tss.sqlite_dsn = os.path.join(gettempdir(), 'server_process_for_test_task_status.db')

    server = FunctionServer(shutdown_mode='terminate', port=8889, task_store_settings=tss)
    server.add_function(status_check_func, [], 1)
    server.add_function(status_check_func_fail, [], 1)

    def terminate(sig, handler):
        try:
            # FIXME: don't stop immediately
            server._app.stop()
            server.exit_with_terminate()
        except Exception:
            pass

    server.start()


def test_task_status():
    p = Process(target=server_process_for_test_task_status)
    p.start()

    while True:
        try:
            get_request('/api/list/data', port=8889)
        except Exception:
            sleep(1)
            continue
        break

    # Normal
    res = post_request('/status_check_func', port=8889)
    assert res.json()['success']

    task_id = res.json()['task_id']

    while True:
        res = get_request(f'/task/info/{task_id}', port=8889)
        if res.json()['status'] != TaskStatus.RUNNING.name:
            break
        sleep(1)
    assert res.json()['status'] == TaskStatus.DONE.name
    assert res.json()['result'] == 42

    # Abnormal
    res = post_request('/status_check_func_fail', port=8889)
    assert res.json()['success']

    task_id = res.json()['task_id']

    while True:
        res = get_request(f'/task/info/{task_id}', port=8889)
        if res.json()['status'] != TaskStatus.RUNNING.name:
            break
        sleep(1)
    assert res.json()['status'] == TaskStatus.FAILED.name
    assert res.json()['result'] == 'Test Fail'

    p.terminate()


def test_task_status_simple_api():
    try:
        p = Process(target=server_process_for_test_task_status)
        p.start()

        while True:
            try:
                get_request('/api/list/data', port=8889)
            except Exception:
                sleep(1)
                continue
            break

        # Normal
        res = post_request('/status_check_func', port=8889)
        assert res.json()['success']

        task_id = res.json()['task_id']

        while True:
            res = get_request(f'/task/done/{task_id}', port=8889)
            if res.json():
                break
            sleep(1)

        res = get_request(f'/task/result/{task_id}', port=8889)
        assert res.json() == 42

        # Abnormal
        res = post_request('/status_check_func_fail', port=8889)
        assert res.json()['success']

        task_id = res.json()['task_id']

        while True:
            res = get_request(f'/task/done/{task_id}', port=8889)
            if res.json():
                break
            sleep(1)

        res = get_request(f'/task/result/{task_id}', port=8889)
        assert res.json() == 'Test Fail'
    finally:
        p.terminate()
