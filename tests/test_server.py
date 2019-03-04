import json
from multiprocessing import Process
from time import sleep

import requests
from restful_functions import FunctionServer
from restful_functions.job import JobState


def test_api_list():
    server = FunctionServer('terminate')

    server._construct_endpoints()

    req, res = server._app.test_client.get('/api/list/data')
    assert res.status == 200
    assert res.json == []

    req, res = server._app.test_client.get('/api/list/text')
    assert res.status == 200
    assert res.text == ''


def test_api_list_2():
    def test1():
        pass

    def test2():
        pass

    server = FunctionServer('terminate')
    server.add_job(test1, [], 1)
    server.add_job(test2, [], 1)

    server._construct_endpoints()

    req, res = server._app.test_client.get('/api/list/data')
    assert res.status == 200
    assert res.json == ['test1', 'test2']

    req, res = server._app.test_client.get('/api/list/text')
    assert res.status == 200
    assert res.text != ''


def test_api_max_concurrency():
    def test1():
        pass

    def test2():
        pass

    server = FunctionServer('terminate')
    server.add_job(test1, [], 1)
    server.add_job(test2, [], 2)

    server._construct_endpoints()

    req, res = server._app.test_client.get(
        '/api/max_concurrency/test1')
    assert res.status == 200
    assert res.json == 1

    req, res = server._app.test_client.get(
        '/api/max_concurrency/test2')
    assert res.status == 200
    assert res.json == 2

    req, res = server._app.test_client.post(
        '/api/max_concurrency',
        data=json.dumps({'func_name': 'test1', 'value': 11}))
    res.status == 200

    req, res = server._app.test_client.post(
        '/api/max_concurrency',
        data=json.dumps({'func_name': 'test2', 'value': 22}))
    res.status == 200

    req, res = server._app.test_client.get(
        '/api/max_concurrency/test1')
    assert res.status == 200
    assert res.json == 11

    req, res = server._app.test_client.get(
        '/api/max_concurrency/test2')
    assert res.status == 200
    assert res.json == 22

    req, res = server._app.test_client.get('/api/current_concurrency/test1')
    assert res.status == 200
    assert res.json == 0

    req, res = server._app.test_client.get('/api/current_concurrency/test2')
    assert res.status == 200
    assert res.json == 0


def long_func():
    sleep(1000)


def server_process_for_test_current_concurrent():
    server = FunctionServer('terminate')
    server.add_job(long_func, [], 10)

    server.start()
    # TODO: Terminate Processes after Receiving SIG_TERM.


def test_current_concurrent():
    p = Process(target=server_process_for_test_current_concurrent)
    p.start()

    while True:
        try:
            requests.get('http://localhost:8888/api/list/data')
        except Exception:
            sleep(1)
            continue
        break

    res = requests.get(
        'http://localhost:8888/api/current_concurrency/long_func')
    assert res.status_code == 200
    assert res.json() == 0

    for i in range(10):
        res = requests.post('http://localhost:8888/call/long_func')
        assert res.status_code == 200
        assert res.json()['successed']

        res = requests.get(
            'http://localhost:8888/api/current_concurrency/long_func')
        assert res.status_code == 200
        assert res.json() == i+1

    res = requests.post('http://localhost:8888/call/long_func')
    assert res.status_code == 200
    assert not res.json()['successed']

    res = requests.get(
        'http://localhost:8888/api/current_concurrency/long_func')
    assert res.status_code == 200
    assert res.json() == 10

    p.terminate()


def status_check_func():
    sleep(2)
    return 42


def status_check_func_fail():
    sleep(2)
    raise Exception('Test Fail')


def server_process_for_test_task_status():
    server = FunctionServer('terminate', port=8889)
    server.add_job(status_check_func, [], 1)
    server.add_job(status_check_func_fail, [], 1)

    server.start()


def test_task_status():
    p = Process(target=server_process_for_test_task_status)
    p.start()

    while True:
        try:
            requests.get('http://localhost:8889/api/list/data')
        except Exception:
            sleep(1)
            continue
        break

    # Normal
    res = requests.post('http://localhost:8889/call/status_check_func')
    assert res.json()['successed']

    task_id = res.json()['task_id']

    while True:
        res = requests.get(f'http://localhost:8889/task/status/{task_id}')
        if res.json()['status'] != JobState.RUNNING:
            break
        sleep(1)
    assert res.json()['status'] == JobState.DONE
    assert res.json()['result'] == 42

    # Abnormal
    res = requests.post('http://localhost:8889/call/status_check_func_fail')
    assert res.json()['successed']

    task_id = res.json()['task_id']

    while True:
        res = requests.get(f'http://localhost:8889/task/status/{task_id}')
        if res.json()['status'] != JobState.RUNNING:
            break
        sleep(1)
    assert res.json()['status'] == JobState.FAILED
    assert res.json()['result'] == 'Test Fail'

    p.terminate()


def test_task_status_simple_api():
    p = Process(target=server_process_for_test_task_status)
    p.start()

    while True:
        try:
            requests.get('http://localhost:8889/api/list/data')
        except Exception:
            sleep(1)
            continue
        break

    # Normal
    res = requests.post('http://localhost:8889/call/status_check_func')
    assert res.json()['successed']

    task_id = res.json()['task_id']

    while True:
        res = requests.get(f'http://localhost:8889/task/done/{task_id}')
        if res.json():
            break
        sleep(1)

    res = requests.get(f'http://localhost:8889/task/result/{task_id}')
    assert res.json() == 42

    # Abnormal
    res = requests.post('http://localhost:8889/call/status_check_func_fail')
    assert res.json()['successed']

    task_id = res.json()['task_id']

    while True:
        res = requests.get(f'http://localhost:8889/task/done/{task_id}')
        if res.json():
            break
        sleep(1)

    res = requests.get(f'http://localhost:8889/task/result/{task_id}')
    assert res.json() == 'Test Fail'

    p.terminate()
