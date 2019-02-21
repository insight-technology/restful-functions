import os
import signal
import sys
from asyncio import get_event_loop, sleep
from typing import Any, Callable, Dict, List, Optional

from sanic import Sanic, request, response

from .job import ArgDefinition, JobDefinition, JobState, validate_arg
from .logger import get_logger
from .task import TaskManager, TaskStoreSettings


class FunctionServer:
    def __init__(
            self,
            shutdown_mode: str,
            port: int = 8888,
            timeout: int = 60*60*24,
            polling_interval: float = 1.0,
            debug: bool = False,
            register_sys_signals: bool = True,
            task_store_settings: TaskStoreSettings = TaskStoreSettings()):

        if shutdown_mode not in ['terminate', 'join']:
            raise ValueError('"shutdown_mode" should be "terminate" or "join"')

        if timeout <= 0:
            raise ValueError('"timeout" should be greater than 0')

        if polling_interval <= 0.0:
            raise ValueError('"polling_interval" should be greater than 0.0')

        self._app = Sanic()

        self._port = port

        self._app.config.REQUEST_TIMEOUT = timeout
        self._app.config.RESPONSE_TIMEOUT = timeout

        self._polling_interval = polling_interval

        self._task_manager = TaskManager(task_store_settings)

        self._debug = debug

        self._shutdown_mode = shutdown_mode

        self._main_process_id = os.getpid()

        self._register_sys_signals = register_sys_signals

        self._logger = get_logger(self.__class__.__name__, debug)

    def start(self):
        @self._app.route('/api/list/data')
        async def get_api_list_data(request: request.Request):
            return response.json(self._task_manager.entrypoints)

        @self._app.route('/api/list/text')
        async def get_api_lisT_text(request: request.Request):
            return response.text(self._task_manager.job_list_text)

        @self._app.post('/api/max_concurrency')
        async def post_api_max_concurrency(request: request.Request):
            data = request.json

            if data is None:
                return response.json(
                        {'error': f'Parameter Missing'},
                        400
                    )

            func_name = data['func_name']
            value = data['value']

            self._task_manager.update_max_concurrency(func_name, value)

            return response.json({})

        @self._app.route('/api/max_concurrency/<func_name>')
        async def get_api_max_concurrency(request: request.Request,
                                          func_name: str):
            return response.json(
                self._task_manager.get_max_concurrency(func_name))

        @self._app.route('/api/current_concurrency/<func_name>')
        async def get_api_current_concurrency(request: request.Request,
                                              func_name: str):
            return response.json(
                self._task_manager.get_current_concurrency(func_name))

        @self._app.route('/task/status/<task_id>')
        async def get_task_status(request: request.Request, task_id: str):
            return response.json(
                self._task_manager.get_status(task_id))

        if self._register_sys_signals:
            def sig_handler(signum, frame):
                if self._shutdown_mode == 'terminate':
                    self.exit_with_terminate()

                elif self._shutdown_mode == 'join':
                    if self._main_process_id == os.getpid():
                        print('Joining Processes now.')
                        print('Press Ctrl+C again to force exit.')
                    signal.signal(signal.SIGINT, lambda _, __: self.exit_with_terminate())  # NOQA
                    signal.signal(signal.SIGTERM, lambda _, __: self.exit_with_terminate())  # NOQA
                    self.exit_with_join()

                else:
                    raise ValueError

            signal.signal(signal.SIGINT, sig_handler)
            signal.signal(signal.SIGTERM, sig_handler)

        self._app.run(
            host='0.0.0.0',
            port=self._port,
            workers=1,
            register_sys_signals=False)

    def _generate_func_args(
            self,
            arg_definitions: List[ArgDefinition],
            data: Optional[Dict[str, Any]]) -> Dict[str, Any]:

        if data is None:
            data = {}

        if len(data) < sum([elm.is_required for elm in arg_definitions]):
            raise ValueError('Parameter Missing')

        func_args = {}

        for arg_def in arg_definitions:
            if arg_def.name in data:
                raw = data[arg_def.name]
                validated = validate_arg(raw, arg_def.type)
                if not validated.is_ok:
                    raise ValueError(f'num_required_argsParameter is Invalid: {arg_def.name} {raw} {arg_def.type.name}')  # NOQA
                func_args[arg_def.name] = validated.value

            elif arg_def.is_required:
                raise ValueError(f'Parameter Missing: {arg_def.name}')

        return func_args

    def exit_with_terminate(self):
        if self._main_process_id == os.getpid():
            self._task_manager.terminate_processes()
            get_event_loop().stop()
        else:
            sys.exit(0)

    def exit_with_join(self):
        if self._main_process_id == os.getpid():
            self._task_manager.join_processes()
            get_event_loop().stop()

    def add_job(
            self,
            func: Callable,
            arg_definitions: List[ArgDefinition],
            max_concurrency,
            description: str = '',
            endpoint_name: Optional[str] = None):

        if max_concurrency < 1:
            raise ValueError

        func_name = func.__name__

        if self._task_manager.has_job(func_name):
            self._logger.info(f'Duplicate Registration: {func_name}')

        if endpoint_name is None:
            endpoint_name = func_name

        api_endpoint = f'/call/{endpoint_name}'
        api_blocking_endpoint = f'/call/blocking/{endpoint_name}'

        self._task_manager.add_job(
            func_name,
            JobDefinition(
                func=func,
                max_concurrency=max_concurrency,
                arg_definitions=arg_definitions,
                endpoint=endpoint_name,
                description=description,
            ))

        @self._app.post(api_endpoint)
        async def post_task_function(request: request.Request):
            try:
                func_args = self._generate_func_args(
                    arg_definitions,
                    request.json)
            except ValueError as e:
                self._logger.info(e)
                return response.json({'error': str(e)}, 500)

            result = self._task_manager.fork_process(func_name, func_args)
            return response.json(result.to_dict())

        @self._app.post(api_blocking_endpoint)
        async def post_task_blocking_function(request: request.Request):
            try:
                func_args = self._generate_func_args(
                    arg_definitions,
                    request.json)
            except ValueError as e:
                self._logger.info(e)
                return response.json({'error': str(e)}, 500)

            result = self._task_manager.fork_process(func_name, func_args)
            if not result.successed:
                return response.json(result.to_dict())

            status = None
            while True:
                await sleep(self._polling_interval)

                status = self._task_manager.get_status(result.task_id)
                if status is None:
                    continue
                if status['status'] != JobState.RUNNING:
                    break

            return response.json(status['result'])

        self._logger.debug(f'Added: {api_endpoint}')
