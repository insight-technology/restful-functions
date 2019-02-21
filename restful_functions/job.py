from enum import Enum, auto
from typing import Any, Callable, List, Optional

from .logger import get_logger

logger = get_logger(__name__)


class ArgType(Enum):
    INTEGER = auto()
    FLOAT = auto()
    STRING = auto()
    BOOLEAN = auto()
    LIST = auto()
    DICT = auto()


class ArgValidateResult:
    __slots__ = ['is_ok', 'value']

    def __init__(self, is_ok: bool, value: Optional[Any]):
        self.is_ok = is_ok
        self.value = value


def validate_arg(
        value: Any,
        arg_type: ArgType) -> ArgValidateResult:

    try:
        stringified = str(value)

        if arg_type == ArgType.INTEGER:
            if isinstance(value, float):
                return ArgValidateResult(True, int(value))
            return ArgValidateResult(True, int(stringified, 10))
            
        elif arg_type == ArgType.FLOAT:
            if isinstance(value, bool):
                raise ValueError
            return ArgValidateResult(True, float(value))

        elif arg_type == ArgType.STRING:
            return ArgValidateResult(True, stringified)

        elif arg_type == ArgType.BOOLEAN:
            lower = stringified.lower()
            if lower in ['true', '1', 't', 'y', 'yes']:
                ret = True
            elif lower in ['false', '0', 'f', 'n', 'no']:
                ret = False
            else:
                raise ValueError
            return ArgValidateResult(True, ret)

        elif arg_type == ArgType.LIST:
            if not isinstance(value, list):
                raise ValueError
            return ArgValidateResult(True, value)
        elif arg_type == ArgType.DICT:
            if not isinstance(value, dict):
                raise ValueError
            return ArgValidateResult(True, value)
        else:
            raise NotImplementedError
    except Exception as e:
        logger.debug(e)
        return ArgValidateResult(False, None)


class ArgDefinition:
    __slots__ = ['name', 'type', 'is_required', 'description']

    def __init__(
            self,
            arg_name: str,
            arg_type: ArgType,
            is_required: bool,
            description: str = ''):
        self.name = arg_name
        self.type = arg_type
        self.is_required = is_required
        self.description = description


class JobDefinition:
    __slots__ = [
        'func',
        'max_concurrency',
        'arg_definitions',
        'endpoint',
        'description',
    ]

    def __init__(
            self,
            func: Callable,
            max_concurrency: int,
            arg_definitions: List[ArgDefinition],
            endpoint: str,
            description: str):
        self.func = func
        self.max_concurrency = max_concurrency
        self.arg_definitions = arg_definitions
        self.endpoint = endpoint
        self.description = description


class JobState:
    RUNNING = 'RUNNING'
    FAILED = 'FAILED'
    DONE = 'DONE'
