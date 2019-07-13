from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class ArgType(Enum):
    INTEGER = 'INTEGER'
    FLOAT = 'FLOAT'
    STRING = 'STRING'
    BOOLEAN = 'BOOLEAN'
    LIST = 'LIST'
    DICT = 'DICT'


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
    except Exception:
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

    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'type': self.type.name,
            'is_required': self.is_required,
            'description': self.description,
        }


class FunctionDefinition:
    __slots__ = [
        'func',
        'arg_definitions',
        'max_concurrency',
        'description',
        'function_name',
    ]

    def __init__(
            self,
            func: Callable,
            arg_definitions: List[ArgDefinition],
            max_concurrency: int,
            description: str,
            function_name: str):
        """.

        Parameters
        ----------
        func
            Python Function
        arg_definitions
            A List of ArgDefinitions
        max_concurrency
            Max Concurrency
        description
            A Description for this Function.
        function_name
            Function Name. It is not necessary to be same with func.__name__

        """
        self.func = func
        self.arg_definitions = arg_definitions
        self.max_concurrency = max_concurrency
        self.description = description
        self.function_name = function_name

    def to_dict(self) -> Dict[str, Any]:
        return {
            'function_name': self.function_name,
            'arg_definitions': [elm.to_dict() for elm in self.arg_definitions],
            'max_concurrency': self.max_concurrency,
            'description': self.description,
        }
