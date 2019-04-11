from typing import Any, List

import pytest
from restful_functions.modules.function import ArgType, validate_arg


@pytest.mark.parametrize('arg_type,expected_list', [
    (
        ArgType.INTEGER,
        [
            -1, 0, 1,
            -1, 0, 1,
            -1, 0, 1,
            None, None,
            None, None,
            None, None,
        ],
    ),
    (
        ArgType.FLOAT,
        [
            -1.0, 0.0, 1.0,
            -1.0, 0.0, 1.0,
            -1.0, 0.0, 1.0,
            None, None,
            None, None,
            None, None,
        ],
    ),
    (
        ArgType.STRING,
        [
            '-1', '0', '1',
            '-1.0', '0.0', '1.0',
            '-1', '0', '1',
            'True', 'False',
            '[]', "['a', 'b']",
            '{}', "{'a': 'A', 'b': 'B'}",
        ],
    ),
    (
        ArgType.BOOLEAN,
        [
            None, False, True,
            None, None, None,
            None, False, True,
            True, False,
            None, None,
            None, None,
        ],
    ),
    (
        ArgType.LIST,
        [
            None, None, None,
            None, None, None,
            None, None, None,
            None, None,
            [], ['a', 'b'],
            None, None,
        ],
    ),
    (
        ArgType.DICT,
        [
            None, None, None,
            None, None, None,
            None, None, None,
            None, None,
            None, None,
            {}, {'a': 'A', 'b': 'B'},
        ],
    ),
])
def test_validate_arg(arg_type: ArgType, expected_list: List[Any]):
    patterns = [
        -1, 0, 1,
        -1.0, 0.0, 1.0,
        '-1', '0', '1',
        True, False,
        [], ['a', 'b'],
        {}, {'a': 'A', 'b': 'B'},
        ]

    for idx in range(len(patterns)):
        elm = patterns[idx]
        expected = expected_list[idx]

        ret = validate_arg(elm, arg_type)

        if expected is None:
            assert not ret.is_ok
        else:
            assert ret.is_ok
            assert ret.value == expected
