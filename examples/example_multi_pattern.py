import concurrent.futures
import math
import os
import sys

# for develpment
try:
    import restful_functions  # NOQA
except Exception:
    print('using local version')
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
# ------------ #

from restful_functions import ArgDefinition, ArgType, FunctionServer


def addition(x: int, y: int):
    print(os.getpid(), os.getppid(), f'{x}+{y}={x+y}')
    return x+y


def no_arg_job():
    print(os.getpid(), os.getppid(), 'No args')


def is_prime(n: int):
    if n % 2 == 0:
        return False

    sqrt_n = int(math.floor(math.sqrt(n)))
    for i in range(3, sqrt_n + 1, 2):
        if n % i == 0:
            return False
    return True


def multi():
    PRIMES = [
        112272535095293,
        112582705942171,
        112272535095293,
        115280095190773,
        115797848077099,
        1099726899285419]

    with concurrent.futures.ProcessPoolExecutor() as executor:
        for number, prime in zip(PRIMES, executor.map(is_prime, PRIMES)):
            print('%d is prime: %s' % (number, prime))

    return True


if __name__ == '__main__':
    server = FunctionServer(debug=True)

    server.add_function(
        addition,
        [
            ArgDefinition('x', ArgType.INTEGER, True, 'x'),
            ArgDefinition('y', ArgType.INTEGER, True, 'y'),
        ],
        10,
        'Simple Addition')

    server.add_function(no_arg_job, [], 1)

    server.add_function(multi, [], 2, 'Multi Process Function')

    # Differenct Endopint for same function. Max concurrency is checked indivisualy.
    server.add_function(multi, [], 2, 'Multi Process Function (Another Name)', 'multi2')

    server.start()
