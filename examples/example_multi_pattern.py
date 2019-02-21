import concurrent.futures
import math
import os
import time

from restful_functions import ArgDefinition, ArgType, FunctionServer


def awesome(x: int, y: int, wait: int):
    time.sleep(wait)
    print(os.getpid(), os.getppid(), f'{x}+{y}={x+y}')
    return x+y


def no_arg_job():
    print(os.getpid(), os.getppid(), 'No args')


def is_prime(n):
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
    server = FunctionServer('join')
    server.add_job(
        awesome,
        [
            ArgDefinition('x', ArgType.INTEGER, True, 'x'),
            ArgDefinition('y', ArgType.INTEGER, True, 'y'),
            ArgDefinition('wait', ArgType.INTEGER, True, 'Waiting Seconds'),
        ],
        10,
        '足し算')
    server.add_job(no_arg_job, [], 1)
    server.add_job(multi, [], 2, 'マルチプロセス')

    server.start()
