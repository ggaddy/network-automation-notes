import functools
import logging
import random
import time

## setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s|%(levelname)s|%(message)s")


## logging decorator
def log_execution(func):
    """
    a decorator showing more execution information
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # setup some logging around the function that will be run
        logging.info(f"START: {func.__name__} | args: {args} | kwargs: {kwargs}")
        start_time = time.time()
        # run the function
        try:
            # get the result of the function
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            # log completed
            logging.info(
                f"COMPLETE: {func.__name__} | result: {result} | time: {execution_time}"
            )
            # pass result along
            return result
        except Exception as err:
            # log the exception and re-raise the error
            logging.error(f"ERROR: {func.__name__} | error: {err}")
            raise err

    return wrapper


## retry decorator
def retry_execution(func, retries: int = 3):
    """
    A decorator to rety failing functions
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        attempts = 0
        while attempts < retries:
            # try the function, return on success
            try:
                result = func(*args, **kwargs)
                # we dont need to increment the attempts if no exeception was raised
                return result
            except Exception as err:
                logging.error(f"retry_execution|retrying attempt: {attempts+1}")
                attempts += 1

    return wrapper


## function that makes random exceptions lol
@retry_execution
@log_execution
def risky_business():
    """
    Runs successfully 50% of the time.
    Raises a random exception the other 50% of the time.
    """

    # A list of standard exceptions to choose from
    possible_exceptions = [
        ValueError,
        TypeError,
        IndexError,
        KeyError,
        RuntimeError,
        ZeroDivisionError,
        AttributeError,
        NotImplementedError,
    ]

    # Flip a coin (50% chance)
    if random.random() < 0.5:
        # Choose a random exception class from the list
        chosen_exception = random.choice(possible_exceptions)

        # Raise an instance of that exception with a custom message
        raise chosen_exception(f"Unlucky! You hit a {chosen_exception.__name__}.")

    else:
        return "Success! You survived the coin flip."


## write a math class
class math:
    def __init__(self, x: int, y: int) -> None:
        self.x = x
        self.y = y

    def multiply(self) -> int:
        return self.x * self.y

    @log_execution
    def divide(self) -> float:
        return self.x / self.y

    def __repr__(self) -> str:
        return f"math: x={self.x} y={self.y}"


if __name__ == "__main__":
    t1 = math(1, 2)
    print(t1)
    print(f"multiply: {t1.multiply()}")
    t2 = math(2, 4)
    print(t2)
    print(f"multiply: {t2.multiply()}")
    print(f"divide: {t2.divide()}")

    ## doing some random exceptions to test the decorators
    for i in range(5):
        logging.info(f"risky_business: run {i}")
        risky_business()
