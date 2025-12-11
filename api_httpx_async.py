#!/usr/bin/env python
"""
Docstring for api_petstore_async

interacting with https://petstore3.swagger.io/
using asyncio and httpx patterns
"""

import asyncio
import logging
import time
from typing import Any, Dict, List

import httpx

_base_url = "https://petstore3.swagger.io/api/v3"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def get_pet_by_id(
    id: str = "",
    timeout: int = 1,
    retries: int = 3,
) -> dict[str, Any]:
    """Fetch a pet by ID and return a single result dict.

    Retries up to `retries` times on network / HTTP errors, then raises
    RuntimeError if all attempts fail.

    curl -X 'GET' \
      'https://petstore3.swagger.io/api/v3/pet/10' \
      -H 'accept: application/json'
    """

    logger.info("get_pet_by_id|start")
    if id is None or not id:
        raise ValueError("get_pet_by_id|invalid id")

    api_url = f"{_base_url}/pet/{id}"

    # define response dict
    result = {"response": None, "status_code": None, "error": None, "id": id}

    for retry in range(retries):
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url=api_url, timeout=timeout)
                result["status_code"] = response.status_code
                # if the pet id is not found we get a http 404
                # include any other http codes we want to skip the retry on here
                if response.status_code not in [404]:
                    response.raise_for_status()
                    response_json = response.json()
                    result["response"] = response_json
                logger.info("get_pet_by_id|done")
                return result
        except httpx.RequestError as err:
            logger.error(f"get_pet_by_id|RequestError: {err}")
            result["error"] = str(err)
        except httpx.HTTPError as err:
            logger.error(f"get_pet_by_id|HTTPError: {err}")
            result["error"] = str(err)
        except Exception as err:
            logger.error(f"get_pet_by_id|unknown exception: {err}")
            result["error"] = str(err)

        backoff_timer = (2**retry) * 0.5
        logger.warning(f"get_pet_by_id|retry: {retry} backoff {backoff_timer}")
        await asyncio.sleep(backoff_timer)

    logger.error(f"get_pet_by_id|max retries: {retries}")
    result["error"] = "max_retries"
    return result


async def test_get_pet_by_id() -> None:
    test = await get_pet_by_id("10")
    print(test)


async def test_get_pets_by_id(concurrent: int = 3) -> None:
    """Run get_pet_by_id concurrently

    This helper is intended for manual/interactive testing rather than as a
    pytest unit test. It will fire off all the requests concurrently using
    asyncio.gather, then print the results for quick inspection.
    """

    ids = list(range(1, 30))

    # Run them concurrently. Print everything at once at the end.
    # coros = [get_pet_by_id(str(pet_id)) for pet_id in ids]
    # results = await asyncio.gather(*coros, return_exceptions=True)
    # print(results)

    # Run them concurrently, limited by the semaphore, print the results as they come in
    semaphore = asyncio.Semaphore(concurrent)

    async def worker(pet_id: int):
        async with semaphore:
            return await get_pet_by_id(str(pet_id))

    coros = [worker(pet_id) for pet_id in ids]

    all_results = []

    for coroutine in asyncio.as_completed(coros):
        result = await coroutine
        print(result)

        # store for later
        all_results.append(result)
    # print the length of the results we got
    print(f"test_get_pets_by_id|got {len(all_results)} pets!")


async def get_all_starwars_people(
    timeout: int = 1, retries: int = 3, max_calls: int = 500
) -> List[Dict[str, Any]]:
    """
    Docstring for get_all_starwars_people

    curl -X 'GET' \
        'https://petstore3.swagger.io/api/v3/pet/findByStatus?status=available' \
        -H 'accept: application/json'
    """
    _url = "https://swapi.dev/api/people/"
    results = []
    max_count = 0
    while _url:
        for retry in range(retries):
            # exit the while loop on max_calls
            max_count += 1
            if max_count > max_calls:
                raise StopIteration(f"max_calls: {max_calls} exceeded")

            logger.info(f"get_all_starwars_people|start {_url}")
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.get(url=_url, timeout=timeout)
                    logger.info(f"get_all_starwars_people|got {_url}")
                    # log any non 200 status
                    if response.status_code != 200:
                        logger.warning(
                            f"get_all_starwars_people|url {_url} status {response.status_code}"
                        )
                    response.raise_for_status()

                    response_json = response.json()
                    results.extend(response_json.get("results", []))
                    _url = response_json.get("next", None)
                    # break out of the retry loop and back up to the while loop
                    break
            except httpx.HTTPError as err:
                logger.error(f"get_all_starwars_people|HTTPError: {err}")
            except httpx.RequestError as err:
                logger.error(f"get_all_starwars_people|RequestError: {err}")
            except Exception as err:
                logger.error(f"get_all_starwars_people|exception: {err}")
            logger.info(f"get_all_starwars_people|retry {_url}")

    return results


async def test_get_all_starwars_people() -> None:
    test = await get_all_starwars_people()
    print(test)


if __name__ == "__main__":
    # To run a single pet fetch:
    # print("### test|test_get_pet_by_id")
    # asyncio.run(test_get_pet_by_id())

    # To run the multi-pet test harness:
    print("### test|test_get_pets_by_id")
    asyncio.run(test_get_pets_by_id())

    # print out the first 100 results from get_all_starwars_people
    # print("### test|get_all_starwars_people")
    # asyncio.run(test_get_all_starwars_people())
