import logging
import time
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor

import requests

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s|%(name)s|%(levelname)s|%(message)s',
)
logger = logging.getLogger(__name__)

_base_url = "http://127.0.0.1:8000/v1"
_token = "fake-jwt-token"

"""
Example curl command
curl -X GET "http://127.0.0.1:8000/v1/devices" \
     -H "Authorization: Bearer fake-jwt-token" \
     -H "Content-Type: application/json"
"""


def get_all_devices(timeout: int = 1, retries: int = 3, pagination_max: int = 500) -> List[Dict]:
    """
    Docstring for get_all_devices
    
    :param timeout: Description
    :type timeout: int
    :param retries: Description
    :type retries: int
    :param pagination_max: Description
    :type pagination_max: int
    :return: Description
    :rtype: List[Dict]
    """
    result = []
    pagination_count = 1
    
    url = f"{_base_url}/devices"
    while url:
        r = call_api(url=url)
        pagination_count+=1
        if pagination_count >= pagination_max:
            raise SystemError(f"get_all_devices|max pages hit: {pagination_count}")
        result.extend(r.get("devices") or [])
        if "next_url" in r.keys():
            if r.get("next_url"):
                url = f"{_base_url}{r.get("next_url")}"
                continue
            else:
                break
        else:
            break
    return result


def call_api(url: str = "", timeout: int = 5, retries: int = 3) -> dict:
    """
    Docstring for call_api
    
    :param url: Description
    :type url: str
    :param timeout: Description
    :type timeout: int
    :param retries: Description
    :type retries: int
    :return: Description
    :rtype: dict
    """
    token = "fake-jwt-token"
    headers = {
        "Authorization": f"Bearer {_token}",
        "Content-Type": "application/json",
    }
    for attempt in range(retries + 1):
        try:
            logger.info(f"call_api|requests.get: {url}")
            response = requests.get(url=url, headers=headers, timeout=timeout)
            
            response.raise_for_status()
            try:
                response_json = response.json()
            except requests.exceptions.JSONDecodeError as json_err:
                # here we expect valid json in the repsonse. Log and re-raise the error if not
                logger.error(f"call_api|json decode error: {response.text}")
                raise
            return response_json
        except requests.exceptions.RequestException as err:
            if attempt < retries:
                wait_time = 2**attempt
                logger.warning(
                    f"call_api|request failed:{err}. retrying in {wait_time}"
                )
                # here we sleep as a backoff
                time.sleep(wait_time)
            else:
                logger.error("call_api|max retries")
                raise


def get_device_by_id(id: str = "") -> dict:
    """
    Docstring for get_device_by_id
    
    :param id: Description
    :type id: str
    :return: Description
    :rtype: dict
    """
    if not id:
        logger.error("get_device_by_id|must input device id")
        raise ValueError("must input device id")
    device_url = f"{_base_url}/devices/{id}"
    device_json = call_api(url = device_url)
    return device_json


def get_devices_by_id_threaded(ids: list = []) -> List[Dict]:
    """
    Docstring for get_devices_by_id_threaded
    
    :param ids: Description
    :type ids: list
    :return: Description
    :rtype: List[Dict]
    """
    if not ids:
        logger.error("get_devices_by_id_threaded|must input device ids")
        raise ValueError("must input device ids")
    logger.info(f"get_devices_by_id_threaded|start for ids count:{len(ids)}")
    with ThreadPoolExecutor(max_workers=5) as tpe:
        futures = [tpe.submit(get_device_by_id, id=str(i)) for i in ids]
        results = [future.result() for future in futures]
        logger.info(f"get_devices_by_id_threaded|done got ids count:{len(results)}")
        if len(ids) != len(results):
            logger.warning(f"get_devices_by_id_threaded|mismatch of results to input devices")
        return results


if __name__=="__main__":
    print(f"{get_all_devices()[0:20]}")
    t = requests.get("http://127.0.0.1:8000/v1/devices/100", headers={'Authorization': 'Bearer fake-jwt-token'})
    # test threaded get devices by id
    t2 = get_devices_by_id_threaded(ids=list(range(1,20)))
    print(f"threaded: {t2}")
