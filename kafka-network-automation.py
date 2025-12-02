import asyncio
import json
import logging
from concurrent.futures import ThreadPoolExecutor

import etcd3  # The standard 'python-etcd3' library
from aiokafka import AIOKafkaConsumer

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("net-worker")


class AsyncNetworkDeviceLock:
    """
    Wraps the synchronous etcd3 lock in an asyncio-friendly Context Manager.
    Runs blocking calls in a separate thread to protect the Event Loop.
    """

    def __init__(self, etcd_client, device_name, ttl=60):
        self.client = etcd_client
        self.lock_name = f"/locks/device/{device_name}"
        self.ttl = ttl
        self.loop = asyncio.get_running_loop()
        # Create the lock object (this is non-blocking, it just sets up the object)
        self.lock = self.client.lock(self.lock_name, ttl=self.ttl)
        self.is_locked = False

    async def __aenter__(self):
        # Run the blocking .acquire() in a thread executor
        # returns True if acquired, False if not
        logger.info(
            f"AsyncNetworkDeviceLock|attempting to acquire lock for {self.lock_name}..."
        )

        acquired = await self.loop.run_in_executor(
            None, self.lock.acquire  # Uses the default ThreadPoolExecutor
        )

        if not acquired:
            raise RuntimeError(
                f"AsyncNetworkDeviceLock|could not acquire lock for {self.lock_name}"
            )

        self.is_locked = True
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.is_locked:
            # Run the blocking .release() in a thread executor
            await self.loop.run_in_executor(None, self.lock.release)
            self.is_locked = False
            logger.info(f"AsyncNetworkDeviceLock|lock released for {self.lock_name}")


async def process_jobs():
    # 1. Setup Sync ETCD Client
    # We create it once. It uses gRPC under the hood.
    etcd_client = etcd3.client(host="localhost", port=2379)

    # 2. Setup Async Kafka Consumer
    consumer = AIOKafkaConsumer(
        "network.config.requests",
        bootstrap_servers="localhost:9092",
        group_id="net-automation-group",
        enable_auto_commit=False,
        max_poll_records=1,
        max_poll_interval_ms=600000,
    )

    await consumer.start()
    loop = asyncio.get_running_loop()

    try:
        async for msg in consumer:
            payload = json.loads(msg.value)
            job_id = payload["job_id"]
            device = payload["device"]
            job_status_key = f"/jobs/{job_id}/status"

            logger.info(f"process_jobs|Received Job {job_id} for {device}")

            # --- STEP 1: DEDUPLICATION (Zombie Check) ---
            # We must wrap the blocking 'get' call
            # etcd3 returns (value, metadata) tuple
            def check_status():
                return etcd_client.get(job_status_key)

            current_value, _ = await loop.run_in_executor(None, check_status)

            if current_value and current_value == b"COMPLETED":
                logger.warning(
                    f"process_jobs|Job {job_id} already marked COMPLETED. Skipping."
                )
                await consumer.commit()
                continue

            # --- STEP 2: DISTRIBUTED LOCKING ---
            try:
                # Use our custom wrapper to keep the loop non-blocking
                async with AsyncNetworkDeviceLock(etcd_client, device) as lock:

                    # --- STEP 3: THE WORK (Async Scrapli/Netmiko) ---
                    logger.info(f"process_jobs|Configuring {device}...")

                    ## !!! This is where your actual async network code goes
                    # Using sleep to simulate SSH latency
                    await asyncio.sleep(2)

                    # --- STEP 4: MARK COMPLETE ---
                    # Wrap the blocking 'put' call
                    def mark_complete():
                        etcd_client.put(job_status_key, "COMPLETED")

                    await loop.run_in_executor(None, mark_complete)
                    logger.info(f"process_jobs|Job {job_id} finished.")

                # --- STEP 5: COMMIT OFFSET ---
                # AIOKafka is native async, so we just await it directly
                await consumer.commit()
                logger.info(f"process_jobs|msg {msg.offset} committed.")

            except RuntimeError:
                logger.error(f"process_jobs|Device {device} is locked. Retrying later.")
                # Strategy: Pause briefly before next poll to let lock clear
                await asyncio.sleep(5)

    finally:
        await consumer.stop()
        # etcd3 client doesn't explicitly need close() usually, but good practice if wrapper supports it
        # etcd_client.close()


if __name__ == "__main__":
    # run continously executing jobs from the kafka bus
    asyncio.run(process_jobs())

    ## nice things to add
    # dill dump and upload on exception
