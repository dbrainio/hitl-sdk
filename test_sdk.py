import asyncio
import logging

from hitl_sdk.sdk import SDK as HitlSDK, Task


def test_retry():
    async def _test():
        sdk = HitlSDK(host='http://localhost:8888', request_retry_strategy=[1, 3, 5])
        logging.debug("OK")
        task = Task(id='123456', images=[b'123456'])
        try:
            await sdk.create_tasks([task], mock=True)
        except Exception as e:
            assert f"{e}" == "Cannot connect to host localhost:8888 ssl:None [Connect call failed ('127.0.0.1', 8888)]"
    asyncio.get_event_loop().run_until_complete(_test())
