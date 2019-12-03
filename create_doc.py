import asyncio
import base64
import json
from hitl_sdk import SDK


async def create_doc(img: str, doc_type: str):
    sdk = SDK(
        host='https://hitl.dbrain.io',
    )

    tasks = await sdk.create_document(
        images=[img],
        document_type=doc_type,
    )

    # tasks = await sdk.wait_until_complete()
    for task in tasks:
        print(json.dumps(
            json.loads(task.to_json()),
            indent=4,
        ))


async def run():
    for f_name in ['/Users/nilhex/Downloads/photo_2019-11-16 22.50.45.jpeg']:
        f = open(f_name, 'rb').read()
        img = base64.b64encode(f).decode()
        await create_doc(
            img=img,
            doc_type='renins_kasko',
        )


l = asyncio.get_event_loop()
l.run_until_complete(run())
