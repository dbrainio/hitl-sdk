import asyncio
import base64
import json
from hitl_sdk import SDK, Task


async def create_doc(img: str, doc_type: str, only_classify: bool = False):
    sdk = SDK(
        host='https://hitl-dev.dbrain.io',
    )

    await sdk.create_document(
        images=[img],
        document_type=doc_type,
        only_classify=only_classify,
    )

    # sdk.document = Task(
    #     id='5de6a7b54597480001c1d9e9'
    # )

    tasks = await sdk.wait_until_complete()
    for task in tasks:
        print(json.dumps(
            json.loads(task.to_json()),
            indent=2,
        ))
        print("+" * 50)

    print("DOCUMENT")
    print(json.dumps(
        json.loads(sdk.document.to_json()),
        indent=2,
    ))


async def run():
    for f_name in ['/Users/nilhex/Desktop/Screenshot 2019-12-06 at 21.10.46.png']:
        f = open(f_name, 'rb').read()
        img = base64.b64encode(f).decode()
        await create_doc(
            img=img,
            doc_type='administrative_offense_definition',
            only_classify=True,
        )


l = asyncio.get_event_loop()
l.run_until_complete(run())
