from typing import *

import os
from dataclasses import dataclass, field
import dataclasses_json

import asyncio
import base64
import datetime
import dateutil.parser

import aiohttp

Value = Union[str, List[str]]


@dataclasses_json.dataclass_json
@dataclass
class Task:
    id: Optional[str] = None
    state: Optional[str] = None

    document_type: Optional[str] = None
    document_id: Optional[str] = None
    field_type: Optional[str] = None

    created_at: Optional[datetime.datetime] = field(
        default=None,
        metadata=dataclasses_json.config(
            decoder=lambda _: dateutil.parser.parse(_) if _ else None
        ),
    )
    completed_at: Optional[datetime.datetime] = field(
        default=None,
        metadata=dataclasses_json.config(
            decoder=lambda _: dateutil.parser.parse(_) if _ else None
        ),
    )

    result: Optional[Value] = None
    tasks: List['Task'] = field(default_factory=list)

    # Using when creating
    image: Optional[Union[str, bytes]] = None
    images: List[Union[str, bytes]] = field(default_factory=list)
    uncut_images: List[Union[str, bytes]] = field(default_factory=list)
    predict: Optional[Value] = None
    predict_confidence: Optional[float] = None
    type: str = 'standard'
    pipeline: Optional[List[str]] = None
    field_name: Optional[str] = None
    code: Optional[str] = None

    def __post_init__(self):
        if self.image:
            self.images.append(self.image)


@dataclass
class SDK:
    host: str
    token: Optional[str] = None
    system_info_token: Optional[str] = None
    tasks: Dict[str, Task] = field(default_factory=dict)
    document: Optional[Task] = None

    @staticmethod
    def _get_task_key(task: Task) -> str:
        return (
            f'{task.id}:{task.field_name}'
            if task.field_name
            else
            task.id
        )

    async def _request(self,
                       method: str,
                       params: Optional[dict] = None,
                       data: Optional[Union[dict, list]] = None,
                       endpoint: str = 'tasks') -> List[dict]:
        headers = {
            'Content-Type': 'application/json',
        }
        if params is None:
            params = {}

        if self.token:
            headers['Authorization'] = f'Token {self.token}'
        if self.system_info_token:
            params['system_info'] = self.system_info_token

        async with aiohttp.ClientSession() as session:
            async with session.request(
                    method=method,
                    url=os.path.join(self.host, endpoint),
                    params=params,
                    json=data,
            ) as resp:
                try:
                    resp.raise_for_status()
                except Exception as e:
                    print(resp.content)
                    raise e

                return await resp.json()

    async def create_tasks(
            self,
            tasks: List[Task],
            document_type: Optional[str] = None,
            document_id: Optional[str] = None,
            task_type: Optional[str] = 'standard',
            mock: bool = False,
            processing_type: Optional[str] = None,
    ) -> List[Task]:
        body = [
            {
                'images': [
                    base64.b64encode(
                        image,
                    ).decode()
                    if isinstance(image, bytes) else
                    image
                    for image in task.images
                ],
                'uncut_images': [
                    base64.b64encode(
                        image,
                    ).decode()
                    if isinstance(image, bytes) else
                    image
                    for image in task.uncut_images
                ],
                'predict': task.predict,
                'predict_confidence': task.predict_confidence,
                'type': task_type,
                'field_name': task.field_name,
                'document_type': document_type,
                'code': task.code,
                'document_id': document_id,
                'pipeline': task.pipeline,
            }
            for task in tasks
            if task.images
        ]
        if not body:
            return []

        params = {}
        if mock:
            params['mock'] = 'true'
        if processing_type:
            params['processing_type'] = processing_type

        resp = await self._request(
            method='POST',
            data=body,
            params=params,
        )

        for item in resp:
            task = Task.from_dict(item)
            self.tasks[self._get_task_key(task)] = task

        return list(self.tasks.values())

    async def create_document(
            self,
            images: List[Union[bytes, str]],
            document_type: Optional[str] = None,
            document_id: Optional[str] = None,
            only_classify: bool = False,
            only_ocr: bool = False,
            integrity_check: bool = False,
            mock: bool = False,
            processing_type: Optional[str] = None,
    ) -> Optional[Task]:
        payload = {
            'images': [
                base64.b64encode(
                    image,
                ).decode()
                if isinstance(image, bytes)
                else
                image
                for image in images
            ],
            'document_type': document_type,
            'document_id': document_id,
        }

        if not payload:
            return None

        params = {}
        if only_classify:
            params['only_classify'] = 'true'
        if only_ocr:
            params['only_ocr'] = 'true'
        if integrity_check:
            params['integrity_check'] = 'true'
        if mock:
            params['mock'] = 'true'
        if processing_type:
            params['processing_type'] = processing_type

        resp = await self._request(
            method='POST',
            endpoint='document',
            data=payload,
            params=params,
        )

        self.document = Task.from_dict(resp)

        return self.document

    async def sync_document(self):
        try:
            resp = await self._request(
                method='GET',
                endpoint='document',
                params={
                    'id': self.document.id,
                }
            )

            self.document = Task.from_dict(resp)

            for task in self.document.tasks:
                task = Task.from_dict(task)
                self.tasks[self._get_task_key(task)] = task
        except Exception as e:
            print(e)

    async def sync_tasks(self):
        tasks_ids = [
            _id.split(':')[0]
            for _id in filter(
                lambda _: not self.tasks.get(_) or not self.tasks[_].completed_at,
                self.tasks.keys(),
            )
        ]
        if not tasks_ids:
            return

        try:
            tasks = await self._request(
                method='GET',
                data={
                    'ids': tasks_ids,
                },
            )

            for task in tasks:
                task = Task.from_dict(task)
                self.tasks[self._get_task_key(task)] = task
        except Exception as e:
            print(e)

    def in_work_count(self) -> Tuple[int, int]:
        return (
            sum(
                1
                for v in self.tasks.values()
                if not v.completed_at
            ),
            int(bool(
                self.document and not self.document.completed_at
            )),
        )

    async def wait_until_complete(self, timeout: float = 5.) -> List[Task]:
        while True:
            in_work = self.in_work_count()
            if not sum(in_work):
                break

            await asyncio.sleep(timeout)

            if in_work[1]:
                print(f'HITL: In work {in_work[1]} document. Sync...')
                await self.sync_document()
            else:
                print(f'HITL: In work {in_work[0]} tasks. Sync...')
                await self.sync_tasks()
        return list(self.tasks.values())

    async def create_and_wait(self,
                              tasks: List[Task], document_type: Optional[str] = None,
                              timeout: float = 5., **kwargs) -> List[Task]:
        await self.create_tasks(
            tasks=tasks,
            document_type=document_type,
            **kwargs,
        )
        return await self.wait_until_complete(timeout=timeout)
