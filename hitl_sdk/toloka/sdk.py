import asyncio
import base64
import datetime
import os
from dataclasses import dataclass, field
from logging import getLogger, Logger
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, Union

import aiohttp

from ..common import default_retry_strategy, DocumentStruct, Task
from ..env import SUGGESTIONS_GATEWAY


@dataclass
class SDK:
    host: str
    token: Optional[str] = None
    license_id: Optional[str] = None
    system_info_token: Optional[str] = None
    tasks: Dict[str, Task] = field(default_factory=dict)
    document: Optional[Task] = None
    request_retry_strategy: Optional[Iterable] = default_retry_strategy()
    suggestions_gateway: Optional[str] = SUGGESTIONS_GATEWAY
    logger: Logger = getLogger('hitl-sdk')
    confidence_threshold: Optional[Any] = None

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
                       endpoint: str = 'tasks',
                       retry_times: Iterator = None) -> List[dict]:
        if not retry_times:
            retry_times = []
        for retry_time in retry_times:
            try:
                return await self.__request(
                    method=method,
                    params=params,
                    data=data,
                    endpoint=endpoint,
                )
            except Exception as e:
                print(e)
                await asyncio.sleep(retry_time)
        return await self.__request(
            method=method,
            params=params,
            data=data,
            endpoint=endpoint,
        )

    async def __request(self,
                        method: str,
                        params: Optional[dict] = None,
                        data: Optional[Union[dict, list]] = None,
                        endpoint: str = 'tasks') -> List[dict]:
        headers = {
            'Content-Type': 'application/json',
        }
        if params is None:
            params = {}

        if self.license_id:
            params['license_id'] = self.license_id

        if self.token:
            headers['Authorization'] = f'Token {self.token}'
        if self.system_info_token:
            params['system_info'] = self.system_info_token

        self.logger.debug(f'HITL SDK request: {method} {endpoint} params={params}')
        async with aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(verify_ssl=False),
                raise_for_status=True,
        ) as session:
            async with session.request(
                    method=method,
                    url=os.path.join(self.host, endpoint),
                    headers=headers,
                    params=params,
                    json=data,
            ) as resp:
                resp.raise_for_status()
                return await resp.json()

    async def create_tasks(
            self,
            tasks: List[Task],
            document_type: Optional[str] = None,
            document_id: Optional[str] = None,
            task_type: Optional[str] = 'standard',
            mock: bool = False,
            processing_type: Optional[str] = None,
            document_structure: DocumentStruct = None,
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
                'field_type': 'bool_array' if task.is_checkbox_array else None,
                'suggestions_gateway': task.suggestions_gateway or self.suggestions_gateway,
                'deadline_at': task.deadline_at and task.deadline_at.isoformat(),

            }
            for task in tasks
            if task.images
        ]
        if not body:
            return []

        if document_structure is not None:
            body[0]['document_structure'] = document_structure.to_dict()
        # Костыль: нужно как-то передать document_structure, но лишь один раз.

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
            deadline_at: datetime.datetime = None,
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
            'suggestions_gateway': self.suggestions_gateway,
            'deadline_at': deadline_at and deadline_at.isoformat(),
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

    async def ocr_multiple(self, *_, **__):
        raise NotImplementedError()

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

    async def sync_tasks(self) -> bool:
        tasks_ids = set(
            _id.split(':')[0]
            for _id in filter(
                lambda x: (
                        not self.tasks.get(x)
                        or
                        not self.tasks[x].completed_at
                ),
                self.tasks.keys(),
            )
        )

        has_updates = False

        if not tasks_ids:
            return has_updates

        try:
            tasks = await self._request(
                method='GET',
                data={
                    'ids': list(tasks_ids),
                },
            )

            for task in tasks:
                task = Task.from_dict(task)
                self.tasks[self._get_task_key(task)] = task
                if task.completed_at:
                    has_updates = True
        except Exception as e:
            print(e)

        return has_updates

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

    async def create_and_wait(
            self,
            tasks: List[Task], document_type: Optional[str] = None,
            timeout: float = 5., **kwargs
    ) -> List[Task]:
        await self.create_tasks(
            tasks=tasks,
            document_type=document_type,
            **kwargs,
        )
        return await self.wait_until_complete(timeout=timeout)
