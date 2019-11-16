from typing import *

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

    # Using when creating
    image: Optional[Union[str, bytes]] = None
    predict: Optional[Value] = None
    predict_confidence: Optional[float] = None
    type: str = 'standard'
    pipeline: Optional[List[str]] = None
    field_name: Optional[str] = None
    code: Optional[str] = None


@dataclass
class SDK:
    host: str
    token: Optional[str] = None
    system_info_token: Optional[str] = None
    tasks: Dict[str, Task] = field(default_factory=lambda: {})

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
                       data: Optional[Union[dict, list]] = None) -> [dict]:
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
                    url=f'{self.host}/tasks',
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
            mock: bool = False
    ) -> List[Task]:
        body = [
            {
                'img': base64.b64encode(
                    task.image,
                ).decode() if isinstance(task.image, bytes) else task.image,
                'predict': task.predict,
                'predict_confidence': task.predict_confidence,
                'type': task_type,
                'field_name': task.field_name,
                'document_type': document_type,
                'code': task.code,
                'document_id': document_id,
                'pipeline': task.pipeline if not mock else ['mock'],
            }
            for task in tasks
            if task.image
        ]
        if not body:
            return []

        tasks = await self._request(
            method='POST',
            data=body,
        )

        for task in tasks:
            task = Task.from_dict(task)
            self.tasks[self._get_task_key(task)] = task

        return list(self.tasks.values())

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

    def in_work_count(self) -> int:
        return sum(
            1
            for v in self.tasks.values()
            if not v.completed_at
        )

    async def wait_until_complete(self, timeout: float = 5.) -> List[Task]:
        while self.in_work_count() != 0:
            print(f'In work {self.in_work_count()} tasks. Sync...')
            await self.sync_tasks()
            await asyncio.sleep(timeout)
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
