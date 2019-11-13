from typing import *

from dataclasses import dataclass, field
import dataclasses_json

import asyncio
import base64
import datetime

import aiohttp


@dataclasses_json.dataclass_json
@dataclass
class Task:
    id: Optional[str] = None
    result: Optional[str] = None
    field_name: Optional[str] = None
    code: Optional[str] = None
    created_at: Optional[datetime.datetime] = field(
        default=None,
        metadata=dataclasses_json.config(
            decoder=lambda _: datetime.datetime.fromisoformat(_) if _ else None
        ),
    )
    completed_at: Optional[datetime.datetime] = field(
        default=None,
        metadata=dataclasses_json.config(
            decoder=lambda _: datetime.datetime.fromisoformat(_) if _ else None
        ),
    )

    # Using when creating
    img: Optional[Union[str, bytes]] = None
    predict: Optional[str] = None
    type: str = 'standard'
    pipeline: List[str] = field(default_factory=lambda: ['validation', 'ocr'])


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
            document_name: Optional[str] = None,
            document_id: Optional[str] = None,
            task_type: Optional[str] = 'standard',
            mock: bool = False
    ) -> List[Task]:
        body = [
            {
                'img': base64.b64encode(
                    task.img,
                ).decode() if isinstance(task.img, bytes) else task.img,
                'predict': task.predict,
                'type': task_type,
                'field_name': task.field_name,
                'document_name': document_name,
                'code': task.code,
                'document_id': document_id,
                'pipeline': task.pipeline if not mock else ['mock'],
            }
            for task in tasks
            if task.img
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
                              tasks: List[Task], document_name: Optional[str] = None,
                              timeout: float = 5., **kwargs) -> List[Task]:
        await self.create_tasks(
            tasks=tasks,
            document_name=document_name,
            **kwargs,
        )
        return await self.wait_until_complete(timeout=timeout)
