import asyncio
import base64
from dataclasses import dataclass, field

import aiohttp


@dataclass
class SDK:
    host: str
    token: str = None
    system_info_token: str = None
    tasks: dict = field(default_factory=lambda: {})

    async def _request(self, method: str, params: dict = {}, data: dict = {}) -> [dict]:
        headers = {
            'Content-Type': 'application/json',
        }
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

    async def create_tasks(self, tasks: [dict], document_id: str = None, task_type: str = 'standard') -> [dict]:
        body = [
            {
                'img': base64.b64encode(
                    task['img'],
                ).decode() if isinstance(task['img'], bytes) else task['img'],
                'predict': task.get('predict'),
                'code': task.get('code'),
                'type': task_type,
                'document_id': task.get('document_id') if not document_id else document_id,
            }
            for task in tasks
            if task.get('img')
        ]
        if not body: return []

        tasks = await self._request(
            method='POST',
            data=body,
        )

        for task in tasks:
            self.tasks[task['id']] = task

        return list(self.tasks.values())

    async def sync_tasks(self):
        tasks_ids = list(filter(
            lambda _: not self.tasks.get(_) or not self.tasks[_]['completed_at'],
            self.tasks.keys(),
        ))
        if not tasks_ids: return

        try:
            tasks = await self._request(
                method='GET',
                data={
                    'ids': tasks_ids,
                },
            )

            for task in tasks:
                self.tasks[task['id']] = task
        except Exception as e:
            print(e)

    def in_work_count(self) -> int:
        return sum(
            1
            for v in self.tasks.values()
            if not v['completed_at']
        )

    async def wait_until_complete(self, timeout: float = 5.) -> [dict]:
        while self.in_work_count() != 0:
            print(f'In work {self.in_work_count()} tasks. Sync...')
            await self.sync_tasks()
            await asyncio.sleep(timeout)
        return list(self.tasks.values())

    async def create_and_wait(self, tasks: list, document_id: str = None, timeout: float = 5.) -> [dict]:
        await self.create_tasks(
            tasks=tasks,
            document_id=document_id,
        )
        return await self.wait_until_complete(timeout=timeout)
