import asyncio
import json
import logging
import time
from datetime import date
from enum import Enum
from typing import Any, Callable, Dict, Union, List

import aiohttp.client_exceptions
from aiohttp.client import ClientSession

from .specs import get_ocr_spec, get_bboxes_spec, get_ocr_multiple_spec


class ProjectState(str, Enum):
    draft = 'draft'
    online = 'online'
    archived = 'archived'


class OperationType(str, Enum):
    labeling = 'labeling'
    bboxes = 'bboxes'
    ocr = 'ocr'
    ocr_multiple = 'ocr_multiple'


OperationSpecFactory = {
    OperationType.ocr: get_ocr_spec,
    OperationType.ocr_multiple: get_ocr_multiple_spec,
    OperationType.bboxes: get_bboxes_spec,
}


class ProjectGroup(str, Enum):
    dev = '7Z'
    stafify_full = '6M5'


Version = Union[None, int, str, Callable[[], Union[int, str]]]


def default_version_factory():
    return int(date.today().strftime('%Y%m%d'))


class Handl:
    attempts = 30
    attempt_delay = 1

    def __init__(
            self,
            url: str,
            username: str,
            password: str,
            prefix: str = 'HITL',
            version: Version = None,
            group: str = ProjectGroup.dev.value,
    ):
        self._url = url
        self._username = username
        self._password = password
        self._prefix = prefix
        self._version = version or default_version_factory
        self._group = group
        self._jwt_token_cached = None
        self._jwt_token_created_at = time.time()
        self._projects = {}

    async def _jwt_token(self) -> str:
        if time.time() - self._jwt_token_created_at > 600:
            self._jwt_token_cached = None

        if self._jwt_token_cached is not None:
            return self._jwt_token_cached

        while True:
            async with ClientSession() as sess:
                creds = dict(username=self._username, password=self._password)
                url = f'{self._url}/login?captcha_id=&solution='
                async with sess.post(url, json=creds) as resp:
                    data = await resp.json()

                if data.get('error') == 'Incorrect CAPTCHA':
                    logging.info('incorrect captcha. retry.')
                    continue

                if 'token' not in data:
                    logging.error(data)

                self._jwt_token_cached = data['token']
                self._jwt_token_created_at = time.time()
                return self._jwt_token_cached

    async def _auth_headers(self) -> Dict[str, str]:
        for _ in range(self.attempts):
            try:
                token = await self._jwt_token()
                return {'authorization': f'Bearer {token}'}
            except aiohttp.client_exceptions.ClientConnectionError:
                await asyncio.sleep(self.attempt_delay)

    async def _list_projects(self):
        url = f'{self._url}/projects'
        return await self._request(url)

    def _get_title(self, operation: OperationType, document_type: str = None, labels: List[str] = None) -> str:
        if callable(self._version):
            version = self._version()
        else:
            version = self._version
        if not isinstance(version, (str, int)):
            raise TypeError('version can be only str, int or Callable[[], Union[str, int]]')
        prefix = f'{self._prefix}_{version}'
        if operation == OperationType.ocr:
            return f'{prefix}__ocr'
        elif operation == OperationType.labeling:
            return f'{prefix}__labeling__{document_type}'
        elif operation == OperationType.bboxes:
            return f'{prefix}__bboxes__{document_type}__' + '__'.join(labels)
        elif operation == OperationType.ocr_multiple:
            return f'{prefix}__ocr_multiple__{document_type}__' + '__'.join(labels)
        raise AssertionError('invalid operation type')

    async def get_or_create_project(
            self,
            operation: OperationType,
            document_type: str = None,
            labels: List[str] = None,
            from_cache: bool = True,
    ) -> Dict[str, Any]:
        title = self._get_title(operation, document_type, labels)

        if from_cache and title in self._projects:
            return self._projects[title]

        if title in self._projects:
            project_id = self._projects['id']
            projects = [await self._get_project(project_id)]
        else:
            projects = await self._list_projects()

        for project in projects:
            if project['title'] == title and project['state'] == ProjectState.online.value:
                break
        else:
            spec_factory = OperationSpecFactory[operation]
            spec = spec_factory(document_type=document_type, labels=labels)
            project = await self._create_project(title, spec)
            project = await self._set_project_state(project['id'], ProjectState.online)

        self._projects[title] = project
        return project

    async def _create_project(self, title: str, spec: Dict[str, Any]) -> Dict[str, Any]:
        project = {
            "title": title,
            "description": str(int(time.time())),
            "type": "Standard",
            "reward_amount": "1",
            "expiration_timeout": 60 * 30,
            "privacy": [self._group],
            **spec,
        }
        url = f'{self._url}/projects'
        return await self._request(url, json=project, method='POST')

    async def _set_project_state(self, project_id: str, state: ProjectState):
        url = f'{self._url}/projects/{project_id}/state'
        return await self._request(url, json=state.value, method='PUT')

    async def create_task(self, name: str, content: bytes, text: str, project_id: str):
        url = f'{self._url}/projects/{project_id}/url?file={name}'
        data = await self._request(url)

        logging.debug(f'upload image: {data}')

        async with ClientSession() as sess:
            async with sess.put(data['uri'], data=content) as resp:
                await resp.read()

        data['text'] = text or ""

        logging.debug(f'create task on handl: {data}')

        url = f'{self._url}/projects/{project_id}/dataset'
        resp = await self._request(url, json=data, method='POST')
        return resp[0]

    async def _get_project(self, project_id: str) -> Dict[str, Any]:
        url = f'{self._url}/projects/{project_id}'
        return await self._request(url)

    async def get_results(self, project_id: str):
        url = f'{self._url}/projects/{project_id}/result'
        return await self._request(url)

    async def get_tasks(self, project_id: str):
        url = f'{self._url}/projects/{project_id}/dataset'
        return await self._request(url)

    async def get_groups(self):
        url = f'{self._url}/users/me/owned_groups'
        return await self._request(url)

    async def get_result(self, project_id: str, task_id: str):
        results = await self.get_results(project_id)
        for result in results:
            if task_id == result['id']:
                return result['payload']['text']

    async def _request(self, url: str, method: str = 'GET', **kw) -> Any:
        headers = await self._auth_headers()
        for _ in range(self.attempts):
            try:
                async with ClientSession() as sess:
                    async with sess.request(
                            method=method,
                            url=url,
                            headers=headers,
                            **kw,
                    ) as resp:
                        if resp.content_type == 'application/octet-stream':
                            return json.loads(await resp.read())
                        return await resp.json()
            except aiohttp.client_exceptions.ClientConnectionError:
                await asyncio.sleep(self.attempt_delay)
