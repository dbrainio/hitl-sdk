import asyncio
import base64
import json
import logging
from io import BytesIO
from dataclasses import dataclass, field
from datetime import datetime
from logging import getLogger, Logger
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from uuid import uuid4

import numpy
import numpy as np
from PIL import Image

from .api import Handl, OperationType
from ..common import default_retry_strategy, Task, concat_v, DocumentStruct
from ..env import (HANDL_GATEWAY, HANDL_GROUP, HANDL_PASSWORD, HANDL_PREFIX, HANDL_TASK_TIMEOUT, HANDL_USERNAME,
                   HANDL_VERSION, SUGGESTIONS_GATEWAY)

handl = Handl(
    url=HANDL_GATEWAY,
    username=HANDL_USERNAME,
    password=HANDL_PASSWORD,
    prefix=HANDL_PREFIX,
    version=HANDL_VERSION,
    group=HANDL_GROUP,
)


@dataclass
class SDK:
    host: Optional[str] = None
    token: Optional[str] = None
    license_id: Optional[str] = None
    system_info_token: Optional[str] = None
    tasks: Dict[str, Task] = field(default_factory=dict)
    document: Optional[Task] = None
    request_retry_strategy: Optional[Iterable] = default_retry_strategy()
    logger: Logger = getLogger('docr.hitl-sdk')
    suggestions_gateway: Optional[str] = SUGGESTIONS_GATEWAY
    confidence_threshold: Optional[Any] = None

    async def annotate_bboxes(
            self,
            document_type: str,
            document_id: str,
            image: np.ndarray,
            labels: Dict[str, List[Tuple[float, float, float, float]]],
    ) -> Dict[str, List[Tuple[float, float, float, float]]]:
        project = await handl.get_or_create_project(
            OperationType.bboxes,
            document_type=document_type,
            labels=list(labels.keys()),
        )
        pid = project['id']

        uid = str(uuid4())
        name = f'{document_type}__{document_id}__{uid}.jpg'

        img = BytesIO()
        Image.fromarray(image, "RGB").save(img, format='JPEG')
        content = img.getvalue()

        predict = json.dumps(labels)
        img = await handl.create_task(name, content, predict, pid)
        task_id = img['id']

        while True:
            results = await handl.get_results(pid)
            for result in results:
                if task_id == result['id']:
                    result = result['payload']['aabb']
                    logging.debug(result)
                    return result
            self.logger.info(f'HITL: wait for {name}')
            await asyncio.sleep(10)

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
        project = await handl.get_or_create_project(OperationType.ocr)
        pid = project['id']

        for task in tasks:
            if not task.images:
                continue
            if (
                    self.confidence_threshold is not None
                    and task.predict_confidence is not None
                    and task.predict_confidence >= self.confidence_threshold
            ):
                continue
            uid = str(uuid4())
            name = f'{document_type}__{document_id}__{task.field_name}__{uid}.jpg'
            content = concat_v(task.images)
            img = await handl.create_task(name, content, task.predict, pid)
            task_id = img['id']
            task.id = task_id
            task.created_at = datetime.utcnow()
            self.tasks[task_id] = task

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
            deadline_at: datetime = None,
    ) -> Optional[Dict[str, Any]]:
        if only_classify or integrity_check or not only_ocr:
            raise AssertionError('hitl with handl backend supports only_ocr=True mode only')

        project = await handl.get_or_create_project(OperationType.ocr)
        pid = project['id']

        uid = str(uuid4())
        name = f'{document_type}__{document_id}__{uid}.jpg'
        content = concat_v(images)
        img = await handl.create_task(name, content, '', pid)
        task_id = img['id']

        self.document = Task(
            id=task_id,
            document_type=document_type,
            document_id=document_id,
            created_at=datetime.utcnow(),
            image=images[0],
            images=images,
        )
        return self.document

    async def ocr_multiple(
            self,
            document_type: str,
            document_id: str,
            image: np.ndarray,
            labels: List[str],
    ) -> Dict[str, str]:
        project = await handl.get_or_create_project(
            OperationType.ocr_multiple,
            document_type=document_type,
            labels=labels,
        )
        pid = project['id']

        uid = str(uuid4())
        name = f'{document_type}__{document_id}__{uid}.jpg'

        img = BytesIO()
        Image.fromarray(image, "RGB").save(img, format='JPEG')
        content = img.getvalue()

        img = await handl.create_task(name, content, '', pid)
        task_id = img['id']

        while True:
            results = await handl.get_results(pid)
            for result in results:
                if task_id == result['id']:
                    result = result['payload']['ocrs']
                    logging.debug(result)
                    return result
            self.logger.info(f'HITL: wait for {name}')
            await asyncio.sleep(10)

    async def _sync_task(self, results: List[Dict[str, Any]], task: Task) -> List[Task]:
        if not task.completed_at:
            lifetime = (datetime.utcnow() - task.created_at).total_seconds()
            if lifetime > HANDL_TASK_TIMEOUT:
                task.state = 'timeout'

        for result in results:
            task_id = result['id']
            if task_id in task.id:
                task.result = result['payload']['text']
                task.completed_at = datetime.utcnow().isoformat()
                return [task]

        return []

    async def sync_document(self):
        try:
            project = await handl.get_or_create_project(OperationType.ocr)
            pid = project['id']

            results = await handl.get_results(pid)

            return await self._sync_task(results, self.document)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(e)
            return []

    async def sync_tasks(self) -> bool:
        try:
            project = await handl.get_or_create_project(OperationType.ocr)
            pid = project['id']

            results = await handl.get_results(pid)

            res = []
            for task in self.tasks.values():
                res.extend(await self._sync_task(results, task))

            return bool(res)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(e)
            return False

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
