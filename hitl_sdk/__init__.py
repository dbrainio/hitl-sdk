from .common import Task
from .env import HITL_BACKEND
from .handl.sdk import SDK as HandlSDK
from .toloka.sdk import SDK as TolokaSDK

if HITL_BACKEND == 'toloka':
    SDK = TolokaSDK
elif HITL_BACKEND == 'handl':
    SDK = HandlSDK
else:
    raise AssertionError('HITL_BACKEND env value is not in list: [toloka, handl]')
