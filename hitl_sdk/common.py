import base64
import datetime
from dataclasses import dataclass, field
from io import BytesIO
from typing import Iterable, List, Optional, Union

import dataclasses_json
import dateutil.parser
from PIL import Image

Value = Union[str, List[str]]


def concat_v(images: List[Union[bytes, str]]):
    imgs = []
    for i in images:
        if isinstance(i, str):
            i = base64.b64decode(i.encode())
        imfile = BytesIO(i)
        im = Image.open(imfile)
        imgs.append(im)

    width = max(i.width for i in imgs)
    height = sum(i.height for i in imgs)

    dst = Image.new('RGB', (width, height))

    y = 0
    for i in imgs:
        dst.paste(i, (0, y))
        y += i.height

    img = BytesIO()
    dst.save(img, format='JPEG')
    img = img.getvalue()

    return img


def default_retry_strategy() -> Iterable:
    return 5, 30


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
            decoder=lambda x: dateutil.parser.parse(x) if x else None
        ),
    )
    completed_at: Optional[datetime.datetime] = field(
        default=None,
        metadata=dataclasses_json.config(
            decoder=lambda x: dateutil.parser.parse(x) if x else None
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
    is_checkbox_array: bool = False
    code: Optional[str] = None

    def __post_init__(self):
        if self.image:
            self.images.append(self.image)

    def get_result(self) -> Optional[Value]:
        if self.completed_at:
            return self.result
        return self.result or self.predict or ''

    def is_timeout(self) -> bool:
        return self.state and 'timeout' in self.state
