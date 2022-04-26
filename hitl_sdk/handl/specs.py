from typing import Any, Dict, List


def get_ocr_spec(overlap: int = 1, **_) -> Dict[str, Any]:
    spec = {
        "template": "ocr",
        "overlap": overlap,
        "input_spec": {
            "image": {
                "type": "url"
            },
            "text": {  # custom
                "type": "string",
                "default": {
                    "type": "l7d"
                }
            },
            "caption": {
                "type": "string",
                "default": {
                    "type": "l7d",
                    "en-us": "Перепишите текст с картинки",
                    "ru-ru": "Перепишите текст с картинки"
                }
            },
            "description": {
                "type": "string",
                "default": {
                    "type": "l7d",
                    "en-us": "Перепишите текст с картинки со знаками препинания. Учитывайте регистр — если буквы маленькие, вводим маленькие, большие переписываем большими. Важно — пробелов быть не должно, но если вам вдруг попалась картинка с раздельным текстом - то пишите через пробел.",
                    "ru-ru": "Перепишите текст с картинки со знаками препинания. Учитывайте регистр — если буквы маленькие, вводим маленькие, большие переписываем большими. Важно — пробелов быть не должно, но если вам вдруг попалась картинка с раздельным текстом - то пишите через пробел."
                }
            },
            "full_description": {
                "type": "string",
                "default": {
                    "type": "l7d"
                }
            }
        },
        "output_spec": {
            "text": {
                "type": "string"
            }
        }
    }
    return spec


def get_bboxes_spec(labels: List[str], overlap: int = 1, **_) -> Dict[str, Any]:
    values = [
        {
            "text": {
                "type": "l7d",
                "en-us": label,
                "ru-ru": label,
            },
            "description": {
                "type": "l7d",
                "en-us": label,
                "ru-ru": label
            },
            "value": label
        }
        for label in labels
    ]

    spec = {
        "template": "aabb",
        "overlap": overlap,
        "input_spec": {
            "image": {
                "type": "url"
            },
            "text": {  # custom
                "type": "string",
                "default": {
                    "type": "l7d"
                }
            },
            "caption": {
                "type": "string",
                "default": {
                    "type": "l7d",
                    "en-us": "Crop document fields",
                    "ru-ru": "Выделите поля документа"
                }
            },
            "description": {
                "type": "string",
                "default": {
                    "type": "l7d",
                    "en-us": "Crop document fields:",
                    "ru-ru": "Выделите поля документа:"
                }
            },
            "full_description": {
                "type": "string",
                "default": {
                    "type": "l7d"
                }
            }
        },
        "output_spec": {
            "aabb": {
                "keys": {
                    "type": "string",
                    "values": values,
                },
                "type": "map",
                "items": {
                    "type": "array",
                    "items": {
                        "type": "polygon"
                    }
                }
            }
        }
    }
    return spec