from typing import Any, Dict


def get_ocr_spec(overlap: int = 1) -> Dict[str, Any]:
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
