import json
from jetengine.fields.base_field import BaseField


class DictField(BaseField):
    def to_son(self, value):
        return value

    def from_son(self, value):
        return value

    def validate(self, value):
        if value is None:
            return True

        if isinstance(value, dict):
            for key in value.keys():
                if not isinstance(key, str):
                    return False
            return True
        if isinstance(value, list):
            return True

        try:
            json.dumps(value)
        except TypeError:
            return False

        return True
