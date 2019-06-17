from jetengine.fields.base_field import BaseField
from jetengine.utils import serialize, deserialize


class JsonField(BaseField):
    """
    Field responsible for storing json objects.

    Usage:

    .. testcode:: modeling_fields

        name = JsonField(required=True)

    Available arguments (apart from those in `BaseField`): `None`

    .. note ::

        If ujson is available, JetEngine will try to use it.
        Otherwise it will fallback to the json serializer that comes with python.
    """

    def validate(self, value):
        if value is None:
            return True

        try:
            serialize(value)
            return True
        except:
            return False

    def to_son(self, value):
        if value is None:
            return None

        return serialize(value)

    def from_son(self, value):
        if value is None:
            return None

        return deserialize(value)
