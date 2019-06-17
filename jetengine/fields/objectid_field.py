from bson.objectid import ObjectId

from jetengine.fields.base_field import BaseField


class ObjectIdField(BaseField):
    """
    Field responsible for storing object ids.

    Usage:

    .. testcode:: modeling_fields

        objectid = ObjectIdField(required=True)
    """

    def validate(self, value):
        return value is None or isinstance(value, ObjectId)
