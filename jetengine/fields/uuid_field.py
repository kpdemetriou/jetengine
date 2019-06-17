from uuid import UUID

from jetengine.fields.base_field import BaseField


class UUIDField(BaseField):
    """
    Field responsible for storing :py:class:`uuid.UUID`.

    Usage:

    .. testcode:: modeling_fields

        name = UUIDField(required=True)
    """

    def validate(self, value):
        if value is None:
            return True

        if isinstance(value, UUID):
            return True

        if isinstance(value, str):
            try:
                UUID(value)
                return True
            except ValueError:
                pass

        return False

    def is_empty(self, value):
        return value is None or str(value) == ""

    def to_son(self, value):
        if value is None:
            return None

        if isinstance(value, str):
            return UUID(value)

        return value
