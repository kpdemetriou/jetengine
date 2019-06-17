__version__ = "1.1.3"

try:
    from pymongo import ASCENDING, DESCENDING

    from jetengine.connection import connect, disconnect, get_connection
    from jetengine.document import Document

    from jetengine.fields import (
        BaseField,
        StringField,
        BooleanField,
        DateTimeField,
        UUIDField,
        ListField,
        EmbeddedDocumentField,
        ReferenceField,
        URLField,
        EmailField,
        IntField,
        FloatField,
        DecimalField,
        BinaryField,
        JsonField,
        ObjectIdField,
        DictField,
    )

    from jetengine.aggregation.base import Aggregation
    from jetengine.query_builder.node import Q, QNot

except ImportError as e:
    # likely setup.py trying to import version
    import sys
    import traceback

    traceback.print_exception(*sys.exc_info())
