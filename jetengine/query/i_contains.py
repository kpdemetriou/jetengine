from jetengine.query.base import QueryOperator


class IContainsOperator(QueryOperator):
    """
    Query operator used to return all documents which specified field contains a string equal to a passed value.

    It is not case sensitive.

    For more information on `$regex` go to https://docs.mongodb.org/manual/reference/operator/query/regex/

    Usage:

    .. testsetup:: icontains_query_operator

        from datetime import datetime

        import tornado.ioloop

        from jetengine import *

    .. testcode:: icontains_query_operator

        class User(Document):
            first_name = StringField()

        query = Q(first_name__icontains='NaR')

        query_result = query.to_query(User)

        # Due to dict ordering
        print('{"first_name": {"$options": "%s", "$regex": "%s"}}' % (
            query_result['first_name']['$options'],
            query_result['first_name']['$regex'],
        ))

    The resulting regex is:

    .. testoutput:: icontains_query_operator

        {"first_name": {"$options": "i", "$regex": "NaR"}}
    """

    def to_query(self, field_name, value):
        return {field_name: {"$regex": r"%s" % value, "$options": "i"}}