from preggy import expect

from jetengine.query.lesser_than_or_equal import LesserThanOrEqualQueryOperator
from tests import AsyncTestCase


class TestLesserThanOrEqualQueryOperator(AsyncTestCase):
    def test_to_query(self):
        query = LesserThanOrEqualQueryOperator()
        expect(query).not_to_be_null()
        expect(query.to_query("field_name", 10)).to_be_like({"field_name": {"$lte": 10}})
