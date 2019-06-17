import asyncio
from decimal import Decimal

from preggy import expect
import mongoengine

import jetengine
from tests.integration.base import BaseIntegrationTest
from tests import async_test

COLLECTION = "IntegrationTestDecimalField"


class MongoDocument(mongoengine.Document):
    meta = {"collection": COLLECTION}
    number = mongoengine.DecimalField()


class MotorDocument(jetengine.Document):
    __collection__ = COLLECTION
    number = jetengine.DecimalField()


class TestIntField(BaseIntegrationTest):
    @async_test
    @asyncio.coroutine
    def test_can_integrate(self):
        mongo_document = MongoDocument(number=Decimal("10.53")).save()

        result = yield from MotorDocument.objects.get(mongo_document.id)

        expect(result._id).to_equal(mongo_document.id)
        expect(result.number).to_equal(mongo_document.number)

    @async_test
    @asyncio.coroutine
    def test_can_integrate_backwards(self):
        motor_document = yield from MotorDocument.objects.create(number=Decimal("10.53"))

        result = MongoDocument.objects.get(id=motor_document._id)

        expect(result.id).to_equal(motor_document._id)
        expect(result.number).to_equal(motor_document.number)
