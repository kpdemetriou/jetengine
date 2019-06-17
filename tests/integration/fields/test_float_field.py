import asyncio

from preggy import expect
import mongoengine
from tests import async_test

import jetengine
from tests.integration.base import BaseIntegrationTest

COLLECTION = "IntegrationTestFloatField"


class MongoDocument(mongoengine.Document):
    meta = {"collection": COLLECTION}
    number = mongoengine.FloatField()


class MotorDocument(jetengine.Document):
    __collection__ = COLLECTION
    number = jetengine.FloatField()


class TestFloatField(BaseIntegrationTest):
    @async_test
    @asyncio.coroutine
    def test_can_integrate(self):
        mongo_document = MongoDocument(number=10.5).save()

        result = yield from MotorDocument.objects.get(mongo_document.id)

        expect(result._id).to_equal(mongo_document.id)
        expect(result.number).to_equal(mongo_document.number)

    @async_test
    @asyncio.coroutine
    def test_can_integrate_backwards(self):
        motor_document = yield from MotorDocument.objects.create(number=10.5)

        result = MongoDocument.objects.get(id=motor_document._id)

        expect(result.id).to_equal(motor_document._id)
        expect(result.number).to_equal(motor_document.number)

    @async_test
    @asyncio.coroutine
    def test_empty_field(self):
        motor_document = yield from MotorDocument.objects.create()

        result = yield from MotorDocument.objects.get(id=motor_document._id)

        expect(result).not_to_be_null()
        expect(result.number).to_be_null()
