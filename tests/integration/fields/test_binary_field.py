import asyncio
import six
from preggy import expect
import mongoengine

import jetengine
from tests.integration.base import BaseIntegrationTest
from tests import async_test

COLLECTION = "IntegrationTestBinaryField"


class MongoDocument(mongoengine.Document):
    meta = {"collection": COLLECTION}
    byte = mongoengine.BinaryField()


class MotorDocument(jetengine.Document):
    __collection__ = COLLECTION
    byte = jetengine.BinaryField()


class TestBinaryField(BaseIntegrationTest):
    @async_test
    @asyncio.coroutine
    def test_can_integrate(self):
        mongo_document = MongoDocument(byte=six.b("some_string")).save()

        result = yield from MotorDocument.objects.get(mongo_document.id)

        expect(result._id).to_equal(mongo_document.id)
        expect(result.byte).to_equal(mongo_document.byte)

    @async_test
    @asyncio.coroutine
    def test_can_integrate_backwards(self):
        motor_document = yield from MotorDocument.objects.create(byte=six.b("other_string"))

        result = MongoDocument.objects.get(id=motor_document._id)

        expect(result.id).to_equal(motor_document._id)
        expect(result.byte).to_equal(motor_document.byte)
