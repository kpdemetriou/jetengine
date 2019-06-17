import asyncio
from preggy import expect
import mongoengine
from tests import async_test

import jetengine
from tests.integration.base import BaseIntegrationTest


class MongoDocument(mongoengine.Document):
    meta = {"collection": "IntegrationTestBooleanField"}
    is_active = mongoengine.BooleanField()


class MotorDocument(jetengine.Document):
    __collection__ = "IntegrationTestBooleanField"
    is_active = jetengine.BooleanField()


class TestBooleanField(BaseIntegrationTest):
    @async_test
    @asyncio.coroutine
    def test_can_integrate(self):
        mongo_document = MongoDocument(is_active=True).save()

        result = yield from MotorDocument.objects.get(mongo_document.id)

        expect(result._id).to_equal(mongo_document.id)
        expect(result.is_active).to_be_true()

    @async_test
    @asyncio.coroutine
    def test_can_integrate_backwards(self):
        motor_document = yield from MotorDocument.objects.create(is_active=True)

        result = MongoDocument.objects.get(id=motor_document._id)

        expect(result.id).to_equal(motor_document._id)
        expect(result.is_active).to_be_true()
