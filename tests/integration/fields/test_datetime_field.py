import asyncio
from datetime import datetime

from preggy import expect
import mongoengine
from tests import async_test

import jetengine
from tests.integration.base import BaseIntegrationTest


class MongoDocument(mongoengine.Document):
    meta = {"collection": "IntegrationTestDateTimeField"}
    date = mongoengine.DateTimeField()


class MotorDocument(jetengine.Document):
    __collection__ = "IntegrationTestDateTimeField"
    date = jetengine.DateTimeField()


class TestDatetimeField(BaseIntegrationTest):
    @async_test
    @asyncio.coroutine
    def test_can_integrate(self):
        mongo_document = MongoDocument(date=datetime.now()).save()

        result = yield from MotorDocument.objects.get(mongo_document.id)

        expect(result._id).to_equal(mongo_document.id)
        expect(result.date).to_be_like(mongo_document.date)

    @async_test
    @asyncio.coroutine
    def test_can_integrate_backwards(self):
        motor_document = yield from MotorDocument.objects.create(date=datetime.now())

        result = MongoDocument.objects.get(id=motor_document._id)

        expect(result.id).to_equal(motor_document._id)
        expect(result.date).to_be_like(motor_document.date)
