import asyncio
from uuid import uuid4

from preggy import expect
import mongoengine
from tests import async_test

import jetengine
from tests.integration.base import BaseIntegrationTest


class MongoDocument(mongoengine.Document):
    meta = {"collection": "IntegrationTestUUIDField"}
    uuid = mongoengine.UUIDField()


class MotorDocument(jetengine.Document):
    __collection__ = "IntegrationTestUUIDField"
    uuid = jetengine.UUIDField()


class TestUUIDField(BaseIntegrationTest):
    @async_test
    @asyncio.coroutine
    def test_can_integrate(self):
        mongo_document = MongoDocument(uuid=uuid4()).save()

        result = yield from MotorDocument.objects.get(mongo_document.id)

        expect(result._id).to_equal(mongo_document.id)
        expect(result.uuid).to_equal(mongo_document.uuid)

    @async_test
    @asyncio.coroutine
    def test_can_integrate_backwards(self):
        motor_document = yield from MotorDocument.objects.create(uuid=uuid4())

        result = MongoDocument.objects.get(id=motor_document._id)

        expect(result.id).to_equal(motor_document._id)
        expect(result.uuid).to_equal(motor_document.uuid)

    @async_test
    @asyncio.coroutine
    def test_can_filter_properly(self):
        yield from MotorDocument.objects.create(uuid=uuid4())
        yield from MotorDocument.objects.create(uuid=uuid4())
        motor_document = yield from MotorDocument.objects.create(uuid=uuid4())

        results = yield from MotorDocument.objects.filter(uuid=motor_document.uuid).find_all()
        expect(results).to_length(1)
        expect(results[0]._id).to_equal(motor_document._id)

    @async_test
    @asyncio.coroutine
    def test_empty_field(self):
        motor_document = yield from MotorDocument.objects.create()

        result = yield from MotorDocument.objects.get(id=motor_document._id)

        expect(result).not_to_be_null()
        expect(result.uuid).to_be_null()
