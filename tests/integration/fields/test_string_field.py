import asyncio
from preggy import expect
import mongoengine
from tests import async_test

import jetengine
from tests.integration.base import BaseIntegrationTest


class MongoDocument(mongoengine.Document):
    meta = {"collection": "IntegrationTestStringField"}
    name = mongoengine.StringField()


class MotorDocument(jetengine.Document):
    __collection__ = "IntegrationTestStringField"
    name = jetengine.StringField()


class TestStringField(BaseIntegrationTest):
    @async_test
    @asyncio.coroutine
    def test_can_integrate(self):
        mongo_document = MongoDocument(name="some_string").save()

        result = yield from MotorDocument.objects.get(mongo_document.id)

        expect(result._id).to_equal(mongo_document.id)
        expect(result.name).to_equal(mongo_document.name)

    @async_test
    @asyncio.coroutine
    def test_can_integrate_backwards(self):
        motor_document = yield from MotorDocument.objects.create(name="other_string")

        result = MongoDocument.objects.get(id=motor_document._id)

        expect(result.id).to_equal(motor_document._id)
        expect(result.name).to_equal(motor_document.name)
