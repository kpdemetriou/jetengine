import asyncio
from preggy import expect
import mongoengine
from tests import async_test

import jetengine
from tests.integration.base import BaseIntegrationTest

COLLECTION = "IntegrationTestEmailField"


class MongoDocument(mongoengine.Document):
    meta = {"collection": COLLECTION}
    email = mongoengine.EmailField()


class MotorDocument(jetengine.Document):
    __collection__ = COLLECTION
    email = jetengine.EmailField()


class TestEmailField(BaseIntegrationTest):
    @async_test
    @asyncio.coroutine
    def test_can_integrate(self):
        mongo_document = MongoDocument(email="someone@gmail.com").save()

        result = yield from MotorDocument.objects.get(mongo_document.id)

        expect(result._id).to_equal(mongo_document.id)
        expect(result.email).to_equal(mongo_document.email)

    @async_test
    @asyncio.coroutine
    def test_can_integrate_backwards(self):
        motor_document = yield from MotorDocument.objects.create(email="someone@gmail.com")

        result = MongoDocument.objects.get(id=motor_document._id)

        expect(result.id).to_equal(motor_document._id)
        expect(result.email).to_equal(motor_document.email)
