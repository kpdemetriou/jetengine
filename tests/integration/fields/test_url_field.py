import asyncio
from preggy import expect
import mongoengine
from tests import async_test

import jetengine
from tests.integration.base import BaseIntegrationTest

COLLECTION = "IntegrationTestURLField"


class MongoDocument(mongoengine.Document):
    meta = {"collection": COLLECTION}
    url = mongoengine.URLField()


class MotorDocument(jetengine.Document):
    __collection__ = COLLECTION
    url = jetengine.URLField()


class TestURLField(BaseIntegrationTest):
    @async_test
    @asyncio.coroutine
    def test_can_integrate(self):
        mongo_document = MongoDocument(url="http://www.google.com/").save()

        result = yield from MotorDocument.objects.get(mongo_document.id)

        expect(result._id).to_equal(mongo_document.id)
        expect(result.url).to_equal(mongo_document.url)

    @async_test
    @asyncio.coroutine
    def test_can_integrate_backwards(self):
        motor_document = yield from MotorDocument.objects.create(url="http://www.wikipedia.org/")

        result = MongoDocument.objects.get(id=motor_document._id)

        expect(result.id).to_equal(motor_document._id)
        expect(result.url).to_equal(motor_document.url)
