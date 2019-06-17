import asyncio
from datetime import datetime
from preggy import expect

from jetengine import Document, DateTimeField, EmbeddedDocumentField
from tests import AsyncTestCase, async_test


class TestDateTimeField(AsyncTestCase):
    @async_test
    @asyncio.coroutine
    def test_document_with_auto_insert_datetime_field(self):
        class Model(Document):
            created = DateTimeField(auto_now_on_insert=True, auto_now_on_update=False)

        yield from self.drop_coll_async(Model.__collection__)

        model = Model()

        expect(model.created).to_be_null()

        yield from model.save()
        created = model.created

        expect(isinstance(created, datetime)).to_be_true()
        expect(created).to_equal(model.created)

        now_before = datetime.now()
        model = yield from Model.objects.create()
        created = model.created

        expect(created).to_equal(model.created)

        now_after = datetime.now()

        expect(model._id).not_to_be_null()
        expect(model.created).to_be_greater_or_equal_to(now_before)
        expect(model.created).to_be_lesser_or_equal_to(now_after)

        yield from model.save()

        expect(created).to_equal(model.created)

    @async_test
    @asyncio.coroutine
    def test_document_with_auto_update_datetime_field(self):
        class Model(Document):
            created = DateTimeField(auto_now_on_insert=True, auto_now_on_update=True)

        yield from self.drop_coll_async(Model.__collection__)

        model = yield from Model.objects.create()

        model_id = model._id
        model = yield from Model.objects.get(model_id)

        created = model.created

        expect(created).to_equal(model.created)

        yield from model.save()
        now_after = datetime.now()

        expect(model.created).to_be_greater_or_equal_to(created)
        expect(model.created).to_be_lesser_or_equal_to(now_after)
        created = model.created
        expect(created).to_equal(model.created)

    @async_test
    @asyncio.coroutine
    def test_embedded_document_with_auto_insert_datetime_field(self):
        class Model(Document):
            created = DateTimeField(auto_now_on_insert=True, auto_now_on_update=False)

        class Doc(Document):
            embedded = EmbeddedDocumentField(embedded_document_type=Model, required=True)

        yield from self.drop_coll_async(Doc.__collection__)
        yield from self.drop_coll_async(Model.__collection__)

        doc = Doc(embedded=Model())

        expect(doc.embedded.created).to_be_null()

        yield from doc.save()

        expect(doc.embedded.created).not_to_be_null()
        expect(isinstance(doc.embedded.created, datetime)).to_be_true()

    @async_test
    @asyncio.coroutine
    def test_embedded_document_with_auto_update_datetime_field(self):
        class Model(Document):
            created = DateTimeField(auto_now_on_insert=True, auto_now_on_update=True)

        class Doc(Document):
            embedded = EmbeddedDocumentField(embedded_document_type=Model, required=False)

        yield from self.drop_coll_async(Doc.__collection__)
        yield from self.drop_coll_async(Model.__collection__)

        doc = yield from Doc.objects.create()

        expect(doc.embedded).to_be_null()

        doc = Doc(embedded=Model())

        expect(doc.embedded.created).to_be_null()

        now_before = datetime.now()
        yield from doc.save()
        now_after = datetime.now()

        expect(doc.embedded.created).not_to_be_null()
        expect(isinstance(doc.embedded.created, datetime)).to_be_true()
        expect(doc.embedded.created).to_be_greater_or_equal_to(now_before)
        expect(doc.embedded.created).to_be_lesser_or_equal_to(now_after)

        created = doc.embedded.created

        yield from doc.save()

        expect(doc.embedded.created).not_to_be_null()
        expect(isinstance(doc.embedded.created, datetime)).to_be_true()
        expect(doc.embedded.created).to_be_greater_than(created)
        expect(doc.embedded.created).to_be_lesser_or_equal_to(datetime.now())
