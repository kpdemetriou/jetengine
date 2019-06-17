from preggy import expect
import asyncio

from jetengine import connect
from tests import AsyncTestCase, async_test


class TestConnect(AsyncTestCase):
    def setUp(self):
        super(TestConnect, self).setUp(auto_connect=False)

    @async_test
    @asyncio.coroutine
    def test_can_connect_to_a_database(self):
        db = connect("test", host="localhost", port=27017, io_loop=self.io_loop)

        res = yield from db.ping()
        ping_result = res["ok"]
        expect(ping_result).to_equal(1.0)
