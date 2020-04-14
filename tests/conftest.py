# -*- coding: utf-8 -*-

import os
import pytest

from emmett import App
from emmett.asgi.loops import loops
from emmett_mongo import Database


@pytest.yield_fixture(scope='session')
def event_loop(request):
    loop = loops.get_loop('auto')
    yield loop
    loop.close()


@pytest.fixture(scope='session')
def app(event_loop):
    rv = App(__name__)
    rv.config.db.adapter = 'mongodb'
    rv.config.db.host = os.environ.get('MONGO_HOST', 'localhost')
    rv.config.db.port = int(os.environ.get('MONGO_PORT', '27017'))
    rv.config.db.database = os.environ.get('MONGO_DB', 'test')
    return rv


def _db_teardown_generator_sync(db):
    def teardown():
        with db.connection():
            for name in db.raw.list_collection_names():
                try:
                    db.raw.drop_collection(name)
                except Exception:
                    pass
    return teardown


def _db_teardown_generator_loop(db):
    async def teardown():
        async with db.connection():
            async for name in db.raw.list_collection_names():
                try:
                    await db.raw.drop_collection(name)
                except Exception:
                    pass
    return teardown


@pytest.fixture(scope='function')
def sync_db(request, app):
    rv = Database(app, policy='sync')
    rv.define_collections('test_sync')
    request.addfinalizer(_db_teardown_generator_sync(rv))
    return rv


@pytest.fixture(scope='function')
def loop_db(request, app):
    rv = Database(app, policy='loop')
    rv.define_collections('test_loop')
    request.addfinalizer(_db_teardown_generator_loop(rv))
    return rv
