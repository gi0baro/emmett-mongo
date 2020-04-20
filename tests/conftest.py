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


def _db_teardown_generator(db):
    def teardown():
        with db.connection():
            for name in db.raw.list_collection_names():
                try:
                    db.raw.drop_collection(name)
                except Exception:
                    pass
    return teardown


@pytest.fixture(scope='function')
def db(request, app):
    rv = Database(app)
    rv.define_collections('tests')
    request.addfinalizer(_db_teardown_generator(rv))
    return rv
