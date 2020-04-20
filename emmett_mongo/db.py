# -*- coding: utf-8 -*-
"""
    emmett_mongo.db
    -------------------

    Provides database facilities for Mongo extension

    :copyright: 2019 Giovanni Barillari
    :license: BSD-3-Clause
"""

import asyncio
import pymongo

from emmett.orm._patches import BaseAdapter
from emmett.orm.adapters import adapters
from emmett.orm.base import (
    Database as _Database
)
from emmett.orm.connection import (
    ConnectionState as _ConnectionState,
    ConnectionStateCtl as _ConnectionStateCtl,
    PooledConnectionManager as _PooledConnectionManager
)
from emmett.utils import cachedprop
from motor import motor_asyncio
from pydal.dialects import dialects
from pydal.dialects.mongo import MongoDialect


class ConnectionState(_ConnectionState):
    __slots__ = list(_ConnectionState.__slots__) + ['_session']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._session = None


class ConnectionStateCtl(_ConnectionStateCtl):
    state_cls = ConnectionState

    @property
    def session(self):
        return self.ctx._session

    @session.setter
    def session(self, value):
        self.ctx._session = value


class PooledConnectionManager(_PooledConnectionManager):
    __slots__ = _PooledConnectionManager.__slots__ + ['_ioloop']
    state_cls = ConnectionStateCtl

    @cachedprop
    def _lock_loop(self):
        self._ioloop = asyncio.get_event_loop()
        return asyncio.Lock()

    def _connector_sync(self):
        return self.adapter.connector_sync()

    def _connector_loop(self):
        return self.adapter.connector_loop()


@adapters.register_for('mongodb')
class MongoAdapter(BaseAdapter):
    _connection_manager_cls = PooledConnectionManager

    def find_driver(self):
        self.driver_sync = pymongo
        self.driver_loop = motor_asyncio

    def connector_sync(self):
        conn = self.driver_sync.MongoClient(self.uri)[self._driver_db]
        conn.close = lambda: None
        return conn

    def connector_loop(self):
        conn = self.driver_loop.AsyncIOMotorClient(
            self.uri, io_loop=self._connection_manager._ioloop
        )[self._driver_db]
        conn.close = lambda: None
        return conn

    def _load_dependencies(self):
        self.dialect = dialects.get_for(self)
        self.parser = None
        self.representer = None

    def _initialize_(self, do_connect):
        super(MongoAdapter, self)._initialize_(do_connect)
        m = pymongo.uri_parser.parse_uri(self.uri)
        if isinstance(m, tuple):
            m = {"database": m[1]}
        if m.get('database') is None:
            raise SyntaxError("Database is required!")
        self._driver_db = m['database']

    @property
    def session(self):
        return self._connection_manager.state.session

    @session.setter
    def session(self, session):
        self._connection_manager.state.session = session

    def reconnect(self, with_transaction=False, reuse_if_open=False):
        if not self._connection_manager.state.closed:
            if reuse_if_open:
                return False
            raise RuntimeError('Connection already opened.')
        self.connection, _opened = self._connection_manager.connect_sync()
        self.session = self.connection.client.start_session(
            causal_consistency=True
        )
        return True

    def close(self, action='commit', really=False):
        is_open = not self._connection_manager.state.closed
        if not is_open:
            return is_open
        try:
            self.session.end_session()
            self._connection_manager.disconnect_sync(self.connection, really)
        finally:
            self.session = None
            self.connection = None
        return is_open

    async def reconnect_loop(self, with_transaction=False, reuse_if_open=False):
        if not self._connection_manager.state.closed:
            if reuse_if_open:
                return False
            raise RuntimeError('Connection already opened.')
        self.connection, _opened = await self._connection_manager.connect_loop()
        self.session = await self.connection.client.start_session(
            causal_consistency=True
        )
        return True

    async def close_loop(self, action='commit', really=False):
        is_open = not self._connection_manager.state.closed
        if not is_open:
            return is_open
        try:
            await self.session.end_session()
            await self._connection_manager.disconnect_loop(
                self.connection, really
            )
        finally:
            self.session = None
            self.connection = None
        return is_open

    def commit(self):
        pass

    def rollback(self):
        pass

    def begin(self, *args):
        pass


class Database(_Database):
    def __init__(self, app, **kwargs):
        self._defined_collections = {}
        super().__init__(app, **kwargs)

    @property
    def raw(self):
        return self._adapter.connection

    def define_collections(self, *names, collection_cls=None):
        collection_cls = collection_cls or Collection
        for name in names:
            self._defined_collections[name] = collection_cls(self, name)

    def __getattr__(self, collection_name):
        if collection_name in self._defined_collections:
            return self._defined_collections[collection_name]
        return super().__getattr__(collection_name)


class Collection:
    __slots__ = ['db', 'name']

    def __init__(self, db, name):
        self.db = db
        self.name = name

    @property
    def raw(self):
        return self.db._adapter.connection[self.name]

    def insert_one(
        self,
        document,
        **kwargs
    ):
        kwargs['session'] = self.db._adapter.session
        return self.raw.insert_one(document, **kwargs)

    def insert_many(
        self,
        documents,
        **kwargs
    ):
        kwargs['session'] = self.db._adapter.session
        return self.raw.insert_many(documents, **kwargs)

    def replace_one(
        self,
        filter,
        replacement,
        **kwargs
    ):
        kwargs['session'] = self.db._adapter.session
        return self.raw.replace_one(filter, replacement, **kwargs)

    def update_one(
        self,
        filter,
        update,
        **kwargs
    ):
        kwargs['session'] = self.db._adapter.session
        return self.raw.update_one(filter, update, **kwargs)

    def update_many(
        self,
        filter,
        update,
        **kwargs
    ):
        kwargs['session'] = self.db._adapter.session
        return self.raw.update_many(filter, update, **kwargs)

    def delete_one(
        self,
        filter,
        **kwargs
    ):
        kwargs['session'] = self.db._adapter.session
        return self.raw.delete_one(filter, **kwargs)

    def delete_many(
        self,
        filter,
        **kwargs
    ):
        kwargs['session'] = self.db._adapter.session
        return self.raw.delete_many(filter, **kwargs)

    def aggregate(
        self,
        pipeline,
        **kwargs
    ):
        kwargs.update(
            allowDiskUse=kwargs.get('allowDiskUse', True),
            session=self.db._adapter.session
        )
        return self.raw.aggregate(pipeline, **kwargs)

    def find(self, *args, **kwargs):
        kwargs['session'] = self.db._adapter.session
        return self.raw.find(*args, **kwargs)

    def find_one(self, *args, **kwargs):
        kwargs['session'] = self.db._adapter.session
        return self.raw.find_one(*args, **kwargs)

    def find_one_and_delete(self, filter, **kwargs):
        kwargs['session'] = self.db._adapter.session
        return self.raw.find_one_and_delete(filter, **kwargs)

    def find_one_and_replace(self, filter, replacement, **kwargs):
        kwargs.update(
            return_document=kwargs.get(
                'return_document', pymongo.collection.ReturnDocument.AFTER
            ),
            session=self.db._adapter.session
        )
        return self.raw.find_one_and_replace(
            filter,
            replacement,
            **kwargs
        )

    def find_one_and_update(self, filter, update, **kwargs):
        kwargs.update(
            return_document=kwargs.get(
                'return_document', pymongo.collection.ReturnDocument.AFTER
            ),
            session=self.db._adapter.session
        )
        return self.raw.find_one_and_update(
            filter,
            update,
            **kwargs
        )

    def count_documents(self, filter, **kwargs):
        kwargs['session'] = self.db._adapter.session
        return self.raw.count_documents(filter, **kwargs)

    def distinct(self, key, filter=None, **kwargs):
        kwargs['session'] = self.db._adapter.session
        return self.raw.distinct(key, filter=filter, **kwargs)

    def __getattr__(self, name):
        return getattr(self.raw, name)


dialects.register_for(MongoAdapter)(MongoDialect)
