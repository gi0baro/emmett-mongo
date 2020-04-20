# -*- coding: utf-8 -*-
"""
    emmett_mongo.indexes
    --------------------

    Provides Mongo indexes facilities

    :copyright: 2019 Giovanni Barillari
    :license: BSD-3-Clause
"""

import pymongo

from collections import defaultdict
from hashlib import sha256
from pymongo.operations import IndexModel

from .helpers import ExtModule


class Indexes(ExtModule):
    @property
    def indexes(self):
        return self.ext.config.indexes

    @staticmethod
    def _build_index_name(collection, keys, unique):
        attr_str = 'idx[collection({!r}):keys({!r}):unique({!r})]'.format(
            collection, keys, unique)
        return 'mext_{}'.format(sha256(attr_str.encode('utf-8')).hexdigest())

    def parse_indexes(self, config):
        rv = {}
        for index in config:
            fields, collection, unique = (
                [], index['collection'], index.get('unique', False))
            for field in index['fields']:
                if isinstance(field, dict):
                    fields.append(
                        (field['name'], getattr(pymongo, field['type'])))
                else:
                    fields.append((field, pymongo.ASCENDING))
            name = self._build_index_name(collection, fields, unique)
            rv[name] = {'collection': collection, 'model': IndexModel(
                fields, name=name, unique=unique, background=True)}
        return rv

    def apply_indexes(self):
        print('> Applying indexes to mongo database..')
        indexes = self.parse_indexes(self.indexes)
        grouped_indexes = defaultdict(dict)
        for index_name, index_data in indexes.items():
            grouped_indexes[index_data['collection']][index_name] = \
                index_data
        to_del, to_add = {}, {}
        existing_indexes = {}
        with self.db.connection():
            for collection, collection_indexes in grouped_indexes.items():
                existing_names = set([
                    el['name']
                    for el in self.db.raw[collection].list_indexes()
                    if el['name'].startswith('mext_')
                ])
                existing_indexes[collection] = existing_names
                config_names = set(collection_indexes.keys())
                missing = config_names - existing_names
                useless = existing_names - config_names
                if missing:
                    to_add[collection] = list(missing)
                if useless:
                    to_del[collection] = list(useless)
            for collection, index_names in to_add.items():
                print(f'  Creating indexes for {collection}:')
                for name in index_names:
                    print(f'  - {name}')
                self.db.raw[collection].create_indexes([
                    grouped_indexes[collection][name]['model']
                    for name in index_names
                ])
            for collection, index_names in to_del.items():
                print(f'  Dropping indexes for {collection}:')
                for name in index_names:
                    print(f'  - {name}')
                    self.db.raw[collection].drop_index(name)
        print('> Indexes applied.')
