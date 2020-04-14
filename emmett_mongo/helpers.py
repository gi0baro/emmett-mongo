# -*- coding: utf-8 -*-
"""
    emmett_mongo.helpers
    --------------------

    Provides helpers for Mongo extension

    :copyright: 2019 Giovanni Barillari
    :license: BSD-3-Clause
"""

class ExtModule:
    def __init__(self, ext):
        self.ext = ext

    @property
    def app(self):
        return self.ext.app

    @property
    def db(self):
        return self.ext.db

    @property
    def db_ops(self):
        return self.ext.db_ops


def _build_migrate_cmd(ext):
    def migrate():
        ext.migrations.apply_migrations()
        ext.indexes.apply_indexes()
    return migrate
