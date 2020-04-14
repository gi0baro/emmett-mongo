# -*- coding: utf-8 -*-
"""
    emmett_mongo.ext
    ----------------

    Provides Mongo extension for Emmett

    :copyright: 2019 Giovanni Barillari
    :license: BSD-3-Clause
"""

import os

from emmett.datastructures import sdict
from emmett.extensions import Extension, listen_signal

from .db import SyncDatabase
from .helpers import _build_migrate_cmd
from .indexes import Indexes
from .migrations import Migrations


class Mongo(Extension):
    default_config = {
        'migrations_folder': 'migrations',
        'indexes': []
    }

    def on_load(self):
        self.db = None
        self.db_ops = None
        self.migrations_collection = f"{self.app.name}_migrations"
        self.migrations_path = os.path.join(
            self.app.root_path, self.config.migrations_folder
        )
        self.indexes = Indexes(self)
        self.migrations = Migrations(self)
        self._configure_cli()

    @listen_signal('after_database')
    def bind_database(self, database):
        if self.db:
            return
        self.db = database
        self.db_ops = SyncDatabase(
            self.app, config=sdict(
                uri=f"mongodb://{database.config.uri.split('://')[-1]}"
            )
        )

    def _configure_cli(self):
        self.app.command('mongo_migrate')(_build_migrate_cmd(self))
