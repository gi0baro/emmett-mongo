# -*- coding: utf-8 -*-
"""
    emmett_mongo.migrations
    -----------------------

    Provides Mongo migrations facilities

    :copyright: 2019 Giovanni Barillari
    :license: BSD-3-Clause
"""

import os
import re
import sys

from .helpers import ExtModule


class Migration(object):
    match_rule = re.compile(r'(?!__init__)(.*\.py)$')

    def __init__(self, version, py_module):
        self.version = version
        self.py_module = py_module

    @property
    def name(self):
        return self.py_module

    def load(self):
        __import__(self.py_module)
        module = sys.modules[self.py_module]
        self.invoker = getattr(module, 'run', lambda *args, **kwargs: None)

    def invoke(self, db):
        self.invoker(db)

    @classmethod
    def _from_file(cls, mig_file):
        py_match = cls.match_rule.match(mig_file)
        if not py_match:
            return None
        py_filename = py_match.group(1)
        py_module = py_filename.split('.py')[0]
        try:
            version = int(py_module.split('_')[0])
        except Exception:
            return None
        version = str(version).zfill(4)
        return cls(version, py_module)


class Migrations(ExtModule):
    def apply_migration(self, migration):
        migration.load()
        migration.invoke(self.db)
        self.db[self.ext.migrations_collection].find_one_and_update(
            {'app_name': self.app.name},
            {'$set': {'current_migration': migration.version}},
            upsert=True
        )

    def get_current_migration(self):
        row = self.db[self.ext.migrations_collection].find_one(
            {'app_name': self.app.name}
        )
        return row['current_migration'] if row else None

    def load_migrations(self):
        if not os.path.exists(self.ext.migrations_path):
            return [], {}
        sys.path.insert(0, self.ext.migrations_path)
        rv = []
        for mig_file in os.listdir(os.path.abspath(self.ext.migrations_path)):
            mig = Migration._from_file(mig_file)
            if mig is None:
                continue
            rv.append(mig)
        rv = sorted(rv, key=lambda mig: mig.version)
        return rv, {
            mig.version: idx for idx, mig in enumerate(rv)}

    def apply_migrations(self):
        print('> Applying migrations to mongo database..')
        migrations, order_dict = self.load_migrations()
        if not migrations:
            print('> No migrations in app')
            return
        with self.db.connection():
            current_migration = self.get_current_migration()
            print(
                f'DB is at migration {current_migration}'
                if current_migration else
                ' DB has no active migrations'
            )
            idx = (
                0 if not current_migration else
                order_dict[current_migration] + 1)
            for migration in migrations[idx:]:
                print(f'- Applying migration {migration.name}')
                try:
                    self.apply_migration(migration)
                except Exception:
                    print(f'Migration {migration.name} FAILED')
                    raise
                print(f'Migration {migration.name} APPLIED')
        print('> Migrations applied.')
