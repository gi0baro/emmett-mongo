# -*- coding: utf-8 -*-

import pytest


def test_cursor_count_sync(db):
    with db.connection():
        assert db.tests.find({}).count() == 0

    with db.connection():
        assert db.tests.find({}).count() == 0
        db.tests.insert_one({'env': 'sync'})
        assert db.tests.find({}).count() == 1

    with db.connection():
        assert db.tests.find({}).count() == 1


@pytest.mark.asyncio
async def test_cursor_count_loop(db):
    async with db.connection():
        assert (await db.tests.find({}).count()) == 0

    async with db.connection():
        assert (await db.tests.find({}).count()) == 0
        await db.tests.insert_one({'env': 'loop'})
        assert (await db.tests.find({}).count()) == 1

    async with db.connection():
        assert (await db.tests.find({}).count()) == 1
