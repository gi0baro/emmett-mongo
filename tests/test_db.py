# -*- coding: utf-8 -*-

import pytest


def test_cursor_count_sync(sync_db):
    with sync_db.connection():
        assert sync_db.test_sync.find({}).count() == 0


@pytest.mark.asyncio
async def test_cursor_count_loop(loop_db):
    async with loop_db.connection():
        assert (await loop_db.test_loop.find({}).count()) == 0
