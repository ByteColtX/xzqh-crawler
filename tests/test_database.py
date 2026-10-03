import pytest
from xzqh_crawler.database import Database, TaskWrite
from xzqh_crawler.models import Division, DivisionLevel, TaskState

@pytest.mark.asyncio
async def test_database_tasks_and_writes(tmp_path):
    async with Database(str(tmp_path / 'xzqh.db')) as db:
        await db.start_run()
        await db.save_divisions([
            Division('11', '北京市', 1),
            Division('1101', '北京市市辖区', 2, parent_code='11'),
            Division('110101', '东城区', 3, parent_code='1101'),
        ])
        parents = await db.get_task_parent_codes(DivisionLevel.TOWNSHIP)
        assert parents == ['1101', '110101']
        assert await db.ensure_tasks(parents, DivisionLevel.TOWNSHIP) == 2
        saved = await db.apply_task_writes([TaskWrite('110101', DivisionLevel.TOWNSHIP, TaskState.SUCCEEDED, (Division('110101001', '景山街道', 4, parent_code='110101'),))])
        assert saved == 1
        assert (await db.get_division('110101001')).parent_code == '110101'
        assert (await db.get_task_counts())['succeeded'] == 1

@pytest.mark.asyncio
async def test_database_rejects_invalid_codes(tmp_path):
    async with Database(str(tmp_path / 'xzqh.db')) as db:
        await db.start_run()
        assert await db.save_divisions([Division('11', '北京市', 1), Division('TW', '台湾省', 1)]) == 1
        assert await db.get_division('TW') is None
