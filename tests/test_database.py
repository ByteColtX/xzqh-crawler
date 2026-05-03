import pytest

from xzqh_crawler.database import CrawlJobWrite, Database
from xzqh_crawler.models import AdministrativeDivision


@pytest.mark.asyncio
async def test_database_generates_jobs_and_applies_writes(tmp_path):
    db_path = tmp_path / "xzqh.db"
    async with Database(str(db_path)) as db:
        await db.save_divisions(
            [
                AdministrativeDivision(code="11", name="北京市", level=1),
                AdministrativeDivision(
                    code="110101",
                    name="东城区",
                    level=3,
                    parent_code="11",
                    parent_name="北京市",
                    name_path="北京市/东城区",
                ),
                AdministrativeDivision(code="62", name="甘肃省", level=1),
                AdministrativeDivision(
                    code="6202",
                    name="嘉峪关市",
                    level=2,
                    parent_code="62",
                    parent_name="甘肃省",
                    name_path="甘肃省/嘉峪关市",
                ),
            ],
        )

        parent_codes = await db.get_parent_codes_for_townships()
        assert parent_codes == ["110101", "6202"]

        inserted = await db.ensure_crawl_jobs(parent_codes)
        assert inserted == 2

        saved = await db.apply_crawl_job_writes(
            (
                CrawlJobWrite(
                    parent_code="110101",
                    state="ok",
                    divisions=(
                        AdministrativeDivision(
                            code="110101001",
                            name="景山街道",
                            level=4,
                            parent_code="110101",
                            parent_name="东城区",
                            name_path="北京市/东城区/景山街道",
                        ),
                    ),
                ),
                CrawlJobWrite(
                    parent_code="6202",
                    state="failed",
                    last_error="upstream timeout",
                ),
            ),
        )
        assert saved == 1

        division = await db.get_division("110101001")
        assert division is not None
        assert division.parent_code == "110101"

        stats = await db.get_statistics()
        assert stats["level_4"] == 1
        assert stats["total"] == 5

        job_counts = await db.get_crawl_job_counts()
        assert job_counts == {"pending": 0, "ok": 1, "failed": 1}


@pytest.mark.asyncio
async def test_database_filters_non_numeric_codes_on_insert_and_writer(tmp_path):
    db_path = tmp_path / "xzqh.db"
    async with Database(str(db_path)) as db:
        saved = await db.save_divisions(
            [
                AdministrativeDivision(code="11", name="北京市", level=1),
                AdministrativeDivision(code="TW", name="台湾省", level=1),
            ],
        )
        assert saved == 1
        assert await db.get_division("11") is not None
        assert await db.get_division("TW") is None

        await db.ensure_crawl_jobs(["110101"])
        saved = await db.apply_crawl_job_writes(
            (
                CrawlJobWrite(
                    parent_code="110101",
                    state="ok",
                    divisions=(
                        AdministrativeDivision(
                            code="110101001",
                            name="景山街道",
                            level=4,
                            parent_code="110101",
                        ),
                        AdministrativeDivision(
                            code="TW-001",
                            name="脏乡级记录",
                            level=4,
                            parent_code="110101",
                        ),
                    ),
                ),
            ),
        )
        assert saved == 1
        assert await db.get_division("110101001") is not None
        assert await db.get_division("TW-001") is None
