import os

import pytest

from xzqh_crawler.client import XzqhClient

pytestmark = pytest.mark.integration


def _pick_first_child(node):
    assert node is not None
    assert getattr(node, "children", None) is not None
    assert len(node.children) > 0
    return node.children[0]


@pytest.mark.skipif(
    os.environ.get("XZQH_INTEGRATION") != "1",
    reason="真实网络集成测试默认关闭。设置 XZQH_INTEGRATION=1 后执行。",
)
@pytest.mark.asyncio
async def test_real_network_can_fetch_level1_to_level4_minimal_chain():
    async with XzqhClient(
        request_timeout=15,
        retry_attempts=2,
        retry_base_delay=1.0,
    ) as client:
        root_l1 = await client.fetch_tree(code="0", max_level=1)
        prov = _pick_first_child(root_l1)
        assert prov.level == 1

        prov_l2 = await client.fetch_tree(code=prov.code, max_level=2)
        city = _pick_first_child(prov_l2)
        assert city.level in (2, 3)

        if city.level == 2:
            city_l3 = await client.fetch_tree(code=city.code, max_level=3)
            county = _pick_first_child(city_l3)
            assert county.level == 3
        else:
            county = city

        county_l4 = await client.fetch_township_tree(county.code)
        town = _pick_first_child(county_l4)
        assert town.level == 4


@pytest.mark.skipif(
    os.environ.get("XZQH_INTEGRATION") != "1",
    reason="真实网络集成测试默认关闭。设置 XZQH_INTEGRATION=1 后执行。",
)
@pytest.mark.asyncio
async def test_real_network_city_can_directly_fetch_townships_for_special_case_jiayuguan():
    async with XzqhClient(
        request_timeout=15,
        retry_attempts=2,
        retry_base_delay=1.0,
    ) as client:
        node = await client.fetch_township_tree("6202")
        assert node is not None
        assert getattr(node, "children", None) is not None
        assert len(node.children) > 0
        assert all(child.level == 4 for child in node.children)
