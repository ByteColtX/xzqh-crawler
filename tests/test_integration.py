import os
import pytest

from src.xzqh_crawler.client import XzqhClient
from src.xzqh_crawler.models import normalize_xzqh_code


pytestmark = pytest.mark.integration


def _pick_first_child(node):
    assert node is not None
    assert getattr(node, "children", None) is not None
    assert len(node.children) > 0
    return node.children[0]


@pytest.mark.skipif(
    os.environ.get("XZQH_INTEGRATION") != "1",
    reason="Real-network integration tests are opt-in. Set XZQH_INTEGRATION=1",
)
def test_real_network_can_fetch_level1_to_level4_minimal_chain():
    """End-to-end minimal chain using real dmfw.mca.gov.cn.

    Success criteria (per user): each level (1-4) is obtainable.
    We keep requests minimal: level1, then expand one branch down to level4.

    Pitfalls covered:
    - 直辖市可能不存在 level2（level1 直接返回 level3）
    - API code/parentCode 可能是 6/9/12 位且尾部补零，入库/关联应先 normalize
    """

    client = XzqhClient(timeout=15, max_retries=2, retry_delay=1.0)

    # Level 1: provinces
    root_l1 = client.get_tree_data(code="0", max_level=1)
    prov = _pick_first_child(root_l1)
    assert prov.level == 1

    # Level 2: cities under one province
    prov_l2 = client.get_tree_data(code=prov.code, max_level=2)
    city = _pick_first_child(prov_l2)

    # NOTE: Some provinces are "直辖市" (e.g., 北京), where the API may return
    # county-level nodes directly at maxLevel=2. So we allow either level 2 or 3 here,
    # and ensure we can still reach level 4.
    assert city.level in (2, 3)

    if city.level == 2:
        # Level 3: counties under one city
        city_l3 = client.get_tree_data(code=city.code, max_level=3)
        county = _pick_first_child(city_l3)
        assert county.level == 3
    else:
        # Already got a county-level node
        county = city

    # Level 4: townships under one county
    # Pitfall: some places may have missing level3 and go level2 -> level4 (e.g., 嘉峪关 6202)
    # The client method should accept either 4-digit(city) or 6-digit(county) parent.
    county_l4 = client.get_township_tree(county.code)
    town = _pick_first_child(county_l4)
    assert town.level == 4


@pytest.mark.skipif(
    os.environ.get("XZQH_INTEGRATION") != "1",
    reason="Real-network integration tests are opt-in. Set XZQH_INTEGRATION=1",
)
def test_real_network_city_can_directly_fetch_townships_for_special_case_jiayuguan():
    """Regression: 嘉峪关(6202) may be level2 -> level4 (no level3).

    Hard assertion (per user): upstream should return children for this parent,
    and all returned children must be level4.

    This test forces the exact pitfall we hit previously: treating parent as always 6-digit.
    """

    client = XzqhClient(timeout=15, max_retries=2, retry_delay=1.0)

    node = client.get_township_tree("6202")
    assert node is not None
    assert getattr(node, "children", None) is not None
    assert len(node.children) > 0
    assert all(ch.level == 4 for ch in node.children)
