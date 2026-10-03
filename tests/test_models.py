from xzqh_crawler.models import ApiDivisionNode, Division, DivisionCode, DivisionLevel


def test_api_node_flattens_without_display_redundancy():
    node = ApiDivisionNode.from_api_data({
        "code": "11", "name": "北京市", "level": 1,
        "children": [{"code": "110101", "name": "东城区", "level": 3, "children": [{"code": "110101001", "name": "景山街道", "level": 4, "children": []}]}],
    })
    divisions = node.flatten()
    assert [item.code for item in divisions] == ["11", "110101", "110101001"]
    assert divisions[2].parent_code == "110101"
    assert not hasattr(divisions[2], "name_path")


def test_division_code_and_level_validation():
    assert DivisionCode(" 1101 " ).province_code == "11"
    assert Division("1101", "测试市", DivisionLevel.CITY).validate()
    assert not Division("1101001", "错误", DivisionLevel.CITY).validate()
