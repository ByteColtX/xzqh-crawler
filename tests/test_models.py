from xzqh_crawler.models import TreeNode


def test_tree_node_from_api_data_handles_none_children():
    node = TreeNode.from_api_data(
        {
            "code": "11",
            "name": "北京市",
            "level": 1,
            "children": None,
        },
    )

    assert node.code == "11"
    assert node.children == []


def test_flatten_preserves_short_codes_and_parent_chain():
    root = TreeNode.from_api_data(
        {
            "code": "11",
            "name": "北京市",
            "level": 1,
            "children": [
                {
                    "code": "110101",
                    "name": "东城区",
                    "level": 3,
                    "children": [
                        {
                            "code": "110101001",
                            "name": "景山街道",
                            "level": 4,
                            "children": [],
                        },
                    ],
                },
            ],
        },
    )

    divisions = root.flatten()
    assert [division.code for division in divisions] == [
        "11",
        "110101",
        "110101001",
    ]
    assert divisions[1].parent_code == "11"
    assert divisions[2].parent_code == "110101"
    assert divisions[2].name_path == "北京市/东城区/景山街道"
