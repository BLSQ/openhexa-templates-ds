org_units_expected_columns = [
        {
            "name": "level_1_id",
            "type": "String",
            "not null": True,
        },
        {
            "name": "level_1_name",
            "type": "String",
            "not null": True,
        },
        {
            "name": "level_2_id",
            "type": "String",
            "not null": True,
        },
        {
            "name": "level_2_name",
            "type": "String",
            "not null": True,
        },
        {
            "name": "level_3_id",
            "type": "String",
            "not null": True,
        },
        {
            "name": "level_3_name",
            "type": "String",
            "not null": True,
        },
        {
            "name": "level_4_id",
            "type": "String",
            "not null": True,
        },
        {
            "name": "level_4_name",
            "type": "String",
            "not null": True,
        }
    ]

org_unit_groups_expected_columns = [
        {
            "name": "id",
            "type": "String",
            "not null": True,
        },
        {
            "name": "name",
            "type": "String",
            "not null": True,
        },
        {
            "name": "organisation_units",
            "type": "String",
            "not null": True,
        }
]

retrieved_datasets_expected_columns = [
        {
            "name": "id",
            "type": "String",
            "not null": True,
        },
        {
            "name": "name",
            "type": "String",
            "not null": True,
        },
        {
            "name": "organisation_units",
            "type": "String",
            "not null": True,
        },
        {
            "name": "data_elements",
            "type": "String",
            "not null": True,
        },
        {
            "name": "indicators",
            "type": "String",
            "not null": False,
        },
        {
            "name": "period_type",
            "type": "String",
            "not null": True,
        }  
]

retrieved_data_elements_expected_columns = [
        {
            "name": "id",
            "type": "String",
            "not null": True,
        },
        {
            "name": "name",
            "type": "String",
            "not null": True,
        },
        {
            "name": "value_type",
            "type": "String",
            "not null": True,
        }
]

retrieved_data_element_groups_expected_columns = [
        {
            "name": "id",
            "type": "String",
            "not null": True,
        },
        {
            "name": "name",
            "type": "String",
            "not null": True,
        },
        {
            "name": "data_elements",
            "type": "String",
            "not null": False,
        }
]

retrieved_categorty_options_expected_columns = [
        {
            "name": "id",
            "type": "String",
            "not null": True,
        },
        {
            "name": "name",
            "type": "String",
            "not null": True,
        }
]