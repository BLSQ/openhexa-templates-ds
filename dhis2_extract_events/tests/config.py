import datetime as dt

import polars as pl

# --- Test extract_events

programs_program_stages = [
    {
        "id": "id_A",
        "name": "Name A",
        "programStages": [
            {"id": "id_alpha", "name": "Name Alpha"},
            {"id": "id_beta", "name": "Name Beta"},
        ],
    },
    {
        "id": "id_B",
        "name": "Name B",
        "programStages": [
            {"id": "id_gamma", "name": "Name Gamma"},
            {"id": "id_delta", "name": "Name Delta"},
            {"id": "id_beta", "name": "Name Beta"},
        ],
    },
]

programs = [
    {"id": "id_A", "name": "Name A", "programType": "WITH_REGISTRATION"},
    {"id": "id_B", "name": "Name B", "programType": "WITHOUT_REGISTRATION"},
    {"id": "id_C", "name": "Name C", "programType": "WITH_REGISTRATION"},
]

event_pages_reduced = [
    {
        "page": 1,
        "total": 2,
        "pageCount": 2,
        "pageSize": 2,
        "instances": [
            {
                "event": "ZxljCbBJ3y0",
                "status": "COMPLETED",
                "program": "Fw4tCvSayjE",
                "programStage": "oDE5NYgmbNZ",
                "enrollment": "RdEl1r9DvvU",
                "trackedEntity": "qVopp6BwDUn",
                "orgUnit": "rdX5nU5lrcx",
                "occurredAt": "2025-07-10T00:00:00.000",
                "deleted": False,
                "attributeOptionCombo": "HllvX50cXC0",
                "dataValues": [  # 4 datavalues
                    {
                        "createdAt": "2025-11-04T09:38:45.457",
                        "updatedAt": "2025-11-04T09:38:54.924",
                        "providedElsewhere": False,
                        "dataElement": "RajRi5SueNA",
                        "value": "Cas acquis localement",
                        "createdBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                        "updatedBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                    },
                    {
                        "createdAt": "2025-11-04T09:38:07.994",
                        "updatedAt": "2025-11-04T09:38:54.924",
                        "providedElsewhere": False,
                        "dataElement": "tAoUjYf7udi",
                        "value": "false",
                        "createdBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                        "updatedBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                    },
                    {
                        "createdAt": "2025-11-04T09:37:43.849",
                        "updatedAt": "2025-11-04T09:38:54.924",
                        "providedElsewhere": False,
                        "dataElement": "ySFpQuE3eQp",
                        "value": "true",
                        "createdBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                        "updatedBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                    },
                    {
                        "createdAt": "2025-11-04T09:36:28.123",
                        "updatedAt": "2025-11-04T09:38:54.924",
                        "providedElsewhere": False,
                        "dataElement": "fnbqMUAGcZf",
                        "value": "Cultivateur",
                        "createdBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                        "updatedBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                    },
                ],
            },
            {
                "event": "D8hcmqDKJxd",
                "status": "COMPLETED",
                "program": "Fw4tCvSayjE",
                "programStage": "xk02Vu8vZ6l",
                "enrollment": "vJf79Vsaf3G",
                "trackedEntity": "KeZfDgfOhPa",
                "orgUnit": "a3GDyZM2uGO",
                "occurredAt": "2025-12-06T00:00:00.000",
                "deleted": False,
                "attributeOptionCombo": "HllvX50cXC0",
                "dataValues": [  # 6 datavalues
                    {
                        "createdAt": "2025-12-06T11:21:55.249",
                        "updatedAt": "2025-12-06T12:04:13.218",
                        "providedElsewhere": False,
                        "dataElement": "OS69VOLAdUq",
                        "value": "true",
                        "createdBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                        "updatedBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                    },
                    {
                        "createdAt": "2025-12-06T11:54:57.144",
                        "updatedAt": "2025-12-06T12:04:13.218",
                        "providedElsewhere": False,
                        "dataElement": "niV52i3sSow",
                        "value": "true",
                        "createdBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                        "updatedBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                    },
                    {
                        "createdAt": "2025-12-06T11:56:03.980",
                        "updatedAt": "2025-12-06T12:04:13.218",
                        "providedElsewhere": False,
                        "dataElement": "d1L9O3pWfu1",
                        "value": "false",
                        "createdBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                        "updatedBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                    },
                    {
                        "createdAt": "2025-12-06T11:22:17.847",
                        "updatedAt": "2025-12-06T12:04:13.218",
                        "providedElsewhere": False,
                        "dataElement": "pCv7sl6Jqxn",
                        "value": "false",
                        "createdBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                        "updatedBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                    },
                    {
                        "createdAt": "2025-12-06T11:21:48.684",
                        "updatedAt": "2025-12-06T12:04:13.218",
                        "providedElsewhere": False,
                        "dataElement": "IlFz06SpHu2",
                        "value": "true",
                        "createdBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                        "updatedBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                    },
                    {
                        "createdAt": "2025-12-06T11:21:14.207",
                        "updatedAt": "2025-12-06T12:04:13.218",
                        "providedElsewhere": False,
                        "dataElement": "aQ5wAWbcoRi",
                        "value": "Bujumbura",
                        "createdBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                        "updatedBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                    },
                ],
            },
            {
                "event": "ySc3DR803Ae",
                "status": "COMPLETED",
                "program": "Fw4tCvSayjE",
                "programStage": "EDLKq7Q4L2G",
                "enrollment": "vJf79Vsaf3G",
                "trackedEntity": "KeZfDgfOhPa",
                "orgUnit": "a3GDyZM2uGO",
                "occurredAt": "2025-12-06T00:00:00.000",
                "deleted": False,
                "attributeOptionCombo": "HllvX50cXC0",
                "dataValues": [  # 6 datavalues
                    {
                        "createdAt": "2025-12-06T12:06:15.850",
                        "updatedAt": "2025-12-06T12:08:18.179",
                        "providedElsewhere": False,
                        "dataElement": "tMvMLgZjtpZ",
                        "value": "true",
                        "createdBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                        "updatedBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                    },
                    {
                        "createdAt": "2025-12-06T12:06:39.974",
                        "updatedAt": "2025-12-06T12:08:18.179",
                        "providedElsewhere": False,
                        "dataElement": "HGyjdxcLZ5N",
                        "value": "Sévère",
                        "createdBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                        "updatedBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                    },
                    {
                        "createdAt": "2025-12-06T12:06:52.932",
                        "updatedAt": "2025-12-06T12:08:18.179",
                        "providedElsewhere": False,
                        "dataElement": "OX6WIu6goJm",
                        "value": "true",
                        "createdBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                        "updatedBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                    },
                    {
                        "createdAt": "2025-12-06T12:06:23.245",
                        "updatedAt": "2025-12-06T12:08:18.179",
                        "providedElsewhere": False,
                        "dataElement": "ZTgxnYgJP6v",
                        "value": "Hopital Militaire",
                        "createdBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                        "updatedBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                    },
                    {
                        "createdAt": "2025-12-06T12:07:20.486",
                        "updatedAt": "2025-12-06T12:08:18.179",
                        "providedElsewhere": False,
                        "dataElement": "dVlgtCWwyNv",
                        "value": "Doxycycline ",
                        "createdBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                        "updatedBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                    },
                    {
                        "createdAt": "2025-12-06T12:07:52.847",
                        "updatedAt": "2025-12-06T12:08:18.179",
                        "providedElsewhere": False,
                        "dataElement": "iiLYrgiclrr",
                        "value": "true",
                        "createdBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                        "updatedBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                    },
                ],
            },
            {
                "event": "SMLLx8YWJ8q",
                "status": "COMPLETED",
                "program": "Fw4tCvSayjE",
                "programStage": "F4yeVA4oLAH",
                "enrollment": "vJf79Vsaf3G",
                "trackedEntity": "KeZfDgfOhPa",
                "orgUnit": "a3GDyZM2uGO",
                "occurredAt": "2025-12-06T00:00:00.000",
                "deleted": False,
                "attributeOptionCombo": "HllvX50cXC0",
                "dataValues": [  # 3 datavalues
                    {
                        "createdAt": "2025-12-06T12:08:46.346",
                        "updatedAt": "2025-12-06T12:09:09.716",
                        "providedElsewhere": False,
                        "dataElement": "x9hhOJeS2hz",
                        "value": "Décédé",
                        "createdBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                        "updatedBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                    },
                    {
                        "createdAt": "2025-12-06T12:08:35.410",
                        "updatedAt": "2025-12-06T12:09:09.716",
                        "providedElsewhere": False,
                        "dataElement": "F6MGMXT1D3j",
                        "value": "2025-12-06",
                        "createdBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                        "updatedBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                    },
                    {
                        "createdAt": "2025-12-06T12:09:03.122",
                        "updatedAt": "2025-12-06T12:09:09.716",
                        "providedElsewhere": False,
                        "dataElement": "QuV77GwP62o",
                        "value": "2025-12-06",
                        "createdBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                        "updatedBy": {
                            "uid": "YXLSGvq4LSy",
                            "username": "ASIBOMANA",
                            "firstName": "Aimable",
                            "surname": "SIBOMANA ",
                        },
                    },
                ],
            },
        ],
    },
    {
        "page": 2,
        "total": 2,
        "pageCount": 2,
        "pageSize": 2,
        "instances": [
            {
                "event": "MAgY4N9DvYb",
                "status": "ACTIVE",
                "program": "Fw4tCvSayjE",
                "programStage": "pN6e6cc6F3n",
                "enrollment": "FXjBNCcc5kW",
                "trackedEntity": "EvI67El74o2",
                "orgUnit": "A3u6arsTDmT",
                "occurredAt": "2025-11-04T00:00:00.000",
                "deleted": False,
                "attributeOptionCombo": "HllvX50cXC0",
                "dataValues": [  # 2 datavalues
                    {
                        "createdAt": "2025-11-04T09:41:46.588",
                        "updatedAt": "2025-11-04T09:41:46.588",
                        "providedElsewhere": False,
                        "dataElement": "tMvMLgZjtpZ",
                        "value": "true",
                        "createdBy": {
                            "uid": "vuAnzSNWuuI",
                            "username": "lionel_cousp",
                            "firstName": "Lionel",
                            "surname": "Gendebien",
                        },
                        "updatedBy": {
                            "uid": "vuAnzSNWuuI",
                            "username": "lionel_cousp",
                            "firstName": "Lionel",
                            "surname": "Gendebien",
                        },
                    },
                    {
                        "createdAt": "2025-11-04T09:31:40.911",
                        "updatedAt": "2025-11-04T09:31:40.911",
                        "providedElsewhere": False,
                        "dataElement": "g8kHZk5nk1f",
                        "value": "2025-11-04",
                        "createdBy": {
                            "uid": "vuAnzSNWuuI",
                            "username": "lionel_cousp",
                            "firstName": "Lionel",
                            "surname": "Gendebien",
                        },
                        "updatedBy": {
                            "uid": "vuAnzSNWuuI",
                            "username": "lionel_cousp",
                            "firstName": "Lionel",
                            "surname": "Gendebien",
                        },
                    },
                ],
            }
        ],
    },
]


event_id_values = (
    ["D8hcmqDKJxd"] * 6
    + ["MAgY4N9DvYb"] * 2
    + ["SMLLx8YWJ8q"] * 3
    + ["ZxljCbBJ3y0"] * 4
    + ["ySc3DR803Ae"] * 6
)

status_values = ["COMPLETED"] * 6 + ["ACTIVE"] * 2 + ["COMPLETED"] * (3 + 4 + 6)
program_id_values = ["Fw4tCvSayjE"] * (6 + 2 + 3 + 4 + 6)
program_stage_id_values = (
    ["xk02Vu8vZ6l"] * 6
    + ["pN6e6cc6F3n"] * 2
    + ["F4yeVA4oLAH"] * 3
    + ["oDE5NYgmbNZ"] * 4
    + ["EDLKq7Q4L2G"] * 6
)
enrollment_id_values = (
    ["vJf79Vsaf3G"] * 6
    + ["FXjBNCcc5kW"] * 2
    + ["vJf79Vsaf3G"] * 3
    + ["RdEl1r9DvvU"] * 4
    + ["vJf79Vsaf3G"] * 6
)
tracked_entity_id_values = (
    ["KeZfDgfOhPa"] * 6
    + ["EvI67El74o2"] * 2
    + ["KeZfDgfOhPa"] * 3
    + ["qVopp6BwDUn"] * 4
    + ["KeZfDgfOhPa"] * 6
)
org_unit_values = (
    ["a3GDyZM2uGO"] * 6
    + ["A3u6arsTDmT"] * 2
    + ["a3GDyZM2uGO"] * 3
    + ["rdX5nU5lrcx"] * 4
    + ["a3GDyZM2uGO"] * 6
)
occurred_at_values = (
    [dt.datetime(2025, 12, 6, 0, 0)] * 6
    + [dt.datetime(2025, 11, 4, 0, 0)] * 2
    + [dt.datetime(2025, 12, 6, 0, 0)] * 3
    + [dt.datetime(2025, 7, 10, 0, 0)] * 4
    + [dt.datetime(2025, 12, 6, 0, 0)] * 6
)
deleted_values = [False] * (6 + 2 + 3 + 4 + 6)
att_option_combo_id_values = ["HllvX50cXC0"] * (6 + 2 + 3 + 4 + 6)
data_element_id_values = (
    [
        "IlFz06SpHu2",
        "OS69VOLAdUq",
        "aQ5wAWbcoRi",
        "d1L9O3pWfu1",
        "niV52i3sSow",
        "pCv7sl6Jqxn",
    ]
    + ["g8kHZk5nk1f", "tMvMLgZjtpZ"]
    + ["F6MGMXT1D3j", "QuV77GwP62o", "x9hhOJeS2hz"]
    + ["RajRi5SueNA", "fnbqMUAGcZf", "tAoUjYf7udi", "ySFpQuE3eQp"]
    + ["HGyjdxcLZ5N", "OX6WIu6goJm", "ZTgxnYgJP6v", "dVlgtCWwyNv", "iiLYrgiclrr", "tMvMLgZjtpZ"]
)
value_values = (
    ["true", "true", "Bujumbura", "false", "true", "false"]
    + ["2025-11-04", "true"]
    + ["2025-12-06", "2025-12-06", "Décédé"]
    + ["Cas acquis localement", "Cultivateur", "false", "true"]
    + ["Sévère", "true", "Hopital Militaire", "Doxycycline ", "true", "true"]
)


# --- Test join_object_names

df = pl.DataFrame(
    {
        "data_element_id": ["id_1", "id_2", "id_3", "id_1", "id_1"],
        "organisation_unit_id": ["id_delta", "id_epsilon", "id_delta", "missing", "id_zeta"],
        "extra_col": ["extra_first", "extra_second", "extra_third", "extra_fourth", "extra_fifth"],
        "program_stage_id": ["id_a", "id_b", "id_a", "missing", "id_b"],
        "program_id": ["id_one", "missing", "id_one", "id_three", "id_two"],
        "value": [10, 20, 30, 40, 50],
    }
)
data_elements_metadata = pl.DataFrame(
    {
        "id": ["id_1", "id_2", "id_3"],
        "name": ["Name 1", "Name 2", "Name 3"],
    }
)
ous_metadata = pl.DataFrame(
    {
        "id": ["id_delta", "id_epsilon", "id_zeta"],
        "level_1_id": ["id_alpha", "id_alpha", "id_alpha"],
        "level_1_name": ["Name Alpha", "Name Alpha", "Name Alpha"],
        "level_2_id": ["id_beta", "id_gamma", "id_beta"],
        "level_2_name": ["Name Beta", "Name Gamma", "Name Beta"],
        "level_3_id": ["id_delta", "id_epsilon", "id_zeta"],
        "level_3_name": ["Name Delta", "Name Epsilon", "Name Zeta"],
    }
)
program_stages_metadata = pl.DataFrame(
    {
        "program_stage_id": ["id_a", "id_b"],
        "program_stage_name": ["Name A", "Name B"],
    }
)

programs_metadata = pl.DataFrame(
    {
        "id": ["id_one", "id_two", "id_three"],
        "name": ["Name One", "Name Two", "Name Three"],
    }
)
