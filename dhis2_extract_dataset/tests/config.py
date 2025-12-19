import polars as pl
from openhexa.toolbox.dhis2.periods import period_from_string

valid_dates = [
    "2023-01-15",
    "20230115",
    "2023W02",
]
invalid_dates = [
    "2023-13-01",
    "2023-00-10",
    "2023-01-32",
    "2023-02-30",
    "2023W532",
    "2023W00",
    "2023-1-5",
    "15-01-2023",
    "2023/01/15",
    "2023015",
    "2023W2",
]
valid_ous = ["ou1", "ou2"]
valid_ou_groups = ["oug1", "oug2"]
empty_ous = []
empty_ou_groups = []

datasets_pages_in = [
    {
        "pager": {"page": 1, "total": 3, "pageSize": 3, "pageCount": 2},
        "dataSets": [
            {
                "name": "first_dataset_name",
                "periodType": "Daily",
                "dataSetElements": [
                    {"dataSet": {"id": "first_dataset"}, "dataElement": {"id": "one"}},
                    {
                        "dataSet": {"id": "first_dataset"},
                        "dataElement": {"id": "two"},
                        "categoryCombo": {"id": "un"},
                    },
                ],
                "indicators": [],
                "id": "first_dataset",
                "organisationUnits": [
                    {"id": "ab"},
                    {"id": "bc"},
                    {"id": "cd"},
                ],
            },
            {
                "name": "second_dataset_name",
                "periodType": "Quarterly",
                "dataSetElements": [
                    {"dataSet": {"id": "second_dataset"}, "dataElement": {"id": "three"}},
                    {"dataSet": {"id": "second_dataset"}, "dataElement": {"id": "four"}},
                    {
                        "dataSet": {"id": "second_dataset"},
                        "dataElement": {"id": "five"},
                        "categoryCombo": {"id": "deux"},
                    },
                ],
                "indicators": [
                    {"id": "aa"},
                    {"id": "bb"},
                    {"id": "cc"},
                    {"id": "dd"},
                ],
                "id": "second_dataset",
                "organisationUnits": [
                    {"id": "ef"},
                    {"id": "fg"},
                    {"id": "gh"},
                    {"id": "hi"},
                    {"id": "ij"},
                ],
            },
            {
                "name": "third_dataset_name",
                "periodType": "Monthly",
                "dataSetElements": [
                    {"dataSet": {"id": "third_dataset"}, "dataElement": {"id": "six"}},
                    {"dataSet": {"id": "third_dataset"}, "dataElement": {"id": "seven"}},
                    {
                        "dataSet": {"id": "third_dataset"},
                        "dataElement": {"id": "eight"},
                        "categoryCombo": {"id": "trois"},
                    },
                    {"dataSet": {"id": "third_dataset"}, "dataElement": {"id": "nine"}},
                    {
                        "dataSet": {"id": "third_dataset"},
                        "dataElement": {"id": "ten"},
                        "categoryCombo": {"id": "quatre"},
                    },
                ],
                "indicators": [],
                "id": "third_dataset",
                "organisationUnits": [
                    {"id": "jk"},
                    {"id": "kl"},
                ],
            },
        ],
    },
    {
        "pager": {"page": 2, "total": 2, "pageSize": 3, "pageCount": 2},
        "dataSets": [
            {
                "name": "fourth_dataset_name",
                "periodType": "SixMonthly",
                "dataSetElements": [
                    {"dataSet": {"id": "fourth_dataset"}, "dataElement": {"id": "eleven"}}
                ],
                "indicators": [],
                "id": "fourth_dataset",
                "organisationUnits": [{"id": "mn"}],
            },
            {
                "name": "fifth_dataset_name",
                "periodType": "Yearly",
                "dataSetElements": [
                    {
                        "dataSet": {"id": "fifth_dataset"},
                        "dataElement": {"id": "twelve"},
                        "categoryCombo": {"id": "cinq"},
                    },
                    {
                        "dataSet": {"id": "fifth_dataset"},
                        "dataElement": {"id": "thirteen"},
                        "categoryCombo": {"id": "six"},
                    },
                    {
                        "dataSet": {"id": "fifth_dataset"},
                        "dataElement": {"id": "fourteen"},
                        "categoryCombo": {"id": "sept"},
                    },
                    {
                        "dataSet": {"id": "fifth_dataset"},
                        "dataElement": {"id": "fifteen"},
                        "categoryCombo": {"id": "huit"},
                    },
                    {"dataSet": {"id": "fifth_dataset"}, "dataElement": {"id": "sixteen"}},
                    {
                        "dataSet": {"id": "fifth_dataset"},
                        "dataElement": {"id": "seventeen"},
                        "categoryCombo": {"id": "neuf"},
                    },
                ],
                "indicators": [
                    {"id": "ee"},
                    {"id": "ff"},
                ],
                "id": "fifth_dataset",
                "organisationUnits": [
                    {"id": "lm"},
                ],
            },
        ],
    },
]


datasets_pages_out = {
    "first_dataset": {
        "name": "first_dataset_name",
        "data_elements": ["one", "two"],
        "indicators": [],
        "organisation_units": ["ab", "bc", "cd"],
        "periodType": "Daily",
    },
    "second_dataset": {
        "name": "second_dataset_name",
        "data_elements": ["three", "four", "five"],
        "indicators": ["aa", "bb", "cc", "dd"],
        "organisation_units": ["ef", "fg", "gh", "hi", "ij"],
        "periodType": "Quarterly",
    },
    "third_dataset": {
        "name": "third_dataset_name",
        "data_elements": ["six", "seven", "eight", "nine", "ten"],
        "indicators": [],
        "organisation_units": ["jk", "kl"],
        "periodType": "Monthly",
    },
    "fourth_dataset": {
        "name": "fourth_dataset_name",
        "data_elements": ["eleven"],
        "indicators": [],
        "organisation_units": ["mn"],
        "periodType": "SixMonthly",
    },
    "fifth_dataset": {
        "name": "fifth_dataset_name",
        "data_elements": ["twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen"],
        "indicators": ["ee", "ff"],
        "organisation_units": ["lm"],
        "periodType": "Yearly",
    },
}

before_add_cols = pl.DataFrame(
    {
        "period": [
            "20251231",  # Day
            "2025W1",  # Week
            "202512",  # Month
            "202501B",  # BiMonth
            "2025Q1",  # Quarter
            "2025S1",  # SixMonth
            "2025",  # Year
            "2025April",  # FinancialApril
            "2025July",  # FinancialJuly
            "2025Oct",  # FinancialOct
            "2025Nov",  # FinancialNov
            "2025WedW1",  # WeekWednesday
            "2025ThuW1",  # WeekThursday
            "2025SatW1",  # WeekSaturday
            "2025SunW1",  # WeekSunday
            "2025BiW1",  # BiWeek]
        ],
        "value": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160],
    }
)
dictionary_ds = {
    "ds1": {
        "name": "Test Dataset",
        "periodType": "Monthly",
    },
    "ds2": {
        "name": "Another Dataset",
        "periodType": "Yearly",
    },
}
after_add_cols = pl.DataFrame(
    {
        "period": [
            "20251231",  # Day
            "2025W1",  # Week
            "202512",  # Month
            "202501B",  # BiMonth
            "2025Q1",  # Quarter
            "2025S1",  # SixMonth
            "2025",  # Year
            "2025April",  # FinancialApril
            "2025July",  # FinancialJuly
            "2025Oct",  # FinancialOct
            "2025Nov",  # FinancialNov
            "2025WedW1",  # WeekWednesday
            "2025ThuW1",  # WeekThursday
            "2025SatW1",  # WeekSaturday
            "2025SunW1",  # WeekSunday
            "2025BiW1",  # BiWeek
        ],
        "value": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160],
        "dataset": ["Test Dataset"] * 16,
        "period_type_configured_dataset": ["Monthly"] * 16,
        "period_type_extracted": [
            "Day",
            "Week",
            "Month",
            "BiMonth",
            "Quarter",
            "SixMonth",
            "Year",
            "FinancialApril",
            "FinancialJuly",
            "FinancialOct",
            "FinancialNov",
            "WeekWednesday",
            "WeekThursday",
            "WeekSaturday",
            "WeekSunday",
            "BiWeek",
        ],
    }
)

data_with_periods_weird = pl.DataFrame(
    {
        "period": ["202311", "202301", "202305", "202306", "202308"],
        "value": [100, 200, 300, 400, 500],
        "data_element_id": ["de2", "de1", "de3", "de4", "de5"],
    }
)
data_with_periods_okey = pl.DataFrame(
    {
        "period": ["202304", "202305", "202306", "202307", "202308", "202309"],
        "value": [100, 200, 300, 400, 500, 600],
        "data_element_id": ["de7", "de7", "de3", "de4", "de5", "de6"],
    }
)
start = period_from_string("202304")
end = period_from_string("202309")
missing_periods = ["202304", "202307", "202309"]
extra_periods = ["202301", "202311"]
missing_des = ["de6", "de7"]
extra_des = ["de1", "de2"]
dictionary_ds_one = {
    "name": "Test Dataset",
    "periodType": "Monthly",
    "data_elements": ["de3", "de4", "de5", "de6", "de7"],
}


pyramid = pl.DataFrame(
    {
        "id": [
            "ou1",
            "ou2",
            "ou3",
            "ou4",
            "ou5",
            "ou6",
            "ou7",
            "ou8",
            "ou9",
            "ou10",
            "ou11",
            "ou12",
            "ou13",
            "ou14",
            "ou15",
            "ou16",
        ],
        "name": [
            "OU1",
            "OU2",
            "OU3",
            "OU4",
            "OU5",
            "OU6",
            "OU7",
            "OU8",
            "OU9",
            "OU10",
            "OU11",
            "OU12",
            "OU13",
            "OU14",
            "OU15",
            "OU16",
        ],
        "level": [1] + [2] * 2 + [3] * 3 + [4] * 4 + [5] * 6,
        "opening_date": [None] * 16,
        "closed_date": [None] * 16,
        "level_1_id": ["ou1"] * 16,
        "level_1_name": ["OU1"] * 16,
        "level_2_id": [None, "ou2", "ou3"]
        + ["ou2"] * 2
        + ["ou3"]
        + ["ou2"] * 3
        + ["ou3"]
        + ["ou2"] * 4
        + ["ou3"] * 2,
        "level_2_name": [None, "OU2", "OU3"]
        + ["OU2"] * 2
        + ["OU3"]
        + ["OU2"] * 3
        + ["OU3"]
        + ["OU2"] * 4
        + ["OU3"] * 2,
        "level_3_id": [None, None, None, "ou4", "ou5", "ou6"]
        + ["ou4"] * 2
        + ["ou5"]
        + ["ou6"]
        + ["ou4"] * 3
        + ["ou5"]
        + ["ou6"] * 2,
        "level_3_name": [None, None, None, "OU4", "OU5", "OU6"]
        + ["OU4"] * 2
        + ["OU5"]
        + ["OU6"]
        + ["OU4"] * 3
        + ["OU5"]
        + ["OU6"] * 2,
        "level_4_id": [None] * 6
        + ["ou7", "ou8", "ou9", "ou10"]
        + ["ou7"]
        + ["ou8"] * 2
        + ["ou9"]
        + ["ou10"] * 2,
        "level_4_name": [None] * 6
        + ["OU7", "OU8", "OU9", "OU10"]
        + ["OU7"]
        + ["OU8"] * 2
        + ["OU9"]
        + ["OU10"] * 2,
        "level_5_id": [None] * 10 + ["ou11", "ou12", "ou13", "ou14", "ou15", "ou16"],
        "level_5_name": [None] * 10 + ["OU11", "OU12", "OU13", "OU14", "OU15", "OU16"],
        "level_6_id": [None] * 16,
        "level_6_name": [None] * 16,
        "geometry": [None] * 16,
    }
)


dataset_ous_in = {"organisationUnits": [{"id": "ou1"}, {"id": "ou2"}, {"id": "ou3"}]}
dataset_ous_out = ["ou1", "ou2", "ou3"]


date_str = "2023-03-15"
expected_periods = {
    "Daily": "20230315",
    "Weekly": "2023W11",
    "WeeklyWednesday": "2023WedW11",
    "WeeklyThursday": "2023ThuW10",
    "WeeklySaturday": "2023SatW10",
    "WeeklySunday": "2023SunW10",
    "Monthly": "202303",
    "BiMonthly": "202302",
    "Quarterly": "2023Q1",
    "SixMonthly": "2023S1",
    "Yearly": "2023",
    "FinancialApril": "2022April",
    "FinancialJuly": "2022July",
    "FinancialOct": "2022Oct",
    "FinancialNov": "2022Nov",
}
