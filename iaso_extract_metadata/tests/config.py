import polars as pl

strings_clean_string = {
    "leyré": "leyre",
    "leÿre": "leyre",
    "leyreç": "leyrec",
    "lêyre": "leyre",
    "leyre!": "leyre",
    "*leyre": "leyre",
    "¿leyre?": "leyre",
    " leyre": "leyre",
    "leyre ": "leyre",
    "leyre test": "leyre_test",
    "LeyrE": "leyre",
    "leyre-test": "leyre-test",
    "leyre_test": "leyre_test",
    "leyre123": "leyre123",
    "leyre 123": "leyre_123",
    "!!!": "",
    "   ": "",
}
chosen_metadata = {
    "questions": {
        "q_1": {
            "name": "name_one",
            "type": "type_1",
            "label": "label_1",
            "calculate": "calculate_1",
            "extra_field": "extra_1",
        },
        "q_2": {
            "name": "name_two",
            "type": "type_2",
            "label": "label_2",
            "calculate": "calculate_2",
            "extra_field": "extra_2",
        },
    },
    "choices": {
        "name_one": [
            {"name": "choice_one", "label": "label_one"},
            {"name": "choice_two", "label": "label_two"},
            {"name": "choice_three", "label": "label_three"},
        ],
        "name_two": [
            {"name": "choice_uno", "label": "label_uno"},
            {"name": "choice_dos", "label": "label_dos"},
        ],
        "name_three": [
            {"name": "choice_alpha", "label": "label_alpha"},
            {"name": "choice_beta", "label": "label_beta"},
            {"name": "choice_gamma", "label": "label_gamma"},
        ],
    },
    "extra_version_field": "extra_version_1",
}
metadata_in_valid_versions = {
    20230101: {"should you chose this": "no"},
    20240101: chosen_metadata,
    20230601: {"should you chose this": "still no"},
}

metadata_in_no_valid_versions = {
    "20240101a": chosen_metadata,
    "20230101a": {"should you chose this": "no"},
    "20230601a": {"should you chose this": "still no"},
}
questions_out = pl.DataFrame(
    (
        {
            "name": "name_one",
            "type": "type_1",
            "label": "label_1",
            "calculate": "calculate_1",
        },
        {
            "name": "name_two",
            "type": "type_2",
            "label": "label_2",
            "calculate": "calculate_2",
        },
    ),
)
questions_read = pl.DataFrame(
    (
        {
            "calculate": "calculate_1",
            "label": "label_1",
            "name": "name_one",
            "type": "type_1",
        },
        {
            "calculate": "calculate_2",
            "label": "label_2",
            "name": "name_two",
            "type": "type_2",
        },
    ),
)
choices_out = pl.DataFrame(
    (
        {"name": "name_one", "choice_value": "choice_one", "choice_label": "label_one"},
        {"name": "name_one", "choice_value": "choice_two", "choice_label": "label_two"},
        {"name": "name_one", "choice_value": "choice_three", "choice_label": "label_three"},
        {"name": "name_two", "choice_value": "choice_uno", "choice_label": "label_uno"},
        {"name": "name_two", "choice_value": "choice_dos", "choice_label": "label_dos"},
        {"name": "name_three", "choice_value": "choice_alpha", "choice_label": "label_alpha"},
        {"name": "name_three", "choice_value": "choice_beta", "choice_label": "label_beta"},
        {"name": "name_three", "choice_value": "choice_gamma", "choice_label": "label_gamma"},
    ),
)
choices_read = pl.DataFrame(
    (
        {"choice_label": "label_one", "choice_value": "choice_one", "name": "name_one"},
        {"choice_label": "label_two", "choice_value": "choice_two", "name": "name_one"},
        {"choice_label": "label_three", "choice_value": "choice_three", "name": "name_one"},
        {"choice_label": "label_uno", "choice_value": "choice_uno", "name": "name_two"},
        {"choice_label": "label_dos", "choice_value": "choice_dos", "name": "name_two"},
        {"choice_label": "label_alpha", "choice_value": "choice_alpha", "name": "name_three"},
        {"choice_label": "label_beta", "choice_value": "choice_beta", "name": "name_three"},
        {"choice_label": "label_gamma", "choice_value": "choice_gamma", "name": "name_three"},
    ),
)
questions_choices = pl.DataFrame(
    (
        {
            "name": "name_one",
            "type": "type_1",
            "label": "label_1",
            "calculate": "calculate_1",
            "choice_value": "choice_one",
            "choice_label": "label_one",
        },
        {
            "name": "name_one",
            "type": "type_1",
            "label": "label_1",
            "calculate": "calculate_1",
            "choice_value": "choice_two",
            "choice_label": "label_two",
        },
        {
            "name": "name_one",
            "type": "type_1",
            "label": "label_1",
            "calculate": "calculate_1",
            "choice_value": "choice_three",
            "choice_label": "label_three",
        },
        {
            "name": "name_two",
            "type": "type_2",
            "label": "label_2",
            "calculate": "calculate_2",
            "choice_value": "choice_uno",
            "choice_label": "label_uno",
        },
        {
            "name": "name_two",
            "type": "type_2",
            "label": "label_2",
            "calculate": "calculate_2",
            "choice_value": "choice_dos",
            "choice_label": "label_dos",
        },
    ),
)
