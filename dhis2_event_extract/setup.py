from setuptools import find_packages, setup

setup(
    name="openhexa-dhis2-event-extract",
    version="0.1.0",
    description="OpenHEXA DHIS2 event extraction pipeline",
    packages=find_packages(),
    python_requires=">=3.12",
    install_requires=[
        "openhexa.sdk>=1.0.0",
        "openhexa-toolbox",  # CRITICAL: include [dhis2] extra
        "polars>=1.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "ruff>=0.1.0",
            "mypy>=1.0.0",
        ]
    },
)
