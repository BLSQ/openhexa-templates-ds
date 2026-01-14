"""Setup file for sync_dhis2_to_dhis2 pipeline."""

from setuptools import setup, find_packages

setup(
    name="sync_dhis2_to_dhis2",
    version="0.1.0",
    description="DHIS2 to DHIS2 sync check pipeline for OpenHEXA",
    packages=find_packages(),
    python_requires=">=3.11",
    install_requires=[
        "openhexa.sdk",
        "openhexa-toolbox[dhis2]",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "ruff>=0.1.0",
            "mypy>=1.0.0",
        ],
    },
)