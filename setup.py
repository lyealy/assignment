import os
from setuptools import setup, find_packages

# Get ENV variables, default version 0.0.0 for local test
MAJOR_VERSION = os.getenv("MAJOR_VERSION", 0)
MINOR_VERSION = os.getenv("MINOR_VERSION", 0)
BUILD_NUMBER = os.getenv("BUILD_NUMBER", 0)


def parse_requirements(filename: str) -> list:
    lines = [line.strip() for line in open(filename)]
    return [line for line in lines if line and not line.startswith("#")]


setup(
    name='preprocessing',
    version=f"{MINOR_VERSION}.{MINOR_VERSION}.{BUILD_NUMBER}",
    description="Preprocessing package",
    autho='lyealy',
    package_data={
        "preprocessing.schemas": ["*.json"]
    },
    packages=find_packages(),
    python_requires=">=3.7",
    install_requires=parse_requirements("requirements.txt")
)