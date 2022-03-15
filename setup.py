#!/usr/bin/env python

import setuptools

requirements = ["apache-airflow"]

setuptools.setup(
    name="nyctransport",
    version="0.1.1",
    description="Operators for the NYC transportation example (chapter 14) in Data Pipelines with apache airflow book "
    "(manning).",
    author="Anonymous",
    author_email="anonymous@example.com",
    install_requires=requirements,
    packages=setuptools.find_packages("src"),
    package_dir={"": "src"},
    url="https://github.com/mrn-aglic/nyc_operators.git",
    python_requires=">=3.8.*",
    license="MIT license",
)
