"""Setup script for Lark BITable."""
import re
import setuptools


with open("pybitable/__init__.py") as f:
    version = re.findall("__version__.*(\d+\.\d+\.\d+).*", f.read())[0]

with open("README.md", "r") as readme_file:
    long_description = readme_file.read()

setuptools.setup(
    name="pybitable",
    version=version,
    author="lloydzhou",
    author_email="lloydzhou@qq.com",
    url="https://github.com/lloydzhou/pybitable",
    description="A Python wrapper around Lark bitable to sql.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    python_requires=">=3.10",
    entry_points={
        'sqlalchemy.dialects': [
            'bitable = pybitable.dialect:BITableDialect',
            'bitable.pybitable = pybitable.dialect:BITableDialect',
        ],
    },
    extras_require={
        'sqlalchemy': ['sqlalchemy'],
    },
    install_requires=[
        "pep249",
        "pyparsing",
        "mo_sql_parsing",
        "ca-lark-sdk"
    ]
)
