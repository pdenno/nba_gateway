import io
import re
from setuptools import setup

with io.open("README.rst", "rt", encoding="utf8") as f:
    readme = f.read()

version = '0.1.11'

setup(
    name="nba_gateway-pdenno",
    version=version,
    url="https://github.com/pdenno/nba_gateway/",
    project_urls={
        "Documentation": "https://github.com/pdenno/nba_gateway/",
        "Code": "https://github.com/pdenno/nba_gateway/",
        "Issue tracker": "https://github.com/pdenno/nba_gateway/issues",
    },
    license="BSD",
    author="Peter Denno",
    author_email="podenno@gmail.com",
    maintainer="Peter Denno",
    maintainer_email="podenno@gmail.com",
    description="Server to communicate between Jupyter notebooks and nba-agent",
    long_description=readme,
    packages=["nba_gateway"],
    include_package_data=True,
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
)
