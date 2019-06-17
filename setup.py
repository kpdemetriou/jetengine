from os import path
from setuptools import setup, find_packages

with open(path.join(path.abspath(path.dirname(__file__)), "README.rst"), encoding="utf-8") as handle:
    readme = handle.read()

setup(
    name="jetengine",
    version="0.1.3",
    description="The no-nonsense asyncio MongoDB ODM for Python 3.5+.",
    long_description=readme,
    url="https://github.com/kpdemetriou/jetengine",
    author="Phil Demetriou",
    author_email="inbox@philonas.net",
    license="BSD",
    packages=find_packages(exclude=["tests"]),
    install_requires=["easydict", "pymongo==3.6", "motor==1.3.1"],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
