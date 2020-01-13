from setuptools import setup, find_packages

from wmfdata import __version__

setup(
    name="wmfdata",
    version=__version__,
    description="Tools for analyzing data on SWAP, a platform for confidential Wikimedia data",
    install_requires=[
        "impyla",
        "IPython",
        "matplotlib>=2.1",  # 2.1 introduced ticker.PercentFormatter
        "mysql-connector-python",
        "pandas",
        "packaging",
        "requests",
        "thrift-sasl==0.2.1",  # impyla can't connect properly to Hive with a later version
    ],
    packages=find_packages(),
    python_requires=">=3"
)
