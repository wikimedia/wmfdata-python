from setuptools import setup, find_packages

setup(
    name="wmfdata",
    version="0.1",
    description="Tools for analyzing data on SWAP, a platform for confidential Wikimedia data",
    install_requires=[
        "impyla",
        "matplotlib>=2.1", # 2.1 introducted ticker.PercentFormatter
        "mysql-connector-python", 
        "pandas",
        "thrift-sasl==0.2.1" # impyla can't connect properly to Hive with a later version
    ],
    packages=find_packages(),
    python_requires=">=3"
)
