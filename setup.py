from setuptools import setup, find_packages

# Load the contents of the metadata module without using import, since importing requires all dependencies
# to be available and at this point pip hasn't checked them yet.
metadata = {}
with open("wmfdata/metadata.py") as file:
    exec(file.read(), metadata)

setup(
    name="wmfdata",
    version = metadata["version"],
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
