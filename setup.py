from setuptools import (
    find_packages,
    setup
)

# Load the contents of the metadata module without using import, since
# importing requires all dependencies to be available and at this point
# pip hasn't checked them yet.
metadata = {}
with open("wmfdata/metadata.py") as file:
    exec(file.read(), metadata)

setup(
    name="wmfdata",
    version=metadata["version"],
    description=(
        "Tools for analyzing data on SWAP, a platform for confidential "
        "Wikimedia data"
    ),
    install_requires=[
        "IPython",
        "findspark",
        # This is the latest version which supports MariaDB Connector/C 3.1.16,
        # which is the version currently available on the analytics clients
        "mariadb==1.0.11",
        "pandas>=0.20.1", # 0.20.1 introduced the errors module
        "packaging",
        "pyarrow",
        "requests",
        "requests_kerberos",
        "presto-python-client",
        "pyhive[hive]",
    ],
    packages=find_packages(),
    python_requires=">=3",
    url=metadata["source"],
    author="Wikimedia Foundation Product Analytics team",
    author_email="product-analytics@wikimedia.org",
    license="BSD 3-Clause"
)
