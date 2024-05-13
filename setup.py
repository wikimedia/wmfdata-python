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
        "dnspython",
        "IPython",
        # The next version of the Mariadb Python connector (1.1.0) increases the required C connector
        # version to 3.2.4 (https://mariadb.com/kb/en/mariadb-connector-python-110-alpha1-release-notes/).
        # As of May 2024, we are running MariaDB 10.5.23, which includes C Connector 3.1.23
        # (https://mariadb.com/kb/en/list-of-mariadb-connector-c-releases/).
        "mariadb==1.0.11",
        # Pandas 1.0 is the the first to support None as a value for
        # `display.max_colwidth`
        # https://github.com/pandas-dev/pandas/issues/31532
        "pandas>=1.0",
        "packaging",
        "pyarrow",
        "requests",
        "requests_kerberos",
        "presto-python-client",
        "pyhive[hive]",
        "urllib3"
    ],
    packages=find_packages(),
    python_requires=">=3",
    url=metadata["source"],
    author="Wikimedia Foundation Product Analytics team",
    author_email="product-analytics@wikimedia.org",
    license="BSD 3-Clause"
)
