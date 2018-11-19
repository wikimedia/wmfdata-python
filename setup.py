from setuptools import setup, find_packages

setup(
    name="wmfdata",
    version="0.1",
    description="Tools for analyzing data on SWAP, a platform for confidential Wikimedia data",
    install_requires=["pandas", "impyla", "mysql-connector-python", "matplotlib"],
    packages=find_packages(),
    python_requires=">=3"
)
