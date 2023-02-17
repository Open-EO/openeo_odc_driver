from setuptools import setup, find_packages

# Load the openeo_odc_driver version info.
#
# Note that we cannot simply import the module, since dependencies listed
# in setup() will very likely not be installed yet when setup.py run.
#
# See:
#   https://packaging.python.org/guides/single-sourcing-package-version


_version = {}
with open("openeo_odc_driver/_version.py") as fp:
    exec(fp.read(), _version)


with open("README.md", "r") as fh:
    long_description = fh.read()

# tests_require = [
#     "pytest>=4.5.0",
#     "mock",
#     "requests-mock>=1.8.0",
#     "httpretty>=1.1.4",
#     "netCDF4",  # e.g. for writing/loading NetCDF data with xarray
#     "matplotlib",
#     "geopandas",
#     "flake8>=5.0.0",
#     "time_machine",
# ]

# docs_require = [
#     "sphinx",
#     "sphinx-autodoc-annotation",
#     "sphinx-autodoc-typehints!=1.21.4",  # Issue #366 doc build fails with version 1.21.4
#     "myst-parser",
# ]

name = "openeo_odc_driver"
setup(
    name=name,
    version=_version["__version__"],
    author="Michele Claus",
    author_email="michele.claus@eurac.edu",
    description="Python based openEO processing engine",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/SARScripts/openeo_odc_driver",
    python_requires=">=3.8",
    packages=find_packages(include=["openeo_odc_driver*"]),
    include_package_data=True,
    # tests_require=tests_require,
    test_suite="tests",
    install_requires=[
        "numpy",
        "xarray",
        "pandas",
        "geopandas",
        "rasterio",
        "rioxarray",
        "dask",
        "distributed",
        "scikit-learn",
        "scipy",
        "proj",
        "gunicorn",
        "netCDF4",
        "flask",
        "opencv-python"
    ],
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: Apache Software License",
        "Development Status :: 4 - Beta",
        "Operating System :: OS Independent",
    ],
)
