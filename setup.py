from setuptools import setup, find_packages

setup(
    name="charizard",
    version="2.0.0",
    description="Radio Interferometry Calibration Pipeline",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        'charizard': ['data/*.yaml', 'data/*.xml'],
    },
    python_requires=">=3.8",
    install_requires=[
        "pyyaml>=5.0",
        "numpy>=1.20",
        "astropy>=5.0",
        "python-casacore>=3.0",
        "rich>=10.0",
        "housekeeper>=2.0",
    ],
    entry_points={
        "console_scripts": [
            "charizard=charizard.main:main",
        ],
    },
)
