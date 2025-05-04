from setuptools import setup, find_packages

setup(
    name="charizard",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "numpy",
        "pyyaml",
    ],
    entry_points={
        'console_scripts': [
            'charizard=charizard.charizard:main',
        ],
    },
    package_data={
        'charizard': ['pokedex.yaml'],
    },
    python_requires=">=3.8",
)