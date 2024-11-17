from setuptools import setup, find_packages

setup(
    name='tlb2_on_spark',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'pyspark',
    ],
    entry_points={
        'console_scripts': [
            'tlb2_on_spark=tlb2_on_spark.main:main',
        ],
    },
)