from setuptools import setup, find_packages

setup(
    packages=find_packages(
        include=[
            "feathr_registry",
            "feathr_registry.registry*"
        ]
    ),
)
