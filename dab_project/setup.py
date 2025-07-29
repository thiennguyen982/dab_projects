from setuptools import setup, find_packages

setup(
    name="dab_project",
    version="0.0.2",
    description="Common function used in the project",
    author="Thien.Nguyen@rackspace.com",
    packages=find_packages(where="./src"),
    package_dir={"": "./src"},
    install_requires=["setuptools"],
    entry_points={"packages": ["main=dab_project.main:main"]},
)
