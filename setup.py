# Copyright (c) 2019 Dan Ryan
# MIT License


import setuptools

setuptools.setup(
    package_dir={"": "src"},
    packages=setuptools.find_packages("src"),
    package_data={"": ["LICENSE*", "README*"]},
)
