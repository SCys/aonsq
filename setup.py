import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setuptools.setup(
    name="aonsq",
    version="0.0.5",
    author="SCys",
    author_email="me@iscys.com",
    description="an other async nsq client library",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/SCys/aonsq",
    packages=setuptools.find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Software Development :: Libraries",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    python_requires=">=3.6",
    install_requires=requirements,
)
