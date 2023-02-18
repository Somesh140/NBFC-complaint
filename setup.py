from setuptools import setup,find_packages
from typing import List

with open("README.md","r",encoding = "utf-8") as f:
    long_description = f.read()

__version__="0.0.0"

REQUIREMENT_FILE_NAME = "requirements.txt"

HYPHEN_E_DOT = "-e ."

PROJECT_NAME = "NBFC-complaint"
VERSION = "0.0.0"
AUTHOR = "Somesh Trivedi"
DESRCIPTION = "This is a sample for industry ready solution"

def get_requirements_list(filepath=REQUIREMENT_FILE_NAME) -> List[str]:
    """
    Description: This function is going to return list of requirement
    mention in requirements.txt file
    return This function is going to return a list which contain name
    of libraries mentioned in requirements.txt file
    """
    with open(filepath) as requirement_file:
        requirement_list = requirement_file.readlines()
        requirement_list = [requirement_name.replace("\n", "") for requirement_name in requirement_list]
        if HYPHEN_E_DOT in requirement_list:
            requirement_list.remove(HYPHEN_E_DOT)
        return requirement_list


setup(
    name=PROJECT_NAME,
    version=__version__,
    author=AUTHOR,
    description=DESRCIPTION,
    long_description=long_description,
    packages=find_packages(),
    install_requires=get_requirements_list()
)