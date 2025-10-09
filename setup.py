from setuptools import setup, find_packages
from typing import List
from pathlib import Path

HYPEN_E_DOT='-e .'

def get_requirements(file_path: str) -> List[str]:
    if not Path(file_path).exists():
        return []

    with open(file_path) as file_obj:
        requirements = [req.strip() for req in file_obj.readlines()]
        if HYPEN_E_DOT in requirements:
            requirements.remove(HYPEN_E_DOT)
    return requirements

setup(
    name = 'rrc_scraping',
    version = '0.0.1',
    author = 'apoorva',
    author_email = 'apoorvasaxena207@gmail.com',
    packages = find_packages(),
    install_requires = get_requirements('requirements.txt')
)