"""A setuptools based setup module.

See:
https://packaging.python.org/guides/distributing-packages-using-setuptools/
https://github.com/pypa/sampleproject
"""
try:
    # pip >=20
    from pip._internal.network.session import PipSession
    from pip._internal.req import parse_requirements
except ImportError:
    try:
        # 10.0.0 <= pip <= 19.3.1
        from pip._internal.req import parse_requirements
        from pip._internal.download import PipSession
    except ImportError:
        # for pip <= 9.0.3
        from pip.req import parse_requirements
        from pip.download import PipSession

from os import path

from setuptools import find_packages, setup

here = path.abspath(path.dirname(__file__))
links = []
requires = []

try:
    # new versions of pip requires a session
    requirements = parse_requirements(
        path.join(here, 'requirements.txt'), session=PipSession())
except Exception as exc:
    requirements = parse_requirements(path.join(here, 'requirements.txt'))


for item in requirements:
    # we want to handle package names and also repo urls
    if getattr(item, 'url', None):  # older pip has url
        links.append(str(item.url))
    if getattr(item, 'link', None): # newer pip has link
        links.append(str(item.link))
    try:
        if item.req:
            requires.append(str(item.req))
    except AttributeError:
        if item.requirement:
            requires.append(str(item.requirement))


setup(
    name='ksnap',
    version='2.0.1',
    packages=find_packages(),
    python_requires='>=3.6, <4',
    install_requires=requires,
    entry_points={
        "console_scripts": [
            "ksnap = ksnap.__main__:main"
        ]
    },
    license_files = ('LICENSE',),
)
