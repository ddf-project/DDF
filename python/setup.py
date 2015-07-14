from setuptools import setup, find_packages
from setuptools.command.install import install

def __read_version(file_path):
    import json
    from collections import defaultdict
    return defaultdict(str, json.loads(open(file_path).read()))

_version = __read_version('./ddf/version.json')['version']

setup(
    cmdclass={'install': install},
    name='ddf',
    version=_version,
    keywords='DDF',
    author='ADATAO Inc.',
    author_email='dev@adatao.com',
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    zip_safe=False,
    scripts=[],
    url='http://ddf.io/',
    license='LICENSE',
    description='Distributed Data Frame',
    long_description=open('README.md').read(),
    install_requires=[
        "py4j",
        "pandas",
        "tabulate"
    ],
    entry_points="""
      # -*- Entry points: -*-
      """
)
