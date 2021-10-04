from setuptools import setup, find_packages
from bitcoin_graph import __version__


setup(
    name='bitcoin-graph',
    version=__version__,
    packages=find_packages(),
    url='https://github.com/Nerolation/bitcoin-graph',
    author='Anton Wahrstätter',
    author_email='anton.wahrstaetter@wu.ac.at',
    description='Graph Creator and Analyzer',
    install_requires=[
        'python-bitcoinlib==0.11.0',
        'networkit==9.0'
    ]
)
