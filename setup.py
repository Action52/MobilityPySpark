from setuptools import setup, find_packages

setup(
    name='pysparkmeos',
    version='0.1.0',
    packages=find_packages(),
    description='A custom PySpark extension for handling MobilityDB and MEOS functionalities.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Luis Alfredo León Villapún',
    author_email='leonvillapun@gmail.com',
    url='https://github.com/yourusername/pysparkmeos',
    install_requires=[
        'pymeos',
        'contextily==1.6.0',
        'distinctipy==1.3.4',
        'pandas==2.2.2',
        'pyspark==3.5.1',
        'geopandas==0.14.4',
        'matplotlib'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Operating System :: OS Independent',
    ],
)