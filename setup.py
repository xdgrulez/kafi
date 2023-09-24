from setuptools import setup
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name='kafi',
    version='0.0.3',
    description='Kafka and Files',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/xdgrulez/kafi',
    author='Ralph M. Debusmann',
    author_email='matthias.debusmann@gmail.com',
    license='Apache License 2.0',
    packages=['kafi'],
    install_requires=['azure-storage-blob',
                      'confluent_kafka>=2.2.0',
                      'fastavro',
                      'grpcio-tools',
                      'jsonschema',
                      'minio',
                      'piny',
                      'requests',
                      'azure-storage-blob',
                      'minio',
                      'lxml',
                      'openpyxl',
                      'pandas',
                      'pyarrow'
                      ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.10',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
