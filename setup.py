from setuptools import setup, find_packages
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name='kafi',
    version='0.0.43',
    description='Kafka and Files',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/xdgrulez/kafi',
    author='Ralph M. Debusmann',
    author_email='matthias.debusmann@gmail.com',
    license='Apache License 2.0',
    packages=find_packages(),
    install_requires=['confluent_kafka[avro,json,protobuf,schemaregistry]',
                      'grpcio-tools',
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
