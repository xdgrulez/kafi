from setuptools import setup, find_packages
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name='kafi',
    version='0.1.0',
    description='Kafka and Files',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/xdgrulez/kafi',
    author='Ralph M. Debusmann',
    author_email='matthias.debusmann@gmail.com',
    license='Apache License 2.0',
    packages=find_packages(),
    install_requires=['confluent-kafka[avro,json,protobuf,schemaregistry]==2.13.0',
                      'grpcio-tools==1.76.0',
                      'piny==1.1.0',
                      'requests==2.34.2',
                      'azure-storage-blob==12.28.0',
                      'cloudpickle==3.1.2',
                      'minio==7.2.20',
                      'fastparquet==2025.12.0',
                      'lxml==6.1.1',
                      'openpyxl==3.1.5',
                      'pandas==2.3.3',
                      'pydbsp==2.0.0'
                      ],
    python_requires='>=3.12',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.12',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
