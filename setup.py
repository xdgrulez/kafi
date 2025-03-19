from setuptools import setup, find_packages
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name='kafi',
    version='0.0.56',
        description='Kafka and Files',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/xdgrulez/kafi',
    author='Ralph M. Debusmann',
    author_email='matthias.debusmann@gmail.com',
    license='Apache License 2.0',
    packages=find_packages(),
    install_requires=['confluent_kafka[avro,json,protobuf,schemaregistry]==2.8.2',
                      'grpcio-tools==1.69.0',
                      'piny==1.1.0',
                      'requests==2.32.3',
                      'azure-storage-blob==12.24.0',
                      'minio==7.2.14',
                      'lxml==5.3.0',
                      'openpyxl==3.1.5',
                      'pandas==2.2.3',
                      'pyarrow==18.1.0'
                      ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.10',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
