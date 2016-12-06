from setuptools import setup, find_packages


version = '0.6.3'


setup(name='openprocurement.complaints.queue',
        version=version,
        description="OpenProcurement complaints queue",
        long_description=open("README").read(),
        # Get more strings from
        # http://pypi.python.org/pypi?:action=list_classifiers
        classifiers=[
            "License :: OSI Approved :: Apache Software License",
            "Programming Language :: Python",
        ],
        keywords='OpenProcurement',
        author='Volodymyr Flonts',
        author_email='flyonts@gmail.com',
        license='Apache License 2.0',
        url='https://github.com/imaginal/openprocurement.complaints.queue',
        packages=find_packages(),
        namespace_packages=['openprocurement'],
        include_package_data=True,
        zip_safe=False,
        install_requires=[
          'iso8601',
          'python-dateutil',
          'simplejson',
          'MySQL-python',
          'openprocurement_client',
        ],
        entry_points={
          'console_scripts': [
              'complaintsd = openprocurement.complaints.queue.queue_worker:main',
          ],
        }
    )
