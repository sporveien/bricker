from setuptools import setup, find_packages

setup(
    name='bricker',
    description='CLI tool for syncing a Databricks folder structure with a local git repo.',
    version='0.4.2',
    author='sporveien',
    author_email='brick@sporveien.com',
    url='https://github.com/sporveien/bricker',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7'
    ],
    py_modules=['bricker'],
    packages=['bricker'],
    install_requires=[
         'Click'
        ,'requests'
        ,'gitpython'
        ,'easydict'
        ,'pyyaml'
    ],
    entry_points='''
        [console_scripts]
        bricker=bricker:cli
    ''',
)
