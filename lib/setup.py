from setuptools import setup

setup(name='lib',
      version='0.1',
      description='our lib',
      url='https://github.com/nycPlanning/db-data-recipes',
      author='Baiyue',
      author_email='baiyue@whatever.com',
      license='MIT',
      packages=['lib'],
      install_requires=[
            'click',
            'python-dotenv', 
            'gdal',
            'psycopg2-binary',
            'pytest'
      ], 
      entry_points='''
        [console_scripts]
        cook=lib.cli:cli
      '''
      )