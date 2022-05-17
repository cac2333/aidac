from setuptools import setup

setup(
   name='aidac',
   version='1.0',
   description='AIDA client side',
   author='Suri Wang',
   author_email='suri980702@gmail.com',
   packages=['aidac'],  #same as name
   install_requires=['wheel', 'numpy', 'pandas'], #external packages as dependencies
)