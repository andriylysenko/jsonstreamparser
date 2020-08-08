# from distutils.core import setup, Extension
from setuptools import setup, Extension, Command
import setuptools

stream_json_parser_utils = Extension('stream_json_parser_utils', extra_compile_args = [ '--std=c++11' ], sources = [ 'utils/src/utils.cpp' ])

if __name__ == '__main__':
   setup(
       name = 'streamjsonparser',
       version = '1.0',
       description = 'Stream Json Parser',
       ext_modules = [stream_json_parser_utils],
       author='Andrey Lysenko',
       author_email='andrey.lysenko@gmail.com',
       packages=setuptools.find_packages(),
       include_package_data=True,
       zip_safe=False,
       python_requires='>=3.4'
   )
