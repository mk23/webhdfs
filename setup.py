from setuptools import setup
from lib.webhdfs import __version__

if __name__ == '__main__':
    setup(
        author='Max Kalika',
        author_email='max.kalika+projects@gmail.com',
        url='https://github.com/mk23/webhdfs',

        name='webhdfs',
        version=__version__,
        scripts=['webhdfs3'],
        packages=['webhdfs'],
        package_dir={'webhdfs': 'lib/webhdfs'},
        license='LICENSE.txt',
        install_requires=['requests', 'setuptools']
    )
