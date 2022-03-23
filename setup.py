from setuptools import setup
from setuptools import find_packages
import pathlib
from glob import glob

# python3 setup.py sdist bdist_wheel
# twine upload dist/*

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()

exec(open('src/yfinhanced/_version.py').read())




setup(
        name = 'yfinhanced',
        packages = find_packages('src'),
        package_dir = {'': 'src'},
        py_modules=[splitext(basename(path))[0] for path in glob('src/*.py')],
        version = __version__,
        license='MIT',
        description = 'A python wrapper around the yahoo finance API that leverages pandas DataFrames',
        long_description = README,
        long_description_content_type = 'text/markdown',
        author = 'ME-64',
        author_email = 'milo_elliott@icloud.com',
        url = 'https://github.com/ME-64/yfinhanced',
        keywords = ['api wrapper', 'yahoo finance', 'markets'],
        include_package_data = True,
        zip_safe = False,
        install_requires=['pandas', 'aiohttp', 'asyncio', 'requests', 'pytz', 'requests'],
        extras_require={
            "dev": ['pytest', 'pyteset-asyncio']},
        classifiers=[
            'Development Status :: 3 - Alpha',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: MIT License',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: 3.8',
            'Programming Language :: Python :: 3.9',
            ]
        )
