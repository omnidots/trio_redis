from setuptools import setup


setup(
    name='trio_redis',
    version='0.2.0',
    description='A Trio-based Redis client.',
    author='Omnidots',
    author_email='support@omnidots.nl',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent'
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    packages=['trio_redis'],
    install_requires=[
        'hiredis',
        'trio',
    ],
)
