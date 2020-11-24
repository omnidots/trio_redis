import setuptools

setuptools.setup(
    name='trio_redis',
    version='0.2.3',
    author='Omnidots B.V.',
    author_email='support@omnidots.com',
    description='A Redis client for Trio.',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    packages=['trio_redis'],
    python_requires='>=3.7',
    install_requires=[
        'hiredis',
        'trio',
    ],
)
