from distutils.core import setup

setup(name='bqueryd',
      version='0.10',
      packages=['bqueryd'],
      install_requires = [
          'bquery>=0.2.10',
          'pyzmq>=16.0.2',
          'redis>=2.10.5',
          'boto>=2.43.0',
          'smart_open>=1.3.5',
          'netifaces>=0.10.5',
          'configobj>=5.0.6',
          'ipython>=5.4.1'
      ],
      entry_points={
            'console_scripts': [
                  'bqueryd = bqueryd.node:main'
            ]
            },
      )
