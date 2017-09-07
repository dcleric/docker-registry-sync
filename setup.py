from setuptools import setup

setup(name='docker_registry_sync',
      version='0.3',
      description='Tool for syncing 2 docker registries',
      url='https://gitlab.2gis.ru/continuous-delivery/docker-registry-sync',
      license='MIT',
      zip_safe=False,
      packages=['docker_registry_sync'],
      entry_points={
        'console_scripts': ['docker-registry-sync=docker_registry_sync:main'],
      },
      install_requires=['requests==2.12', 'docker==2.5.1']
      )
