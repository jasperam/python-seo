"""
@author Neo
@time 2020/04/06
"""

from setuptools import setup, find_packages

setup(
    name='seo',
    version='0.0.3',
    description="seo workflow",
    author='Neo',
    author_email='neo.lin@jaspercapital.com',
    python_requires='>=3.6',
    packages=find_packages('src'),
    package_dir={"": "src"},
    # package_data={
    #      'seo': [             
    #      ]
    # },
    install_requires=[      
        'jt',
        'jt.invest',
        'numpy',
        'pandas',
        'python-docx'
    ],
    zip_safe=False,
)
