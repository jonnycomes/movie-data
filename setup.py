from setuptools import setup, find_packages

setup(
    name='movie_project',
    version='0.1',
    packages=find_packages(where='src'), 
    package_dir={'': 'src'}, 
    install_requires=[
        'pandas', 
        'seaborn', 
        'matplotlib', 
        'scikit-learn', 
        'numpy',  
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License', 
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6', 
)
