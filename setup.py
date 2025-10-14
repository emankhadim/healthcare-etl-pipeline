from setuptools import setup, find_packages

setup(
    name="healthcare-etl-pipeline",
    version="1.0.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "pandas>=2.1.0",
        "numpy>=1.26.0",
        "sqlalchemy>=2.0.19",
        "psycopg2-binary>=2.9.6",
        "python-dotenv>=1.0.0",
        "lxml>=4.9.3",
        "streamlit>=1.31.0",
        "plotly>=5.18.0",
        "pytest>=8.0.0",
    ],
    python_requires=">=3.10,<3.13",
)
