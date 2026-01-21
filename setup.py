"""Setup configuration for MAUDE Analyzer package."""

from setuptools import setup, find_packages

setup(
    name="maude-analyzer",
    version="1.0.0",
    description="FDA MAUDE Database Analyzer for SCS Devices",
    author="Alex",
    author_email="",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.11",
    install_requires=[
        "duckdb>=1.0.0",
        "pandas>=2.1.0",
        "numpy>=1.26.0",
        "streamlit>=1.32.0",
        "plotly>=5.18.0",
        "requests>=2.31.0",
        "httpx>=0.26.0",
        "openpyxl>=3.1.0",
        "xlsxwriter>=3.1.0",
        "python-dotenv>=1.0.0",
        "tqdm>=4.66.0",
        "tenacity>=8.2.0",
    ],
    extras_require={
        "dev": [
            "pytest>=8.0.0",
            "pytest-cov>=4.1.0",
            "black>=24.1.0",
            "ruff>=0.2.0",
        ],
        "fast": [
            "polars>=0.20.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "maude-load=scripts.initial_load:main",
            "maude-update=scripts.weekly_update:main",
        ],
    },
)
