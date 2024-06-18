
# Crash Analysis Project

## Overview

This project is designed to perform detailed crash analysis using a PySpark-based application. The analysis covers various aspects of crash data, including the number of crashes with specific conditions, vehicle involvement, driver details, and more. The application reads data from multiple CSV files, performs analyses, and outputs the results in a structured format.

## Project Structure
crash_analysis/
├── config/
│ └── config.yaml
├── data/
│ ├── Charges_use.csv
│ ├── Damages_use.csv
│ ├── Endorse_use.csv
│ ├── Primary_Person_use.csv
│ ├── Restrict_use.csv
│ ├── Units_use.csv
├── output/
│ └── analysis_results.json
├── src/
│ ├── main.py
│ ├── analysis.py
│ ├── spark_utils.py
│ ├── analysis_utils.py
├── README.md
└── requirements.txt


## Environment Setup

### Prerequisites

- **Python**: 3.6 or above
- **Java**: 8 or above
- **Apache Spark**: 3.0 or above
- **Hadoop**: 2.7 or above

### Creating and Activating a Virtual Environment

1. Navigate to the project directory and create a virtual environment:

    cd /path/to/your/crash_analysis
    python -m venv venv


2. Activate the virtual environment:

    - **Windows**:
        .\venv\Scripts\activate

    - **macOS/Linux**:
        source venv/bin/activate


3. Install the required packages:
    pip install -r requirements.txt


### Verifying Installation

Ensure that the packages are correctly installed by listing them:

pip list

### Configuration
The configuration file config/config.yaml contains paths to input data files and the output directory. The paths can be specified as relative or absolute paths. The application will convert relative paths to absolute paths at runtime.

## Config File Example (config/config.yaml)
data_paths:
  charges: '../data/Charges_use.csv'
  damages: '../data/Damages_use.csv'
  endorse: '../data/Endorse_use.csv'
  primary_person: '../data/Primary_Person_use.csv'
  restrict: '../data/Restrict_use.csv'
  units: '../data/Units_use.csv'

output_path: '../output'


Config File Example (config/config.yaml)
yaml
Copy code
data_paths:
  charges: '../data/Charges_use.csv'
  damages: '../data/Damages_use.csv'
  endorse: '../data/Endorse_use.csv'
  primary_person: '../data/Primary_Person_use.csv'
  restrict: '../data/Restrict_use.csv'
  units: '../data/Units_use.csv'

output_path: '../output'

### Running the Application
spark-submit src/main.py

## Logging
The application logs its progress and any errors to a log file named logs.log in the output directory. This file includes timestamps and log levels to help with troubleshooting and monitoring.


Code Explanation
main.py
The main script that orchestrates the loading of configuration, initialization of Spark, reading of data, performing analyses, and writing results.

analysis.py
Contains the CrashAnalysis class with methods for each analysis. Each method includes a description of the analysis, the input data, and the output results.

spark_utils.py
Utility functions for Spark operations, including getting a Spark session, reading CSV files, resolving absolute paths, writing output, loading data, and configuring logging.

analysis_utils.py
Utility functions to perform all analyses and write the results to the output file.

 
