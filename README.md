# MS Access Database to Journal Voucher Converter
This Python script is designed to read data from a Microsoft Access database and extract stock in/stock out information, as well as unit prices of the stocks. It utilizes popular libraries such as pyodbc, pandas, and openpyxl to accomplish this task. Additionally, it generates Account Codes based on the data from the database and formats the extracted data into a Journal Voucher that can be easily interfaced with an accounting software.

Prerequisites

Before using this script, you need to ensure you have the following prerequisites installed on your system:

Python 3.x
pyodbc library for database connectivity
pandas for data manipulation
openpyxl for Excel file handling
Microsoft Access database file (.accdb) containing the relevant stock data


Output

The script will generate a Journal Voucher in an Excel file format (.xlsx) based on the data extracted from the database. You can then use this Journal Voucher to interface with your accounting software.
