Estate Data Integration & Journal Entry Automation

Tech Stack: Python, PyODBC, Pandas, NumPy, OpenPyXL, MS Access

Overview

This project automates the integration of estate stock data from Microsoft Access databases into structured journal entry reports. It connects to multiple tables, consolidates stock transactions, generates account codes based on plantation status (Mature, Replant, Others), and outputs a formatted Excel interface for accounting entries.

Features

Database Integration: Connects to MS Access using pyodbc and retrieves stock, block, and task data.

Data Transformation: Cleans and merges multiple query results into a unified dataset with calculated totals.

Automated Account Coding: Dynamically generates account codes based on block status and task type.

Excel Report Generation: Uses OpenPyXL to produce journal entry reports with proper formatting, account codes, and monthly summaries.

Error Handling: Provides user prompts if data is missing or files are locked.

Problem Solved

Estate staff previously had to manually reconcile stock usage, assign account codes, and prepare journal entries for fertilizer, chemicals, and other materials. This process was time‑consuming and error‑prone.

Impact

Reduced manual data entry and improved accuracy of journal entries.

Standardized reporting format for easier review by finance teams.

Saved significant time in monthly closing processes.

Provided clear segregation of fertilizer, chemical, and other stock categories for better cost tracking.

🔗 [LinkedIn](https://www.linkedin.com/in/chin-kee-ming-588685148)


