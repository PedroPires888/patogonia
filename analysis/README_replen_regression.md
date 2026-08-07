# Replen Regression Workbook Package

This repository includes the source data and a Python script that creates an Excel workbook named `replen_regression.xlsx` with:

- **Raw Data** sheet
- **Model** sheet with regression coefficients
- **Forecast** sheet for entering `Total Lines` and `avg_replen_units` to predict `Replens`

## Files

- `data/replen_data.csv` – source data provided by the user
- `scripts/build_replen_regression_workbook.py` – generates the workbook
- `analysis/replen_regression.xlsx` – generated workbook output

## Model

The script fits a multiple linear regression of the form:

`Predicted Replens = Intercept + b1 * Total Lines + b2 * avg_replen_units`

using ordinary least squares with NumPy.

## How to regenerate

Run:

```bash
python scripts/build_replen_regression_workbook.py
```

The workbook will be written to `analysis/replen_regression.xlsx`.
