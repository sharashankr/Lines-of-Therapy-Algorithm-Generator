# LOTGenerator

**LOTGenerator** is a package designed to efficiently generate **Line of Therapy (LOT) data** for any dataset or indication using a streamlined three-step process.

## 📌 Features
- **Secure & Scalable**: Processes large datasets (millions of records) using **Amazon Redshift**, ensuring high performance.
- **Flexible Workflow**: Start at any step based on available data.
- **Patient Data Security**: No patient data is stored in system memory.

## 🔄 Process Overview

### 1️⃣ Input Excel File
- Populate key parameters in the provided Excel file.
- This file guides **LOT generation** but **excludes patient data** for confidentiality.

### 2️⃣ Data Processing
- The package reads the Excel file and applies logic to generate LOT data.

### 3️⃣ Post-Processing Adjustments
- Users can refine the output using built-in methods.
- Ensures structured, high-quality LOT data.

## 🛠️ How It Works
1. Install the package:
   ```bash
   pip install LOTGenerator
   ```
2. Prepare the input Excel file following the predefined format.
3. Run the generator:
   ```python
   from LOTGenerator import process_lot

   process_lot("input_file.xlsx")
   ```
4. Retrieve and analyze the generated LOT data.

## ⚡ Scalability & Security
- All computations occur within **Amazon Redshift** for efficient handling of large datasets.
- No patient data is loaded into system memory, ensuring compliance with data security standards.

## 🔄 Flexible Workflow
- The process is **not strictly linear**—start at any step based on available data.
- If **treatment episode data exists**, you can **skip earlier steps** and begin directly from regimen assignment.
- Maintaining the correct **table structure and naming conventions** ensures seamless integration without reprocessing prior steps.

## 📜 Summary
LOTGenerator provides a **secure, scalable, and flexible** solution for **LOT data generation**, optimized for large, complex datasets while ensuring **patient data security**. **Amazon Redshift** enables **high-performance processing** throughout the workflow.

---
📌 **Need Help?** Open an issue or reach out to the maintainers.
