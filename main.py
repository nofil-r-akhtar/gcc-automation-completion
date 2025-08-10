from flask import Flask, request, jsonify
import zipfile
import shutil
from pathlib import Path
import os
import subprocess
import sys
import pandas as pd

app = Flask(__name__)

@app.route("/clean-automated-csv", methods=["POST"])
def extract_and_clean_zip():
    try:
        # Get uploaded file and extraction path from form
        zip_file = request.files.get("zip_file")
        extract_to = request.form.get("extract_to")

        if not zip_file or not extract_to:
            return jsonify({"error": "zip_file and extract_to are required"}), 400

        # Ensure the extraction directory exists
        extract_path = Path(extract_to)
        extract_path.mkdir(parents=True, exist_ok=True)

        # Save uploaded zip file temporarily in the extraction folder
        temp_zip_path = extract_path / zip_file.filename
        with open(temp_zip_path, "wb") as buffer:
            shutil.copyfileobj(zip_file.stream, buffer)

        # Extract the zip file
        with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)

        # Remove the temporary zip file
        temp_zip_path.unlink()

        # Find specialization-report CSV file
        specialization_file = None
        for file in extract_path.rglob("*.csv"):
            if "specialization-report" in file.name.lower():
                specialization_file = file
                break

        if not specialization_file:
            return jsonify({"error": "specialization-report CSV not found in extracted files"}), 404

        # Load CSV into pandas
        df = pd.read_csv(specialization_file)

        # Step 1: Remove rows where "Removed From Program" = "Yes"
        if "Removed From Program" in df.columns:
            df = df[df["Removed From Program"].str.strip().str.lower() != "yes"]

        # Step 2: Drop unwanted columns
        drop_columns = [
            "External Id",  # Column C
            "Specialization Slug",  # Column E
            "University",  # Column F
            "Enrollment Time",  # Column G
            "Last Specialization Activity Time",  # Column H
            "# Completed Courses",  # Column I
            "# Courses in Specialization",  # Column J
            "Removed From Program",  # Column L
            "Program Slug",  # Column M
            "Enrollment Source",  # Column O
            "Specialization Completion Time",  # Column P
            "Specialization Certificate URL"  # Column Q
        ]
        df = df.drop(columns=[col for col in drop_columns if col in df.columns], errors="ignore")

        # Step 3: Keep only rows where Completed == "Yes"
        if "Completed" in df.columns:
            df = df[df["Completed"].str.strip().str.lower() == "yes"]

        # Step 4: Remove duplicate emails (only if more than 1 occurrence)
        if "Email" in df.columns:
            duplicate_emails = df["Email"][df["Email"].duplicated(keep=False)]
            if not duplicate_emails.empty:
                df = df.drop_duplicates(subset=["Email"], keep="first")

        # Step 5: Count male/female completions based on Program Name
        male_count = 0
        female_count = 0
        if "Program Name" in df.columns:
            for program in df["Program Name"].dropna():
                prog_lower = program.lower()
                if "female" in prog_lower:
                    female_count += 1
                elif "male" in prog_lower:
                    male_count += 1

        # Save cleaned CSV
        cleaned_file_path = extract_path / "specialization-report-cleaned.csv"
        df.to_csv(cleaned_file_path, index=False)

        # Open the cleaned CSV
        if sys.platform.startswith("win"):  # Windows
            os.startfile(cleaned_file_path)
        elif sys.platform.startswith("darwin"):  # macOS
            subprocess.run(["open", cleaned_file_path])
        else:  # Linux
            subprocess.run(["xdg-open", cleaned_file_path])

        return jsonify({
            "message": f"Zip extracted and cleaned CSV saved to {cleaned_file_path}",
            "cleaned_csv": str(cleaned_file_path),
            "rows_after_cleaning": len(df),
            "male_completed": male_count,
            "female_completed": female_count
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 400


if __name__ == "__main__":
    app.run(debug=True)
