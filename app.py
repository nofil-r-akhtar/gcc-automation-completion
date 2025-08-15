from flask import Flask, request, jsonify
from flask_cors import CORS  # Import CORS
import zipfile
import shutil
from pathlib import Path
import os
import subprocess
import sys
import pandas as pd

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

@app.route("/clean-automated-csv", methods=["POST"])
def extract_and_clean_zip():
    try:
        # Get uploaded file, extraction path, and filter from form
        zip_file = request.files.get("zip_file")
        extract_to = request.form.get("extract_to")
        completed_filter = request.form.get("completed_filter", "yes").strip().lower()  # default "yes"

        if not zip_file or not extract_to:
            return jsonify({"error": "zip_file and extract_to are required"}), 400
        if completed_filter not in ["yes", "no"]:
            return jsonify({"error": "completed_filter must be 'yes' or 'no'"}), 400

        # Ensure the extraction directory exists
        extract_path = Path(extract_to)
        extract_path.mkdir(parents=True, exist_ok=True)

        # Save uploaded zip file temporarily
        temp_zip_path = extract_path / zip_file.filename
        with open(temp_zip_path, "wb") as buffer:
            shutil.copyfileobj(zip_file.stream, buffer)

        # Extract the zip file
        with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
        temp_zip_path.unlink()  # remove the zip

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
            "External Id", "Specialization Slug", "University", "Enrollment Time",
            "Last Specialization Activity Time", "# Completed Courses", "# Courses in Specialization",
            "Removed From Program", "Program Slug", "Enrollment Source",
            "Specialization Completion Time", "Specialization Certificate URL"
        ]
        df = df.drop(columns=[col for col in drop_columns if col in df.columns], errors="ignore")

        # Step 3: Apply completion filter logic
        if "Completed" not in df.columns:
            return jsonify({"error": "'Completed' column is missing in CSV"}), 400

        if completed_filter == "yes":
            df = df[df["Completed"].str.strip().str.lower() == "yes"]
            if "Email" in df.columns:
                df = df.drop_duplicates(subset=["Email"], keep="first")

        elif completed_filter == "no":
            df_yes = df[df["Completed"].str.strip().str.lower() == "yes"]
            df_no = df[df["Completed"].str.strip().str.lower() != "yes"]
            if "Email" in df.columns:
                learners_with_yes = set(df_yes["Email"].dropna())
                df_no = df_no[~df_no["Email"].isin(learners_with_yes)]
                df_no = df_no.drop_duplicates(subset=["Email"], keep="first")
            df = df_no

        # Step 4: Count male/female (only for completed_filter = "yes")
        male_count = 0
        female_count = 0
        if completed_filter == "yes" and "Program Name" in df.columns:
            for program in df["Program Name"].dropna():
                prog_lower = program.lower()
                if "female" in prog_lower:
                    female_count += 1
                elif "male" in prog_lower:
                    male_count += 1

        # Save cleaned CSV
        cleaned_file_path = extract_path / f"specialization-report-cleaned-{completed_filter}.csv"
        df.to_csv(cleaned_file_path, index=False)

        # Try opening the cleaned CSV (ignored on servers)
        try:
            if sys.platform.startswith("win"):
                os.startfile(cleaned_file_path)
            elif sys.platform.startswith("darwin"):
                subprocess.run(["open", cleaned_file_path])
            else:
                subprocess.run(["xdg-open", cleaned_file_path])
        except Exception:
            pass

        return jsonify({
            "message": f"Zip extracted and cleaned CSV saved to {cleaned_file_path}",
            "cleaned_csv": str(cleaned_file_path),
            "rows_after_cleaning": len(df),
            "male_completed": male_count if completed_filter == "yes" else None,
            "female_completed": female_count if completed_filter == "yes" else None
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 400

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
