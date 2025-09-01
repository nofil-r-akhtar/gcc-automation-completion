from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import zipfile
import shutil
from pathlib import Path
import pandas as pd

app = Flask(__name__)
CORS(app, resources={
    r"/download-cleaned/*": {"origins": "*"},   # allow CSV download links
    r"/process": {"origins": "*"}               # your main processing endpoint
})

UPLOAD_FOLDER = Path("/tmp/uploads")
UPLOAD_FOLDER.mkdir(parents=True, exist_ok=True)

@app.route("/clean-automated-csv", methods=["POST"])
def extract_and_clean_zip():
    try:
        zip_file = request.files.get("zip_file")
        completed_filter = request.form.get("completed_filter", "yes").strip().lower()

        if not zip_file:
            return jsonify({"error": "zip_file is required"}), 400
        if completed_filter not in ["yes", "no"]:
            return jsonify({"error": "completed_filter must be 'yes' or 'no'"}), 400

        # Clear old files
        for old_file in UPLOAD_FOLDER.glob("*"):
            old_file.unlink()

        # Save uploaded zip
        temp_zip_path = UPLOAD_FOLDER / zip_file.filename
        with open(temp_zip_path, "wb") as buffer:
            shutil.copyfileobj(zip_file.stream, buffer)

        # Extract zip
        with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
            zip_ref.extractall(UPLOAD_FOLDER)
        temp_zip_path.unlink()

        # Find specialization CSV
        specialization_file = None
        for file in UPLOAD_FOLDER.rglob("*.csv"):
            if "specialization-report" in file.name.lower():
                specialization_file = file
                break
        if not specialization_file:
            return jsonify({"error": "specialization-report CSV not found"}), 404

        df = pd.read_csv(specialization_file)

        # Remove unwanted rows
        if "Removed From Program" in df.columns:
            df = df[df["Removed From Program"].str.strip().str.lower() != "yes"]

        # Drop columns
        drop_columns = [
            "External Id", "Specialization Slug", "University", "Enrollment Time",
            "Last Specialization Activity Time", "# Completed Courses", "# Courses in Specialization",
            "Removed From Program", "Program Slug", "Enrollment Source",
            "Specialization Completion Time", "Specialization Certificate URL"
        ]
        df = df.drop(columns=[c for c in drop_columns if c in df.columns], errors="ignore")

        # Filter completed/no
        if "Completed" not in df.columns:
            return jsonify({"error": "'Completed' column missing"}), 400

        if completed_filter == "yes":
            df = df[df["Completed"].str.strip().str.lower() == "yes"]
            if "Email" in df.columns:
                df = df.drop_duplicates(subset=["Email"], keep="first")
        else:
            df_yes = df[df["Completed"].str.strip().str.lower() == "yes"]
            df_no = df[df["Completed"].str.strip().str.lower() != "yes"]
            if "Email" in df.columns:
                learners_with_yes = set(df_yes["Email"].dropna())
                df_no = df_no[~df_no["Email"].isin(learners_with_yes)]
                df_no = df_no.drop_duplicates(subset=["Email"], keep="first")
            df = df_no

        # Convert CSV to bytes
        csv_bytes = df.to_csv(index=False).encode("utf-8")

        # Always save file to disk (so it exists for /download-cleaned)
        cleaned_file_name = f"specialization-report-cleaned-{completed_filter}.csv"
        cleaned_file_path = UPLOAD_FOLDER / cleaned_file_name
        with open(cleaned_file_path, "wb") as f:
            f.write(csv_bytes)


        # If filter is YES → return JSON (counts) + CSV file for download
        if completed_filter == "yes":
            male_count = female_count = 0
            if "Program Name" in df.columns:
                for program in df["Program Name"].dropna():
                    prog_lower = program.lower()
                    if "female" in prog_lower:
                        female_count += 1
                    elif "male" in prog_lower:
                        male_count += 1

            return jsonify({
                "message": "Processing complete",
                "rows_after_cleaning": len(df),
                "male_completed": male_count,
                "female_completed": female_count,
                "download_url": f"{request.host_url}download-cleaned/{cleaned_file_name}"
            })

        # If filter is NO → return CSV file directly
        return jsonify({
            "message": "Processing complete",
            "rows_after_cleaning": len(df),
            "download_url": f"{request.host_url}download-cleaned/{cleaned_file_name}"
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route('/download-cleaned/<filename>')
def download_file(filename):
    file_path = UPLOAD_FOLDER / filename
    if not file_path.exists():
        return jsonify({"error": "File not found"}), 404

    return send_file(
        file_path,
        as_attachment=True,   # forces download
        download_name=filename,  # correct filename
        mimetype='text/csv'
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
