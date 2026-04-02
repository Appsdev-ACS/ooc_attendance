import os
import sys
import logging

import pandas as pd
import gspread
from flask import Flask
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from google.auth import default


from daily_attendance import (
    get_access_token,
    get_daily_attendance,
    update_attendance,
)

app = Flask(__name__)
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

TOKEN_URL = "https://accounts.veracross.com/acsad/oauth/token"
DAILY_ATTENDANCE_URL = "https://api.veracross.com/ACSAD/v3/master_attendance"

CLIENT_ID = os.getenv("VC_OLD_CLIENT_ID")
CLIENT_SECRET = os.getenv("VC_OLD_CLIENT_SECRET")

SERVICE_ACCOUNT_FILE = "service-account.json"
SPREADSHEET_NAME = "Family Return Data"
SHEET_NAME = "OOCAttend"


def get_google_sheet_df():
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    # creds, _ = default(scopes=[
    #     "https://www.googleapis.com/auth/spreadsheets",
    #     "https://www.googleapis.com/auth/drive"
    # ])
    client = gspread.authorize(creds)

    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet(SHEET_NAME)
        records = sheet.get_all_records()
        df = pd.DataFrame(records)

        if df.empty:
            logger.warning(
                "Google Sheet is empty. Spreadsheet='%s', Sheet='%s'",
                SPREADSHEET_NAME,
                SHEET_NAME,
            )
            return df

        df.columns = df.columns.str.strip()

        # Optional rename if your first columns vary
        rename_map = {}
        if len(df.columns) > 0 and df.columns[0] == "Date":
            rename_map["Date"] = "student_id"
        if len(df.columns) > 1 and df.columns[1] not in ["student_name"]:
            rename_map[df.columns[1]] = "student_name"

        if rename_map:
            df = df.rename(columns=rename_map)

        logger.info(
            "Loaded %d rows from spreadsheet '%s', sheet '%s'",
            len(df),
            SPREADSHEET_NAME,
            SHEET_NAME,
        )
        logger.info("Google Sheet columns: %s", df.columns.tolist())

        return df

    except gspread.SpreadsheetNotFound:
        logger.exception(
            "Spreadsheet '%s' not found. Check name and service account permissions.",
            SPREADSHEET_NAME,
        )
        raise RuntimeError(
            f"Spreadsheet '{SPREADSHEET_NAME}' not found. Check name and permissions."
        )
    except gspread.WorksheetNotFound:
        logger.exception(
            "Worksheet '%s' not found in spreadsheet '%s'.",
            SHEET_NAME,
            SPREADSHEET_NAME,
        )
        raise RuntimeError(
            f"Worksheet '{SHEET_NAME}' not found in spreadsheet '{SPREADSHEET_NAME}'."
        )


@app.route("/run")
def run_job():
    try:
        google_sheet_df = get_google_sheet_df()
        logger.info("Loaded google sheet rows: %d", len(google_sheet_df))

        access_token = get_access_token(CLIENT_ID, CLIENT_SECRET, TOKEN_URL)
        if not access_token:
            return {"error": "Failed to get access token"}, 500

        student_df = get_daily_attendance(DAILY_ATTENDANCE_URL, access_token)
        logger.info("Loaded attendance rows: %d", len(student_df))

        result = update_attendance(
            student_df=student_df,
            google_sheet_df=google_sheet_df,
            access_token=access_token,
            max_workers=3,
            batch_size=25,
            pause_seconds=5,
        )
        failed_results = [r for r in result["results"] if r["action"] == "failed"]


        return {
            "message": "Completed",
            "total": result["total"],
            "success": result["success"],
            "failed": result["failed"],
            "skipped": result["skipped"],
            "failed_samples": failed_results[:10],
        }, 200

    except Exception:
        logger.exception("Error in /run")
        return {"error": "Internal Server Error"}, 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)


# import pandas as pd
# import gspread
# from google.oauth2.service_account import Credentials
# import os
# from google.auth import default
# from daily_attendance import get_daily_attendance,get_access_token,update_attendance
# from dotenv import load_dotenv
# from concurrent.futures import ThreadPoolExecutor
# from flask import Flask, jsonify
# import logging
# import sys

# app = Flask(__name__)
# load_dotenv()


# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s %(levelname)s %(name)s %(message)s",
#     stream=sys.stdout,
# )

# logger = logging.getLogger(__name__)
# # Configuration
# TOKEN_URL = "https://accounts.veracross.com/acsad/oauth/token"
# DAILY_ATTENDANCE_URL = "https://api.veracross.com/ACSAD/v3/master_attendance"

# CLIENT_ID = os.getenv("CLIENT_ID")

# CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# SERVICE_ACCOUNT_FILE = "service-account.json"  # Update this path
# SPREADSHEET_NAME = "Family Return Data"
# SHEET_NAME = "OOCAttend"

# def get_google_sheet_df():
#     """Uploads the DataFrame to Google Sheets."""
#     creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
#     # creds, _ = default(scopes=[
#     #     "https://www.googleapis.com/auth/spreadsheets",
#     #     "https://www.googleapis.com/auth/drive"
#     # ])
#     client = gspread.authorize(creds)

#     # Open or create the Google Sheet
#     try:
#         sheet = client.open(SPREADSHEET_NAME).worksheet(SHEET_NAME)
#         records = sheet.get_all_records()
#         df = pd.DataFrame(records)
#         if not df.empty:
#             cols = list(df.columns)

#             if len(cols) > 0 and cols[0] == "Date":
#                 cols[0] = "student_id"
#             if len(cols) > 1:
#                 cols[1] = "student_name"

#             df.columns = cols
#         logger.info("Loaded %d rows from spreadsheet '%s', sheet '%s'",
#                     len(df), SPREADSHEET_NAME, SHEET_NAME)
        
#         return df

#     except gspread.SpreadsheetNotFound:
#         logger.exception(
#             "Spreadsheet '%s' not found. Check spreadsheet name and service account permissions.",
#             SPREADSHEET_NAME,
#         )
#         raise RuntimeError(
#             f"Spreadsheet '{SPREADSHEET_NAME}' not found. Check name and permissions."
#         )

# @app.route("/run")
# def run_job():
#     try:
#         google_sheet_df = get_google_sheet_df()
#         logger.info("Loaded google sheet rows: %d", len(google_sheet_df))

#         access_token = get_access_token(CLIENT_ID, CLIENT_SECRET, TOKEN_URL)
#         if not access_token:
#             return {"error": "Failed to get access token"}, 500

#         student_df = get_daily_attendance(DAILY_ATTENDANCE_URL, access_token)
#         logger.info("Loaded attendance rows: %d", len(student_df))

#         result = update_attendance(
#             student_df=student_df,
#             google_sheet_df=google_sheet_df,
#             access_token=access_token
#         )

#         return {
#             "message": "Completed",
#             "total": result["total"],
#             "success": result["success"],
#             "failed": result["failed"],
#             "skipped": result["skipped"],
#             "sample_results": result["results"][:10]
#         }, 200

#     except Exception:
#         logger.exception("Error in /run")
#         return {"error": "Internal Server Error"}, 500



# # @app.route("/run")
# # def run_job():
# #     google_sheet_df = get_google_sheet_df()
# #     print(google_sheet_df)
# #     try:
# #         access_token = get_access_token(CLIENT_ID, CLIENT_SECRET, TOKEN_URL)
# #         student_df = get_daily_attendance(DAILY_ATTENDANCE_URL,access_token)

# #         result = update_attendance(student_df = student_df,google_sheet_df=google_sheet_df, access_token=access_token)

# #         if result["success"]:
# #             return {"message": "Success"}, 200
# #         else:
# #             return {"error": result["error"]}, 500

# #     except Exception:
# #         logger.exception("Error in /test")
# #         return {"error": "Internal Server Error"}, 500


# # @app.route("/get_sheet_value")
# # def get_sheet_value():
# #     google_sheet = get_google_sheet_df()
# #     print(google_sheet)
    
# #     return "success"
    
    
# if __name__ == "__main__":
#     # app.run() #staging
#     port = int(os.environ.get("PORT", 8080))
#     app.run(host="0.0.0.0", port=port)