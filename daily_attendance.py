import logging
import time
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

logger = logging.getLogger(__name__)


def get_access_token(CLIENT_ID, CLIENT_SECRET, TOKEN_URL):
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "master_attendance:list master_attendance:update",
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    try:
        response = requests.post(TOKEN_URL, data=data, headers=headers, timeout=30)

        if response.status_code == 200:
            token = response.json().get("access_token")
            logger.info("Access token fetched successfully")
            return token

        logger.error("Error fetching access token: %s", response.text)
        return None

    except requests.RequestException:
        logger.exception("Request failed while fetching access token")
        return None


def get_daily_attendance(DAILY_ATTENDANCE_URL, access_token):
    if not access_token:
        logger.error("No access token")
        return pd.DataFrame()

    attendance = []
    page = 1
    page_size = 1000

    while True:
        headers = {
            "Authorization": f"Bearer {access_token}",
            "X-Page-Number": str(page),
            "X-Page-Size": str(page_size),
            "X-API-Value-Lists": "include",
        }

        params = {
            "attendance_date": date.today().isoformat()
        }

        try:
            response = requests.get(
                DAILY_ATTENDANCE_URL,
                headers=headers,
                params=params,
                timeout=30,
            )

            logger.info("Attendance API page=%s status=%s", page, response.status_code)

            if response.status_code != 200:
                logger.error("Error fetching attendance: %s", response.text)
                break

            attendance_data = response.json()

            if not attendance_data.get("data"):
                break

            attendance.extend(attendance_data["data"])
            logger.info(
                "Fetched %d records from page %d",
                len(attendance_data["data"]),
                page,
            )
            page += 1

        except requests.RequestException:
            logger.exception("Request failed while fetching attendance page %s", page)
            break

    df = pd.DataFrame(attendance)

    if df.empty:
        logger.warning("Attendance DataFrame is empty")
        return df

    required_cols = ["id", "attendance_date", "person_id", "person"]
    df = df[[c for c in required_cols if c in df.columns]].copy()

    df["person_id"] = df["person_id"].astype(str).str.strip()

    logger.info("Fetched total %d attendance rows", len(df))
    return df


def patch_one_attendance(record, access_token, timeout=30):
    student_id = record["student_id"]
    att_id = int(record["id"])
    att_code = int(record["Att Code"])
    note_code = record.get("Note Code", "")

    url = f"https://api.veracross.com/ACSAD/v3/master_attendance/{att_id}"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    payload = {
        "data": {
            "student_attendance_status": att_code,
            "notes": "" if pd.isna(note_code) else str(note_code),
        }
    }

    try:
        response = requests.patch(
            url,
            json=payload,
            headers=headers,
            timeout=timeout,
        )

        if response.status_code == 204:
            logger.info(
                "Updated attendance successfully. student_id=%s att_id=%s att_code=%s",
                student_id,
                att_id,
                att_code,
            )
            return {
                "success": True,
                "student_id": student_id,
                "att_id": att_id,
                "status_code": 204,
                "action": "updated",
            }

        logger.error(
            "Failed update. student_id=%s att_id=%s status=%s response=%s",
            student_id,
            att_id,
            response.status_code,
            response.text,
        )
        return {
            "success": False,
            "student_id": student_id,
            "att_id": att_id,
            "status_code": response.status_code,
            "action": "failed",
            "error": response.text,
        }

    except requests.RequestException as e:
        logger.exception(
            "Request exception while updating attendance. student_id=%s att_id=%s",
            student_id,
            att_id,
        )
        return {
            "success": False,
            "student_id": student_id,
            "att_id": att_id,
            "status_code": None,
            "action": "failed",
            "error": str(e),
        }


def update_attendance(
    student_df,
    google_sheet_df,
    access_token,
    max_workers=5,
    batch_size=50,
    pause_seconds=1,
):
    if student_df is None or student_df.empty:
        return {
            "total": 0,
            "success": 0,
            "failed": 0,
            "skipped": 0,
            "results": [],
            "message": "student_df is empty",
        }

    if google_sheet_df is None or google_sheet_df.empty:
        return {
            "total": 0,
            "success": 0,
            "failed": 0,
            "skipped": 0,
            "results": [],
            "message": "google_sheet_df is empty",
        }

    student_df = student_df.copy()
    google_sheet_df = google_sheet_df.copy()

    google_sheet_df.columns = google_sheet_df.columns.str.strip()
    student_df.columns = student_df.columns.str.strip()

    required_google_cols = ["student_id", "Returned", "Att Code", "Note Code"]
    missing_cols = [col for col in required_google_cols if col not in google_sheet_df.columns]
    if missing_cols:
        logger.error("Missing required Google Sheet columns: %s", missing_cols)
        return {
            "total": 0,
            "success": 0,
            "failed": 0,
            "skipped": 0,
            "results": [],
            "message": f"Missing required Google Sheet columns: {missing_cols}",
        }

    google_sheet_df["student_id"] = google_sheet_df["student_id"].astype(str).str.strip()
    student_df["person_id"] = student_df["person_id"].astype(str).str.strip()

    google_sheet_df["Att Code"] = pd.to_numeric(google_sheet_df["Att Code"], errors="coerce")

    merged_df = google_sheet_df.merge(
        student_df[["id", "person_id"]],
        left_on="student_id",
        right_on="person_id",
        how="left",
    )

    rows_to_update = []
    skipped_results = []

    for _, record in merged_df.iterrows():
        student_id = record.get("student_id")
        returned_value = record.get("Returned")
        att_id = record.get("id")
        att_code = record.get("Att Code")

        if pd.notna(returned_value) and str(returned_value).strip() != "":
            skipped_results.append({
                "success": False,
                "student_id": student_id,
                "att_id": None,
                "status_code": None,
                "action": "skipped",
                "reason": "Returned has value",
            })
            continue

        if pd.isna(att_id):
            skipped_results.append({
                "success": False,
                "student_id": student_id,
                "att_id": None,
                "status_code": None,
                "action": "skipped",
                "reason": "No matching person_id in attendance data",
            })
            continue

        if pd.isna(att_code):
            skipped_results.append({
                "success": False,
                "student_id": student_id,
                "att_id": int(att_id),
                "status_code": None,
                "action": "skipped",
                "reason": "Att Code is not numeric",
            })
            continue

        rows_to_update.append(record.to_dict())

    logger.info("Rows ready for update: %d", len(rows_to_update))
    logger.info("Rows skipped before update: %d", len(skipped_results))

    update_results = []

    for start in range(0, len(rows_to_update), batch_size):
        batch = rows_to_update[start:start + batch_size]

        logger.info(
            "Processing batch %d to %d of %d",
            start + 1,
            start + len(batch),
            len(rows_to_update),
        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(patch_one_attendance, record, access_token)
                for record in batch
            ]

            for future in as_completed(futures):
                update_results.append(future.result())

        time.sleep(pause_seconds)

    all_results = skipped_results + update_results

    success_count = sum(1 for r in all_results if r["action"] == "updated")
    failed_count = sum(1 for r in all_results if r["action"] == "failed")
    skipped_count = sum(1 for r in all_results if r["action"] == "skipped")

    logger.info(
        "Update complete. total=%d success=%d failed=%d skipped=%d",
        len(all_results),
        success_count,
        failed_count,
        skipped_count,
    )

    return {
        "total": len(all_results),
        "success": success_count,
        "failed": failed_count,
        "skipped": skipped_count,
        "results": all_results,
    }

# import requests
# import pandas as pd
# from datetime import date


# today = date.today().isoformat()
# print(today,"date")


# def get_access_token(CLIENT_ID,CLIENT_SECRET,TOKEN_URL):
    
#     """Fetch the access token from Veracross API."""
#     client_id = CLIENT_ID
#     client_secret = CLIENT_SECRET

#     data = {
#         "grant_type": "client_credentials",
#         "client_id": client_id,
#         "client_secret": client_secret,
#         "scope": "master_attendance:list master_attendance:update"
#     }
#     headers = {"Content-Type": "application/x-www-form-urlencoded"}

#     response = requests.post(TOKEN_URL, data=data, headers=headers)

#     if response.status_code == 200:
#         return response.json().get("access_token")
#     else:
#         print("Error fetching access token:", response.text)
#         return None


# def get_daily_attendance(DAILY_ATTENDANCE_URL,access_token):
#     """Fetch all student data using pagination via headers."""
#     access_token = access_token
#     if not access_token:
#         print("No access token")
#         return

#     attendance = []
#     page = 1
#     page_size = 1000  # Max allowed is 1000, but we start with 100
#     print("here")
#     while True:
#         headers = {
#             "Authorization": f"Bearer {access_token}",
#             "X-Page-Number": str(page),
#             "X-Page-Size": str(page_size),
#             "X-API-Value-Lists" : "include"
#         }

#         params = {
#             "attendance_date": date.today().isoformat()
#         }

#         response = requests.get(DAILY_ATTENDANCE_URL, headers=headers, params=params)
#         print("got response for attendance")

#         if response.status_code == 200:
#             attendance_data = response.json()
#             if attendance_data["data"] == []:
#                 break
            

#             print(len(attendance_data["data"]))
#             print("got it")
#             attendance.extend(attendance_data["data"])
#             page += 1  
#             print("page")
#         else:
#             print("Error fetching students:", response.text)
#             break

#     df = pd.DataFrame(attendance)
#     df = df[["id","attendance_date","person_id","person"]]
#     print(df,"fdf")
    
#     return df



# def update_attendance(student_df,google_sheet_df, access_token):
#     student_df = student_df
    
#     for record in google_sheet_df.values():
#         att_id = student_df[record["student_id"]]["id"] # i want to get student df of "id" where student id is common in both df
#         if record["Returned"] != None: #pass if returned column has any date or value
#             continue
#         url = f"https://api.veracross.com/ACSAD/v3/master_attendance/{att_id}"

#         headers = {
#             "Authorization": f"Bearer {access_token}",
#             "Content-Type": "application/json"
#         }

#         payload = {
#             "data": {
#                 "student_attendance_status": record["Att Code"] ,
#                 "notes": record["Note Code"]
#             }
#         }
#         try:
#             response = requests.patch(url, json=payload, headers=headers)

#             if response.status_code == 204:
#                 return {
#                     "success": True,
#                     "att_id": att_id,
#                     "status_code": 204
#                 }

#             else:
#                 return {
#                     "success": False,
#                     "att_id": att_id,
#                     "status_code": response.status_code,
#                     "error": response.text
#                 }

#         except Exception as e:
#             return {
#                 "success": False,
#                 "att_id": att_id,
#                 "status_code": None,
#                 "error": str(e)
#             }
