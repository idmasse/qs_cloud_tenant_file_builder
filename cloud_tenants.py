import pandas as pd
from pathlib import Path

# file paths for the source files
TENANTS_FILE = r""
ALTERNATE_TENANTS_FILE = r""
OUTPUT_FILE = r""
SOURCE_SHEET_NAME = 0

BAD_EMAILS = {
    "quikstorcloud@gmail.com",
    "#noemail",
    "cloud_tenant@quikstor.com",
    "no@email.com",
    "none@none.com",

}

QMS_COLUMNS = [
    "Email", "FirstName", "LastName", "CompanyName", "Gender", "DateOfBirth",
    "AddressStreet1", "AddressStreet2", "AddressCity", "AddressCountry",
    "AddressState", "AddressPostalCode", "PhoneNumberPrefix", "PhoneNumber",
    "PhoneNumberType", "AlternateContactFirstName", "AlternateContactLastName",
    "AlternateContactEmail", "AlternateContactRelationship",
    "AlternateContactAddressStreet1", "AlternateContactAddressStreet2",
    "AlternateContactAddressCity", "AlternateContactAddressCountry",
    "AlternateContactAddressState", "AlternateContactAddressPostalCode",
    "AlternateContactPhoneNumberPrefix", "AlternateContactPhoneNumber",
    "AlternateContactPhoneNumberType", "MilitaryProfileBranchOfService",
    "MilitaryProfileRank", "MilitaryProfileIsRetired", "MilitaryProfilePlaceOfBirth",
    "MilitaryProfileMilitaryUnit", "MilitaryProfileSquadron",
    "MilitaryProfileMilitaryEmail", "MilitaryProfileLastFourSsnDigits",
    "MilitaryProfileDivision", "MilitaryProfileTypeOfService",
    "MilitaryProfileCurrentDutyLocationStreet1",
    "MilitaryProfileCurrentDutyLocationStreet2",
    "MilitaryProfileCurrentDutyLocationCity",
    "MilitaryProfileCurrentDutyLocationCountry",
    "MilitaryProfileCurrentDutyLocationState",
    "MilitaryProfileCurrentDutyLocationPostalCode",
    "MilitaryProfileDateEnteredService",
    "MilitaryProfileEndOfActiveServiceDate", "MilitaryProfileMilitaryId",
    "MilitaryProfileCommandingOfficerFirstName",
    "MilitaryProfileCommandingOfficerLastName",
    "MilitaryProfileCommandingOfficerPhoneNumberPrefix",
    "MilitaryProfileCommandingOfficerPhoneNumber",
    "MilitaryProfileAgentFirstName", "MilitaryProfileAgentLastName",
    "MilitaryProfileAgentEmail", "MilitaryProfileAgentPhoneNumberPrefix",
    "MilitaryProfileAgentPhoneNumber", "MilitaryProfileAgentAddressStreet1",
    "MilitaryProfileAgentAddressStreet2", "MilitaryProfileAgentAddressCity",
    "MilitaryProfileAgentAddressCountry", "MilitaryProfileAgentAddressState",
    "MilitaryProfileAgentAddressPostalCode", "DriverLicenseNumber",
    "DriverLicenseState", "DriverLicenseExpirationDate",
]

# LegacyTenantId for joining other sheets later
EXTRA_KEY_COLUMNS = ["LegacyTenantId"]

def load_source(path: str | Path, sheet_name=0) -> pd.DataFrame:
    path = Path(path)
    if path.suffix.lower() in [".xlsx", ".xls"]:
        df = pd.read_excel(path, sheet_name=sheet_name, dtype=str)
    else:
        df = pd.read_csv(path, dtype=str)
    # Ensure all columns are string and strip whitespace
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    return df

def choose_best_phone(row: pd.Series) -> tuple[str, str, str]:
    """
    Choose the best phone in this order:
      1. CellPhoneNumber  -> 'Cell Phone'
      2. HomePhoneNumber  -> 'Home Phone'
      3. WorkPhoneNumber  -> 'Work Phone'

    Returns: (phone_number, phone_prefix, phone_type)

    - Prefix is always '+1' when a number exists.
    - Treats 'nan' and '' as empty.
    """
    def is_filled(val) -> bool:
        if val is None:
            return False
        s = str(val).strip()
        return s != "" and s.lower() != "nan"

    phone_candidates = [
        ("CellPhoneNumber",  "Cell Phone"),
        ("HomePhoneNumber",  "Home Phone"),
        ("WorkPhoneNumber",  "Work Phone"),
    ]

    for num_col, phone_type in phone_candidates:
        num = row.get(num_col)
        if is_filled(num):
            return str(num).strip(), "+1", phone_type

    return "", "", ""


def combine_middle_last(middle: str, last: str) -> str:
    """
    Combines MiddleName + LastName safely.
    Removes 'nan', trims spaces, returns just LastName if middle is blank.
    """
    parts = []

    # normalize middle name
    if middle and str(middle).strip().lower() != "nan":
        parts.append(str(middle).strip())

    # normalize last name
    if last and str(last).strip().lower() != "nan":
        parts.append(str(last).strip())

    return " ".join(parts)



def transform_to_qms(df_src: pd.DataFrame) -> pd.DataFrame:
    # start with empty QMS dataframe
    df_qms = pd.DataFrame(columns=QMS_COLUMNS)

    if "Email" in df_src.columns:
    # normalize emails and clear banned ones
        cleaned_emails = []
        for email in df_src["Email"].astype(str):
            email_lower = email.strip().lower()
            if email_lower in BAD_EMAILS:
                cleaned_emails.append("")   # remove banned email
            elif email_lower == "nan":
                cleaned_emails.append("")   # remove nan-like values
            else:
                cleaned_emails.append(email.strip())
        df_qms["Email"] = cleaned_emails


    if "FirstName" in df_src.columns:
        df_qms["FirstName"] = df_src["FirstName"]

    if "LastName" in df_src.columns:
        combined_lastnames = []
        for i, row in df_src.iterrows():
            middle = row.get("MiddleName", "")
            last = row.get("LastName", "")
            combined_lastnames.append(combine_middle_last(middle, last))
        df_qms["LastName"] = combined_lastnames

    if "CompanyName" in df_src.columns:
        df_qms["CompanyName"] = df_src["CompanyName"]

    if "DateOfBirth" in df_src.columns:
        df_qms["DateOfBirth"] = df_src["DateOfBirth"]

    # address mappings
    df_qms["AddressStreet1"] = df_src.get("AddressLine", "")
    df_qms["AddressStreet2"] = df_src.get("AddressLineOptional", "")
    df_qms["AddressCity"] = df_src.get("City", "")
    df_qms["AddressState"] = df_src.get("State", "")
    df_qms["AddressPostalCode"] = df_src.get("PostalCode", "")
    df_qms["AddressCountry"] = df_src.get("Country", "")

    # phone logic: choose cell -> home -> work, and set type accordingly
    phone_numbers = []
    phone_prefixes = []
    phone_types = []

    for _, row in df_src.iterrows():
        num, prefix, phone_type = choose_best_phone(row)
        phone_numbers.append(num)
        phone_prefixes.append(prefix)
        phone_types.append(phone_type)

    df_qms["PhoneNumber"] = phone_numbers
    df_qms["PhoneNumberPrefix"] = phone_prefixes
    df_qms["PhoneNumberType"] = phone_types

    # drivers license mappings
    df_qms["DriverLicenseNumber"] = df_src.get("DriversLicense", "")
    df_qms["DriverLicenseState"] = df_src.get("DriversLicenseState", "")
    # no expiration info in source – leave blank
    df_qms["DriverLicenseExpirationDate"] = ""

    # eEverything else not present in the source stays as empty string
    for col in QMS_COLUMNS:
        if col not in df_qms.columns:
            df_qms[col] = ""

    # optionally attach LegacyTenantId outside the QMS structure for later merging
    for key_col in EXTRA_KEY_COLUMNS:
        if key_col in df_src.columns:
            df_qms[key_col] = df_src[key_col]
        else:
            df_qms[key_col] = ""

    # reorder columns: QMS columns first, then extra key columns
    df_qms = df_qms[QMS_COLUMNS + EXTRA_KEY_COLUMNS]

    return df_qms


def merge_alternate_contacts(df_qms: pd.DataFrame, alt_path: str | Path) -> pd.DataFrame:
    """
    Load alternate_tenants file and merge its data into the QMS dataframe
    using LegacyTenantId.

    NOTE: If there are multiple alternate rows for the same LegacyTenantId,
    this keeps the FIRST one and ignores the rest.
    """
    alt_path = Path(alt_path)
    df_alt = load_source(alt_path)
    df_alt = df_alt.dropna(how="all")

    # normalize LegacyTenantId
    df_alt["LegacyTenantId"] = df_alt["LegacyTenantId"].astype(str).str.strip()
    df_qms["LegacyTenantId"] = df_qms["LegacyTenantId"].astype(str).str.strip()

    # remove banned emails from alternate contacts
    cleaned_alt_emails = []
    for email in df_alt["Email"].astype(str):
        email_lower = email.strip().lower()
        if email_lower in BAD_EMAILS:
            cleaned_alt_emails.append("")
        elif email_lower == "nan":
            cleaned_alt_emails.append("")
        else:
            cleaned_alt_emails.append(email.strip())
    df_alt["Email"] = cleaned_alt_emails

    # combine alternate middle + last name
    alt_combined_lastnames = []
    for _, row in df_alt.iterrows():
        middle = row.get("MiddleName", "")
        last = row.get("LastName", "")
        alt_combined_lastnames.append(combine_middle_last(middle, last))

    # build mapped alternate-contact dataframe
    alt_phone_numbers = []
    alt_phone_prefixes = []
    alt_phone_types = []

    for _, row in df_alt.iterrows():
        num, prefix, phone_type = choose_best_phone(row)
        alt_phone_numbers.append(num)
        alt_phone_prefixes.append(prefix)
        alt_phone_types.append(phone_type)

    df_alt_mapped = pd.DataFrame({
        "LegacyTenantId": df_alt.get("LegacyTenantId", ""),
        "AlternateContactFirstName": df_alt.get("FirstName", ""),
        "AlternateContactLastName": alt_combined_lastnames,
        "AlternateContactEmail": df_alt.get("Email", ""),
        "AlternateContactRelationship": df_alt.get("Relationship", ""),
        "AlternateContactAddressStreet1": df_alt.get("AddressLine", ""),
        "AlternateContactAddressStreet2": df_alt.get("AddressLineOptional", ""),
        "AlternateContactAddressCity": df_alt.get("City", ""),
        "AlternateContactAddressState": df_alt.get("State", ""),
        "AlternateContactAddressPostalCode": df_alt.get("PostalCode", ""),
        "AlternateContactAddressCountry": df_alt.get("Country", ""),
        "AlternateContactPhoneNumber": alt_phone_numbers,
        "AlternateContactPhoneNumberPrefix": alt_phone_prefixes,
        "AlternateContactPhoneNumberType": alt_phone_types,
    })

    # use LegacyTenantId as index and ensure uniqueness
    df_alt_mapped = df_alt_mapped.set_index("LegacyTenantId")
    df_alt_mapped = df_alt_mapped[~df_alt_mapped.index.duplicated(keep="first")]

    # for each AlternateContact column, map by LegacyTenantId
    for col in [
        "AlternateContactFirstName",
        "AlternateContactLastName",
        "AlternateContactEmail",
        "AlternateContactRelationship",
        "AlternateContactAddressStreet1",
        "AlternateContactAddressStreet2",
        "AlternateContactAddressCity",
        "AlternateContactAddressCountry",
        "AlternateContactAddressState",
        "AlternateContactAddressPostalCode",
        "AlternateContactPhoneNumber",
        "AlternateContactPhoneNumberPrefix",
        "AlternateContactPhoneNumberType",
    ]:
        if col in df_qms.columns:
            df_qms[col] = df_qms["LegacyTenantId"].map(df_alt_mapped[col]).fillna(df_qms[col])
        else:
            df_qms[col] = df_qms["LegacyTenantId"].map(df_alt_mapped[col]).fillna("")

    return df_qms


def main():
    df_src = load_source(TENANTS_FILE, sheet_name=SOURCE_SHEET_NAME)
    df_src = df_src.dropna(how="all")

    df_qms = transform_to_qms(df_src)

    # merge in alternate contacts
    df_qms = merge_alternate_contacts(df_qms, ALTERNATE_TENANTS_FILE)

    # create duplicatecheck column
    df_qms["duplicatecheck"] = (
        df_qms["Email"].fillna("").astype(str)
        + df_qms["FirstName"].fillna("").astype(str)
        + df_qms["LastName"].fillna("").astype(str)
        + df_qms["AddressStreet1"].fillna("").astype(str)
    )

    # remove all spaces from duplicatecheck
    df_qms["duplicatecheck"] = df_qms["duplicatecheck"].str.replace(" ", "", regex=False)

    # create firstLast column
    df_qms["firstLast"] = (
        df_qms["FirstName"].fillna("").astype(str)
        + df_qms["LastName"].fillna("").astype(str)
    )

    # remove all spaces from firstLast
    df_qms["firstLast"] = df_qms["firstLast"].str.replace(" ", "", regex=False)

    # clean up NaNs
    df_qms = df_qms.fillna("")

    # reorder columns so:
    # A = LegacyTenantId
    # B = duplicatecheck
    # C = firstLast
    cols = df_qms.columns.tolist()

    # remove these from wherever they currently are
    cols.remove("LegacyTenantId")
    cols.remove("duplicatecheck")
    cols.remove("firstLast")

    # rebuild final order
    new_order = ["LegacyTenantId", "duplicatecheck", "firstLast"] + cols

    df_qms = df_qms[new_order]

    # save
    df_qms.to_csv(OUTPUT_FILE, index=False)
    print(f"Written QMS-formatted data to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()