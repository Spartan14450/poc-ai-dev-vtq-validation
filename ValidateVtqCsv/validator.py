# ValidateVtqCsv/validator.py

import csv
import re
from dataclasses import dataclass
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional, Set, TextIO

# ---------------------------------------------------------------------------
# Specification constants
# ---------------------------------------------------------------------------

EXPECTED_COLUMNS: List[str] = [
    "RecordType",
    "UniqueCandidateIdentifier",
    "UniqueLearnerNumber",
    "UniquePupilNumber",
    "CandidateIdentifierOther",
    "FirstName",
    "MiddleNames",
    "Surname",
    "Sex",
    "DOB",
    "NCN",
    "CentreURN",
    "LAESTAB",
    "CentreIdentifierOther",
    "UKPRN",
    "CentreName",
    "CentreAddress1",
    "CentreAddress2",
    "CentreAddress3",
    "CentreAddress4",
    "CentrePostcode",
    "CentreType",
    "QualificationNumberType",
    "QualificationNumber",
    "SpecificationCode",
    "SpecificationTitle",
    "QualificationType",
    "ExamSeries",
    "RegistrationDate",
    "FirstEntryDate",
    "AwardDate",
    "QualificationGrade",
    "PrivateCandidate",
    "PartialAbsence",
]

SENTINEL_DATES = {"2999-12-31", "31/12/2999"}

# Simplified union of UK / BFPO / Irish postcode patterns
UK_POSTCODE_RE = r"[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}"
BFPO_POSTCODE_RE = r"BFPO\s*\d{1,4}"
IRISH_POSTCODE_RE = r"[AC-FHKNPRTV-Y]\d{1,2}[0-9A-Z]?\s*[0-9AC-FHKNPRTV-Y]{4}"
CENTRE_POSTCODE_RE = rf"^({UK_POSTCODE_RE}|{BFPO_POSTCODE_RE}|{IRISH_POSTCODE_RE})$"

QUALIFICATION_TYPE_VALUES = {
    "Alternative Academic Qualification",
    "Apprenticeship Assessment Qualification",
    "Digital Functional Skills Qualification",
    "End-Point Assessment",
    "English For Speakers of Other Languages",
    "Essential Digital Skills",
    "Essential Skills (Northern Ireland)",
    "Functional Skills",
    "Key Skills",
    "Occupational Qualification",
    "Other General Qualification",
    "Other Vocational Qualification",
    "Other Life Skills Qualification",
    "Performing Arts Graded Examination",
    "QCF",
    "Technical Occupation Qualification",
    "Technical Qualification",
    "Vocationally-Related Qualification",
}


@dataclass
class FieldSpec:
    name: str
    mandatory: bool = False
    allow_minus2: bool = False
    length: Optional[int] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    allowed_values: Optional[Set[str]] = None
    pattern: Optional[str] = None
    type: Optional[str] = None  # e.g. "date"


FIELD_SPECS: Dict[str, FieldSpec] = {
    "RecordType": FieldSpec(
        name="RecordType",
        mandatory=True,
        allowed_values={"Entry", "Result", "Amendment", "Deletion"},
    ),
    "UniqueCandidateIdentifier": FieldSpec(
        name="UniqueCandidateIdentifier",
        mandatory=False,           # optional individually, group rule enforced elsewhere
        allow_minus2=True,
        length=13,
        pattern=r"^[A-Z0-9]{13}$",
    ),
    "UniqueLearnerNumber": FieldSpec(
        name="UniqueLearnerNumber",
        mandatory=False,
        allow_minus2=True,
        length=10,
        pattern=r"^\d{10}$",
    ),
    "UniquePupilNumber": FieldSpec(
        name="UniquePupilNumber",
        mandatory=False,
        allow_minus2=True,
        length=13,
        pattern=r"^[A-Z0-9]{13}$",
    ),
    "CandidateIdentifierOther": FieldSpec(
        name="CandidateIdentifierOther",
        mandatory=False,
        allow_minus2=True,
        min_length=1,
        max_length=20,
    ),
    "FirstName": FieldSpec(
        name="FirstName",
        mandatory=True,
        min_length=1,
        max_length=150,
    ),
    "MiddleNames": FieldSpec(
        name="MiddleNames",
        mandatory=False,
        min_length=1,
        max_length=150,
    ),
    "Surname": FieldSpec(
        name="Surname",
        mandatory=True,
        min_length=1,
        max_length=150,
    ),
    "Sex": FieldSpec(
        name="Sex",
        mandatory=True,
        allow_minus2=True,
        allowed_values={"M", "F"},
    ),
    "DOB": FieldSpec(
        name="DOB",
        mandatory=False,
        type="date",
    ),
    "NCN": FieldSpec(
        name="NCN",
        mandatory=True,
        allow_minus2=True,
        length=5,
        pattern=r"^\d{5}$",
    ),
    "CentreURN": FieldSpec(
        name="CentreURN",
        mandatory=True,
        allow_minus2=True,
        length=6,
        pattern=r"^\d{6}$",
    ),
    "LAESTAB": FieldSpec(
        name="LAESTAB",
        mandatory=True,
        allow_minus2=True,
        length=8,
    ),
    "CentreIdentifierOther": FieldSpec(
        name="CentreIdentifierOther",
        mandatory=True,
        allow_minus2=True,
        min_length=1,
        max_length=15,
    ),
    "UKPRN": FieldSpec(
        name="UKPRN",
        mandatory=True,
        allow_minus2=True,
        length=8,
        pattern=r"^\d{8}$",
    ),
    "CentreName": FieldSpec(
        name="CentreName",
        mandatory=False,
        min_length=1,
        max_length=150,
    ),
    "CentreAddress1": FieldSpec(
        name="CentreAddress1",
        mandatory=False,
        min_length=1,
        max_length=150,
    ),
    "CentreAddress2": FieldSpec(
        name="CentreAddress2",
        mandatory=False,
        min_length=1,
        max_length=150,
    ),
    "CentreAddress3": FieldSpec(
        name="CentreAddress3",
        mandatory=False,
        min_length=1,
        max_length=150,
    ),
    "CentreAddress4": FieldSpec(
        name="CentreAddress4",
        mandatory=False,
        min_length=1,
        max_length=150,
    ),
    "CentrePostcode": FieldSpec(
        name="CentrePostcode",
        mandatory=True,
        allow_minus2=True,
        pattern=CENTRE_POSTCODE_RE,
    ),
    "CentreType": FieldSpec(
        name="CentreType",
        mandatory=True,
        allow_minus2=True,
        allowed_values={str(i) for i in range(1, 14)},
    ),
    "QualificationNumberType": FieldSpec(
        name="QualificationNumberType",
        mandatory=True,
        allowed_values={"QAN", "Other"},
    ),
    "QualificationNumber": FieldSpec(
        name="QualificationNumber",
        mandatory=True,
        min_length=1,
        max_length=10,
    ),
    "SpecificationCode": FieldSpec(
        name="SpecificationCode",
        mandatory=True,
        allow_minus2=True,
        min_length=1,
        max_length=20,
    ),
    "SpecificationTitle": FieldSpec(
        name="SpecificationTitle",
        mandatory=True,
        min_length=1,
        max_length=250,
    ),
    "QualificationType": FieldSpec(
        name="QualificationType",
        mandatory=True,
        allowed_values=QUALIFICATION_TYPE_VALUES,
    ),
    "ExamSeries": FieldSpec(
        name="ExamSeries",
        mandatory=True,
        allow_minus2=True,
        pattern=r"^(January|February|March|April|May|June|July|August|September|October|November|December) \d{4}$",
    ),
    "RegistrationDate": FieldSpec(
        name="RegistrationDate",
        mandatory=True,
        type="date",
    ),
    "FirstEntryDate": FieldSpec(
        name="FirstEntryDate",
        mandatory=False,  # conditional
        type="date",
    ),
    "AwardDate": FieldSpec(
        name="AwardDate",
        mandatory=False,  # conditional
        type="date",
    ),
    "QualificationGrade": FieldSpec(
        name="QualificationGrade",
        mandatory=False,  # conditional
        allow_minus2=True,
        min_length=1,
        max_length=20,
    ),
    "PrivateCandidate": FieldSpec(
        name="PrivateCandidate",
        mandatory=True,
        allow_minus2=True,
        allowed_values={"0", "1"},
    ),
    "PartialAbsence": FieldSpec(
        name="PartialAbsence",
        mandatory=True,
        allow_minus2=True,
        allowed_values={"0", "1"},
    ),
}

# Fields where -2 is explicitly allowed by the exclusions doc
EXCLUSION_MINUS2_FIELDS = {
    "UKPRN",
    "UniqueCandidateIdentifier",
    "UniqueLearnerNumber",
    "UniquePupilNumber",
    "NCN",
    "CentreURN",
    "LAESTAB",
    "SpecificationCode",
    "ExamSeries",
    "QualificationGrade",
    "PrivateCandidate",
    "PartialAbsence",
}

DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y")


@dataclass
class ValidationError:
    row_number: int
    field: str
    error_code: str
    message: str
    value: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_date(value: str) -> Tuple[Optional[date], bool]:
    """
    Parse date string into Python date or mark as sentinel.
    Returns (date_obj, is_sentinel).
    """
    v = (value or "").strip()
    if not v:
        return None, False
    if v in SENTINEL_DATES:
        return None, True

    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(v, fmt).date()
            return dt, False
        except ValueError:
            continue
    return None, False


def is_permitted_exclusion(field: str, value: str, error_code: str) -> bool:
    """
    Exclusions are checked *after* a rule is violated.
    If this returns True, the error is suppressed.
    """
    v = (value or "").strip()

    # Bullet list of -2 exclusions
    if v == "-2" and field in EXCLUSION_MINUS2_FIELDS:
        return True

    # QualificationNumber may contain `/x`
    if field == "QualificationNumber" and "/x" in v:
        return True

    # MiddleNames allowed to be blank
    if field == "MiddleNames" and v == "":
        return True

    # Sex allowed to be blank
    if field == "Sex" and v == "":
        return True

    return False


def validate_field(field: str, value: str, record_type: str) -> List[Tuple[str, str]]:
    """
    Validate a single field against FIELD_SPECS + date rules.
    Returns list of (error_code, message).
    """
    spec = FIELD_SPECS.get(field)
    errors: List[Tuple[str, str]] = []
    v = (value or "").strip()
    is_blank = v == ""

    conditional_mandatory = {"FirstEntryDate", "AwardDate", "QualificationGrade"}

    if spec:
        # Simple mandatory check (conditional handled at record level)
        if spec.mandatory and field not in conditional_mandatory and is_blank:
            errors.append(("MISSING", "Mandatory field is blank"))
            return errors

        if is_blank:
            return errors

        # -2, if allowed
        if v == "-2" and spec.allow_minus2:
            return errors

        # Date handling
        if spec.type == "date":
            dt, is_sentinel = parse_date(v)
            if not dt and not is_sentinel:
                errors.append(("DATE_FORMAT", f"Invalid date format: {v!r}"))
                return errors

            # Range checks for real dates (not sentinel)
            if dt:
                if dt < date(1900, 1, 1):
                    errors.append(("DATE_RANGE", f"Date {v!r} must be on or after 1900-01-01"))

                if field == "DOB":
                    today = date.today()
                    age = (today - dt).days / 365.25
                    if age < 5 or age > 80:
                        errors.append(
                            ("DATE_RANGE", f"Age from DOB {v!r} must be between 5 and 80 years")
                        )
            return errors

        # Allowed values
        if spec.allowed_values is not None and v not in spec.allowed_values:
            errors.append(
                ("VALUE", f"Value {v!r} not in allowed set: {sorted(spec.allowed_values)}")
            )

        # Pattern
        if spec.pattern and not re.match(spec.pattern, v):
            errors.append(("PATTERN", f"Value {v!r} does not match pattern {spec.pattern!r}"))

        # Length / min / max length
        if spec.length is not None and len(v) != spec.length:
            errors.append(("LENGTH", f"Value {v!r} must be exactly {spec.length} characters"))

        if spec.min_length is not None and len(v) < spec.min_length:
            errors.append(
                ("MIN_LENGTH", f"Value {v!r} must be at least {spec.min_length} characters")
            )

        if spec.max_length is not None and len(v) > spec.max_length:
            errors.append(
                ("MAX_LENGTH", f"Value {v!r} must be at most {spec.max_length} characters")
            )

    return errors


def validate_record_level(row: Dict[str, str], row_number: int) -> List[ValidationError]:
    """
    Record-level rules (cross-field logic).
    """
    errors: List[ValidationError] = []
    record_type = (row.get("RecordType") or "").strip()

    # At least one learner identifier supplied (per spec notes).
    id_fields = [
        "UniqueCandidateIdentifier",
        "UniqueLearnerNumber",
        "UniquePupilNumber",
        "CandidateIdentifierOther",
    ]
    if all((row.get(f, "") or "").strip() == "" for f in id_fields):
        errors.append(
            ValidationError(
                row_number=row_number,
                field="LearnerIdentifierGroup",
                error_code="GROUP_RULE",
                message="At least one learner identifier (UCI/ULN/UPN/Other) must be supplied.",
                value="",
            )
        )

    # Conditional mandatory rules for Result/Amendment
    if record_type in {"Result", "Amendment"}:
        for fname in ("FirstEntryDate", "AwardDate", "QualificationGrade"):
            if not (row.get(fname) or "").strip():
                errors.append(
                    ValidationError(
                        row_number=row_number,
                        field=fname,
                        error_code="MISSING",
                        message=f"{fname} is mandatory for RecordType={record_type}.",
                        value=row.get(fname, ""),
                    )
                )

    # Entry-specific behaviour
    if record_type == "Entry":
        qg = (row.get("QualificationGrade") or "").strip()
        if qg != "-2":
            errors.append(
                ValidationError(
                    row_number=row_number,
                    field="QualificationGrade",
                    error_code="ENTRY_RULE",
                    message="For RecordType=Entry, QualificationGrade must be '-2'.",
                    value=qg,
                )
            )
        for fname in ("FirstEntryDate", "AwardDate"):
            val = (row.get(fname) or "").strip()
            if val not in SENTINEL_DATES:
                errors.append(
                    ValidationError(
                        row_number=row_number,
                        field=fname,
                        error_code="ENTRY_RULE",
                        message=f"For RecordType=Entry, {fname} must be 2999-12-31 or 31/12/2999.",
                        value=val,
                    )
                )

    # Result / Amendment date rules
    if record_type in {"Result", "Amendment"}:
        reg_str = (row.get("RegistrationDate") or "").strip()
        fe_str = (row.get("FirstEntryDate") or "").strip()
        aw_str = (row.get("AwardDate") or "").strip()

        reg_dt, reg_sent = parse_date(reg_str)
        fe_dt, fe_sent = parse_date(fe_str)
        aw_dt, aw_sent = parse_date(aw_str)
        today = date.today()

        # Sentinel not allowed on those dates here
        if fe_sent:
            errors.append(
                ValidationError(
                    row_number=row_number,
                    field="FirstEntryDate",
                    error_code="SENTINEL_NOT_ALLOWED",
                    message="FirstEntryDate sentinel allowed only when RecordType=Entry.",
                    value=fe_str,
                )
            )
        if aw_sent:
            errors.append(
                ValidationError(
                    row_number=row_number,
                    field="AwardDate",
                    error_code="SENTINEL_NOT_ALLOWED",
                    message="AwardDate sentinel allowed only when RecordType=Entry.",
                    value=aw_str,
                )
            )

        if fe_dt and fe_dt > today:
            errors.append(
                ValidationError(
                    row_number=row_number,
                    field="FirstEntryDate",
                    error_code="DATE_FUTURE",
                    message="FirstEntryDate must not be in the future for Result/Amendment.",
                    value=fe_str,
                )
            )
        if aw_dt and aw_dt > today:
            errors.append(
                ValidationError(
                    row_number=row_number,
                    field="AwardDate",
                    error_code="DATE_FUTURE",
                    message="AwardDate must not be in the future for Result/Amendment.",
                    value=aw_str,
                )
            )

        if aw_dt and fe_dt and aw_dt < fe_dt:
            errors.append(
                ValidationError(
                    row_number=row_number,
                    field="AwardDate",
                    error_code="DATE_ORDER",
                    message="AwardDate must be on or after FirstEntryDate.",
                    value=aw_str,
                )
            )

        if aw_dt and reg_dt and aw_dt < reg_dt:
            errors.append(
                ValidationError(
                    row_number=row_number,
                    field="AwardDate",
                    error_code="DATE_ORDER",
                    message="AwardDate must be on or after RegistrationDate.",
                    value=aw_str,
                )
            )

        qg = (row.get("QualificationGrade") or "").strip()
        if qg == "-2":
            errors.append(
                ValidationError(
                    row_number=row_number,
                    field="QualificationGrade",
                    error_code="RESULT_RULE",
                    message="For Result/Amendment, QualificationGrade cannot be '-2'.",
                    value=qg,
                )
            )

    return errors


# ---------------------------------------------------------------------------
# CSV validation entrypoints
# ---------------------------------------------------------------------------

def validate_csv_stream(stream: TextIO) -> List[ValidationError]:
    errors: List[ValidationError] = []

    reader = csv.reader(stream)

    try:
        header = next(reader)
    except StopIteration:
        errors.append(
            ValidationError(
                row_number=1,
                field="*FILE*",
                error_code="EMPTY",
                message="File is empty.",
                value="",
            )
        )
        return errors

    header = [h.strip("\ufeff ").strip() for h in header]

    # Exact header match
    if header != EXPECTED_COLUMNS:
        errors.append(
            ValidationError(
                row_number=1,
                field="*HEADER*",
                error_code="HEADER_MISMATCH",
                message=(
                    "Header row does not match expected VTQ specification. "
                    f"Expected: {EXPECTED_COLUMNS}; Found: {header}"
                ),
                value="",
            )
        )

    seen_rows: Set[Tuple[str, ...]] = set()
    row_idx = 1

    for row in reader:
        row_idx += 1

        if not any(cell.strip() for cell in row):
            continue

        if len(row) != len(EXPECTED_COLUMNS):
            errors.append(
                ValidationError(
                    row_number=row_idx,
                    field="*ROW*",
                    error_code="COLUMN_COUNT",
                    message=f"Row must contain {len(EXPECTED_COLUMNS)} columns, found {len(row)}.",
                    value="",
                )
            )

        if len(row) < len(EXPECTED_COLUMNS):
            row = row + [""] * (len(EXPECTED_COLUMNS) - len(row))
        elif len(row) > len(EXPECTED_COLUMNS):
            row = row[: len(EXPECTED_COLUMNS)]

        row_tuple = tuple(row)
        if row_tuple in seen_rows:
            errors.append(
                ValidationError(
                    row_number=row_idx,
                    field="*ROW*",
                    error_code="DUPLICATE_ROW",
                    message="Duplicate row within submission.",
                    value="",
                )
            )
        else:
            seen_rows.add(row_tuple)

        row_dict = dict(zip(EXPECTED_COLUMNS, row))
        record_type = (row_dict.get("RecordType") or "").strip()

        # Field-level validation
        for field_name in EXPECTED_COLUMNS:
            val = row_dict.get(field_name, "")
            field_errors = validate_field(field_name, val, record_type)
            for code, msg in field_errors:
                if is_permitted_exclusion(field_name, val, code):
                    continue
                errors.append(
                    ValidationError(
                        row_number=row_idx,
                        field=field_name,
                        error_code=code,
                        message=msg,
                        value=val,
                    )
                )

        # Record-level
        rec_errors = validate_record_level(row_dict, row_idx)
        errors.extend(rec_errors)

    return errors


def validate_csv_text(csv_text: str) -> List[ValidationError]:
    from io import StringIO

    stream = StringIO(csv_text)
    return validate_csv_stream(stream)
