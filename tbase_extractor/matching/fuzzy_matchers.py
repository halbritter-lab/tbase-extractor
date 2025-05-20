from datetime import date
from typing import Optional
from rapidfuzz import fuzz  # Or your chosen fuzzy matching library
from .models import MatchInfo

class FuzzyMatcher:
    def __init__(
        self,
        string_similarity_threshold: float = 0.85,
        date_year_tolerance: int = 1,
    ):
        if not (0.0 <= string_similarity_threshold <= 1.0):
            raise ValueError("string_similarity_threshold must be between 0.0 and 1.0")
        self.string_similarity_threshold = string_similarity_threshold
        self.date_year_tolerance = date_year_tolerance

    def calculate_string_similarity(self, str1: str, str2: str) -> float:
        # rapidfuzz.fuzz.WRatio handles empty strings gracefully, returning 0.0
        return fuzz.WRatio(str1, str2) / 100.0

    def compare_names(self, field_name: str, input_name: Optional[str], db_name: Optional[str]) -> MatchInfo:
        input_name_clean = (input_name or "").strip().lower()
        db_name_clean = (db_name or "").strip().lower()

        if not input_name_clean:  # Input name not provided or empty
            return MatchInfo(field_name, input_name, db_name, "NotCompared", details="Input name not provided")
        if not db_name_clean:  # DB name not provided or empty, but input was
            return MatchInfo(field_name, input_name, db_name, "MissingDBValue", details="DB name not provided")

        if input_name_clean == db_name_clean:
            return MatchInfo(field_name, input_name, db_name, "Exact", 1.0)

        similarity = self.calculate_string_similarity(input_name_clean, db_name_clean)
        if similarity >= self.string_similarity_threshold:
            return MatchInfo(field_name, input_name, db_name, "Fuzzy", similarity)
        else:
            return MatchInfo(field_name, input_name, db_name, "Mismatch", similarity)

    def compare_dates(self, input_dob: Optional[date], db_dob: Optional[date]) -> MatchInfo:
        # Using a fixed field_name for DOB comparisons for consistency in MatchInfo
        _FIELD_NAME_DOB = "DOB"

        if input_dob is None:
            return MatchInfo(_FIELD_NAME_DOB, input_dob, db_dob, "NotCompared", details="Input DOB not provided")
        if db_dob is None:
            return MatchInfo(_FIELD_NAME_DOB, input_dob, db_dob, "MissingDBValue", details="DB DOB not provided")

        if input_dob == db_dob:
            return MatchInfo(_FIELD_NAME_DOB, input_dob, db_dob, "Exact", 1.0)

        # Year Mismatch (Day and Month match, year difference within tolerance)
        if input_dob.month == db_dob.month and input_dob.day == db_dob.day:
            year_difference = abs(input_dob.year - db_dob.year)
            if 0 < year_difference <= self.date_year_tolerance:  # 0 < to ensure it's not an exact match
                # Score for year mismatch can be made more granular if needed
                return MatchInfo(_FIELD_NAME_DOB, input_dob, db_dob, "YearMismatch", 0.7, f"Year diff: {year_difference}")

        return MatchInfo(_FIELD_NAME_DOB, input_dob, db_dob, "Mismatch", 0.0)
