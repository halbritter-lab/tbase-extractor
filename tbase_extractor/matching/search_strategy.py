from datetime import date, datetime
from typing import List, Dict, Any, Tuple, Optional
import logging

from ..sql_interface.db_interface import SQLInterface
from ..sql_interface.query_manager import QueryManager
from .fuzzy_matchers import FuzzyMatcher
from .models import MatchCandidate

logger = logging.getLogger(__name__)

DEFAULT_PATIENT_SEARCH_CONFIG = {
    "db_column_map": {
        "first_name": "Vorname",
        "last_name": "Name",
        "dob": "Geburtsdatum"
    },
    "field_weights": {
        "LastName": 0.4,
        "FirstName": 0.3,
        "DOB": 0.3
    },
    "score_mapping": {
        "Exact": 1.0,
        "Fuzzy": "use_similarity",
        "YearMismatch": 0.7,
        "MissingInput": 0.0,  # Should be handled by NotCompared
        "MissingDBValue": -0.1,
        "Mismatch": 0.0,
        "NotCompared": 0.0
    }
}

class PatientSearchStrategy:
    def __init__(
        self,
        sql_interface: SQLInterface,
        query_manager: QueryManager,
        fuzzy_matcher: FuzzyMatcher,
        config: Optional[Dict[str, Any]] = None
    ):
        self.sql_interface = sql_interface
        self.query_manager = query_manager
        self.fuzzy_matcher = fuzzy_matcher
        # Deep merge config if it contains nested dicts like db_column_map
        merged_config = {**DEFAULT_PATIENT_SEARCH_CONFIG}
        if config:
            for key, value in config.items():
                if isinstance(value, dict) and key in merged_config and isinstance(merged_config[key], dict):
                    merged_config[key] = {**merged_config[key], **value}
                else:
                    merged_config[key] = value
        self.config = merged_config

        # Validate essential keys in config
        for map_key in ['first_name', 'last_name', 'dob']:
            if map_key not in self.config['db_column_map']:
                raise ValueError(f"db_column_map in config is missing required key: {map_key}")

    def _fetch_candidates_from_db(self, query: str, params: Tuple[Any, ...]) -> List[Dict[str, Any]]:
        if self.sql_interface.execute_query(query, params):
            results = self.sql_interface.fetch_results()
            return results if results is not None else []
        return []

    def _evaluate_candidate(
        self,
        db_row: Dict[str, Any],
        search_params: Dict[str, Any]  # e.g., {'first_name': 'Jon', 'last_name': 'Doe', 'dob': date_obj}
    ) -> MatchCandidate:
        candidate = MatchCandidate(db_record=db_row)
        
        fn_col = self.config['db_column_map']['first_name']
        ln_col = self.config['db_column_map']['last_name']
        dob_col = self.config['db_column_map']['dob']

        input_fn = search_params.get('first_name')
        db_fn_val = db_row.get(fn_col)
        candidate.match_fields_info.append(
            self.fuzzy_matcher.compare_names("FirstName", input_fn, str(db_fn_val) if db_fn_val is not None else None)
        )

        input_ln = search_params.get('last_name')
        db_ln_val = db_row.get(ln_col)
        candidate.match_fields_info.append(
            self.fuzzy_matcher.compare_names("LastName", input_ln, str(db_ln_val) if db_ln_val is not None else None)
        )

        input_dob = search_params.get('dob')  # This should be a date object
        db_dob_raw = db_row.get(dob_col)
        
        processed_db_dob: Optional[date] = None
        if isinstance(db_dob_raw, date):
            processed_db_dob = db_dob_raw
        elif isinstance(db_dob_raw, datetime):  # Handle if DB returns datetime
            processed_db_dob = db_dob_raw.date()
        # Add parsing for string if your DB returns dates as strings and SQLInterface doesn't handle it
        # For now, assume SQLInterface._clean_field_value or pyodbc handles basic date types.

        candidate.match_fields_info.append(
            self.fuzzy_matcher.compare_dates(input_dob, processed_db_dob)
        )
        
        candidate.calculate_overall_score_and_type(
            field_weights=self.config['field_weights'],
            score_mapping=self.config['score_mapping']        )
        return candidate

    def search(
        self,
        search_params: Dict[str, Any],  # Expects {'first_name': Optional[str], 'last_name': Optional[str], 'dob': Optional[date]}
        min_overall_score: float = 0.0,
        include_diagnoses: bool = False
    ) -> List[MatchCandidate]:
        candidate_sql: Optional[str] = None
        candidate_params: Tuple[Any, ...] = tuple()  # Ensure it's always a tuple
        
        ln_search = search_params.get("last_name")
        dob_search = search_params.get("dob")  # Expected to be a date object        # Determine the SQL query for fetching initial candidates
        if dob_search and isinstance(dob_search, date):
            start_year = dob_search.year - self.fuzzy_matcher.date_year_tolerance
            end_year = dob_search.year + self.fuzzy_matcher.date_year_tolerance
            # Check if query manager supports include_diagnoses parameter
            if hasattr(self.query_manager, 'get_patients_by_dob_year_range_query') and 'include_diagnoses' in self.query_manager.get_patients_by_dob_year_range_query.__code__.co_varnames:
                candidate_sql, candidate_params = self.query_manager.get_patients_by_dob_year_range_query(start_year, end_year, include_diagnoses=include_diagnoses)
            else:
                candidate_sql, candidate_params = self.query_manager.get_patients_by_dob_year_range_query(start_year, end_year)
            logger.info(f"Candidate SQL strategy: DOB year range ({start_year}-{end_year}).")
        elif ln_search and isinstance(ln_search, str):
            # Check if query manager supports include_diagnoses parameter
            if hasattr(self.query_manager, 'get_patients_by_lastname_like_query') and 'include_diagnoses' in self.query_manager.get_patients_by_lastname_like_query.__code__.co_varnames:
                candidate_sql, candidate_params = self.query_manager.get_patients_by_lastname_like_query(ln_search, include_diagnoses=include_diagnoses)
            else:
                candidate_sql, candidate_params = self.query_manager.get_patients_by_lastname_like_query(ln_search)
            logger.info(f"Candidate SQL strategy: LastName LIKE '{ln_search}%'.")
        else:
            logger.warning("Neither DOB nor LastName provided for initial SQL filtering. "
                       "Falling back to fetching ALL patients. This can be very slow on large databases.")
            # Check if query manager supports include_diagnoses parameter
            if hasattr(self.query_manager, 'get_all_patients_query') and 'include_diagnoses' in self.query_manager.get_all_patients_query.__code__.co_varnames:
                candidate_sql, candidate_params = self.query_manager.get_all_patients_query(include_diagnoses=include_diagnoses)
            else:
                candidate_sql, candidate_params = self.query_manager.get_all_patients_query()

        if not candidate_sql:  # Should only happen if get_all_patients_query also failed to load
            logger.error("Failed to build candidate SQL query (template not found or other QM error).")
            return []

        logger.debug(f"Fetching candidates with SQL: {candidate_sql} PARAMS: {candidate_params}")
        db_candidates_raw = self._fetch_candidates_from_db(candidate_sql, candidate_params)
        logger.info(f"Fetched {len(db_candidates_raw)} raw candidates from DB.")

        evaluated_candidates: List[MatchCandidate] = []
        if db_candidates_raw:  # Proceed only if candidates were fetched
            for db_row in db_candidates_raw:
                candidate = self._evaluate_candidate(db_row, search_params)
                if candidate.overall_score >= min_overall_score:
                    evaluated_candidates.append(candidate)
        
        logger.info(f"Evaluated to {len(evaluated_candidates)} candidates after scoring (min_score: {min_overall_score}).")

        evaluated_candidates.sort(key=lambda c: c.overall_score, reverse=True)
        
        return evaluated_candidates
