"""Data models for patient matching functionality."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Union


@dataclass
class MatchInfo:
    """Information about a field match comparison."""

    field_name: str
    input_value: Any
    db_value: Any
    match_type: str
    similarity_score: Optional[float] = None  # Standardize to 0.0-1.0
    details: Optional[str] = None


@dataclass
class MatchCandidate:
    """A candidate match for patient record with scoring information."""

    db_record: Dict[str, Any]
    match_fields_info: List[MatchInfo] = field(default_factory=list)
    overall_score: float = 0.0
    primary_match_type: str = "NoMatch"
    csv_input_row_number: Optional[int] = None
    csv_input_data: Optional[Dict[str, Any]] = None

    def calculate_overall_score_and_type(
        self,
        field_weights: Mapping[str, float],
        score_mapping: Mapping[str, Union[float, str]],  # Allow both float and string values
    ) -> None:
        """Calculate overall match score and determine primary match type."""
        calculated_score = 0.0
        field_match_summaries = []
        num_exact_matches_for_weighted_fields = 0
        num_weighted_fields_considered = 0

        for info in self.match_fields_info:
            weight = field_weights.get(info.field_name, 0.0)
            base_score = 0.0

            if info.match_type in score_mapping:
                score_source = score_mapping[info.match_type]
                if isinstance(score_source, str) and score_source == "use_similarity":
                    base_score = info.similarity_score if info.similarity_score is not None else 0.0
                elif isinstance(score_source, (int, float)):
                    base_score = float(score_source)

            calculated_score += base_score * weight

            summary_entry = f"{info.field_name}:{info.match_type}"
            if info.match_type == "Fuzzy" and info.similarity_score is not None:
                summary_entry += f"({info.similarity_score:.2f})"
            field_match_summaries.append(summary_entry)

            if weight > 0:  # Only consider for primary_match_type determination if field has weight
                num_weighted_fields_considered += 1
                if info.match_type == "Exact":
                    num_exact_matches_for_weighted_fields += 1

        self.overall_score = calculated_score

        # Determine primary_match_type
        if (
            num_weighted_fields_considered > 0
            and num_exact_matches_for_weighted_fields == num_weighted_fields_considered
        ):
            self.primary_match_type = "Exact Match"
        elif self.overall_score > 0.01:  # Use a small threshold to avoid "Partial Match" for near-zero scores
            self.primary_match_type = "Partial Match: " + "|".join(sorted(field_match_summaries))
        else:
            self.primary_match_type = "No Significant Match"
