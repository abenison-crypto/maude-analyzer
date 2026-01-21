"""Text analysis module for MDR narratives."""

import re
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
import pandas as pd
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import get_logger
from src.database import get_connection

logger = get_logger("text_analysis")


# Common medical device adverse event terms
ADVERSE_EVENT_TERMS = {
    # Outcomes
    "death": ["death", "died", "deceased", "fatal", "fatality", "mortality"],
    "injury": ["injury", "injured", "harm", "damage", "trauma"],
    "infection": ["infection", "infected", "sepsis", "abscess", "bacterial"],
    "pain": ["pain", "painful", "discomfort", "ache", "soreness", "burning"],
    "bleeding": ["bleeding", "hemorrhage", "blood loss", "hematoma"],
    "revision": ["revision", "explant", "removal", "replacement", "reoperation"],

    # Device issues
    "migration": ["migration", "migrated", "moved", "displaced", "dislodged"],
    "fracture": ["fracture", "fractured", "broken", "break", "cracked"],
    "malfunction": ["malfunction", "failure", "failed", "defect", "defective"],
    "shock": ["shock", "shocked", "jolted", "stimulation"],
    "battery": ["battery", "power", "depleted", "drained"],
    "lead": ["lead", "electrode", "wire", "conductor"],
    "programming": ["programming", "programmed", "settings", "parameters"],
    "mri": ["mri", "magnetic resonance", "imaging"],

    # Symptoms
    "numbness": ["numbness", "numb", "tingling", "paresthesia"],
    "weakness": ["weakness", "weak", "paralysis", "paresis"],
    "headache": ["headache", "head pain", "cephalgia"],
    "nausea": ["nausea", "vomiting", "emesis"],
}

# Stop words for keyword extraction
STOP_WORDS = {
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "as", "is", "was", "are", "were", "been",
    "be", "have", "has", "had", "do", "does", "did", "will", "would",
    "could", "should", "may", "might", "must", "shall", "can", "need",
    "dare", "ought", "used", "this", "that", "these", "those", "i", "you",
    "he", "she", "it", "we", "they", "what", "which", "who", "whom",
    "patient", "device", "report", "reported", "per", "also", "not", "no",
}


@dataclass
class TextAnalysisResult:
    """Result of text analysis."""

    mdr_report_key: str
    term_matches: Dict[str, List[str]] = field(default_factory=dict)
    keywords: List[Tuple[str, int]] = field(default_factory=list)
    word_count: int = 0
    sentence_count: int = 0


@dataclass
class TermFrequency:
    """Term frequency data."""

    term_category: str
    term: str
    count: int
    percentage: float


def analyze_text(
    text: str,
    terms_dict: Optional[Dict[str, List[str]]] = None,
) -> TextAnalysisResult:
    """
    Analyze a single MDR text narrative.

    Args:
        text: The narrative text to analyze.
        terms_dict: Dictionary of term categories to search for.

    Returns:
        TextAnalysisResult with analysis findings.
    """
    if not text:
        return TextAnalysisResult(mdr_report_key="", word_count=0)

    terms_dict = terms_dict or ADVERSE_EVENT_TERMS
    text_lower = text.lower()

    result = TextAnalysisResult(
        mdr_report_key="",
        word_count=len(text.split()),
        sentence_count=len(re.findall(r'[.!?]+', text)),
    )

    # Find term matches
    for category, terms in terms_dict.items():
        matches = []
        for term in terms:
            if term.lower() in text_lower:
                matches.append(term)
        if matches:
            result.term_matches[category] = matches

    # Extract keywords (simple frequency-based)
    words = re.findall(r'\b[a-zA-Z]{3,}\b', text_lower)
    word_counts = Counter(w for w in words if w not in STOP_WORDS)
    result.keywords = word_counts.most_common(20)

    return result


def get_narrative_text(
    mdr_report_key: str,
    conn=None,
) -> Optional[str]:
    """
    Get concatenated narrative text for an MDR.

    Args:
        mdr_report_key: The MDR report key.
        conn: Database connection.

    Returns:
        Combined narrative text or None.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    try:
        result = conn.execute("""
            SELECT text_content
            FROM mdr_text
            WHERE mdr_report_key = ?
            ORDER BY text_type_code
        """, [mdr_report_key]).fetchall()

        if result:
            return " ".join(row[0] for row in result if row[0])
        return None

    finally:
        if own_conn:
            conn.close()


def search_narratives(
    search_terms: List[str],
    manufacturers: Optional[List[str]] = None,
    product_codes: Optional[List[str]] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: int = 100,
    conn=None,
) -> pd.DataFrame:
    """
    Search MDR narratives for specific terms.

    Args:
        search_terms: List of terms to search for.
        manufacturers: Filter by manufacturers.
        product_codes: Filter by product codes.
        start_date: Start date filter.
        end_date: End date filter.
        limit: Maximum results.
        conn: Database connection.

    Returns:
        DataFrame with matching MDRs.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    try:
        params = []
        where_clauses = []

        # Build search condition
        search_conditions = []
        for term in search_terms:
            search_conditions.append("t.text_content ILIKE ?")
            params.append(f"%{term}%")

        if search_conditions:
            where_clauses.append(f"({' OR '.join(search_conditions)})")

        if manufacturers:
            placeholders = ", ".join(["?" for _ in manufacturers])
            where_clauses.append(f"m.manufacturer_clean IN ({placeholders})")
            params.extend(manufacturers)

        if product_codes:
            placeholders = ", ".join(["?" for _ in product_codes])
            where_clauses.append(f"m.product_code IN ({placeholders})")
            params.extend(product_codes)

        if start_date:
            where_clauses.append("m.date_received >= ?")
            params.append(start_date)

        if end_date:
            where_clauses.append("m.date_received <= ?")
            params.append(end_date)

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        sql = f"""
            SELECT DISTINCT
                m.mdr_report_key,
                m.date_received,
                m.manufacturer_clean,
                m.product_code,
                m.event_type,
                t.text_content
            FROM master_events m
            JOIN mdr_text t ON m.mdr_report_key = t.mdr_report_key
            {where_sql}
            ORDER BY m.date_received DESC
            LIMIT {limit}
        """

        return conn.execute(sql, params).fetchdf()

    finally:
        if own_conn:
            conn.close()


def get_term_frequency(
    term_category: Optional[str] = None,
    manufacturers: Optional[List[str]] = None,
    product_codes: Optional[List[str]] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    conn=None,
) -> pd.DataFrame:
    """
    Get frequency of adverse event terms in narratives.

    Args:
        term_category: Specific category to analyze (or all if None).
        manufacturers: Filter by manufacturers.
        product_codes: Filter by product codes.
        start_date: Start date filter.
        end_date: End date filter.
        conn: Database connection.

    Returns:
        DataFrame with term frequencies.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    try:
        # Get total count for percentage calculation
        params = []
        where_clauses = []

        if manufacturers:
            placeholders = ", ".join(["?" for _ in manufacturers])
            where_clauses.append(f"m.manufacturer_clean IN ({placeholders})")
            params.extend(manufacturers)

        if product_codes:
            placeholders = ", ".join(["?" for _ in product_codes])
            where_clauses.append(f"m.product_code IN ({placeholders})")
            params.extend(product_codes)

        if start_date:
            where_clauses.append("m.date_received >= ?")
            params.append(start_date)

        if end_date:
            where_clauses.append("m.date_received <= ?")
            params.append(end_date)

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        # Get total MDRs with text
        total_query = f"""
            SELECT COUNT(DISTINCT m.mdr_report_key)
            FROM master_events m
            JOIN mdr_text t ON m.mdr_report_key = t.mdr_report_key
            {where_sql}
        """
        total_count = conn.execute(total_query, params).fetchone()[0] or 1

        # Count each term
        results = []
        terms_to_check = ADVERSE_EVENT_TERMS
        if term_category:
            terms_to_check = {term_category: ADVERSE_EVENT_TERMS.get(term_category, [])}

        for category, terms in terms_to_check.items():
            for term in terms:
                term_params = params.copy()
                term_where = where_sql

                # Add term search
                if term_where:
                    term_where += " AND t.text_content ILIKE ?"
                else:
                    term_where = "WHERE t.text_content ILIKE ?"
                term_params.append(f"%{term}%")

                count_query = f"""
                    SELECT COUNT(DISTINCT m.mdr_report_key)
                    FROM master_events m
                    JOIN mdr_text t ON m.mdr_report_key = t.mdr_report_key
                    {term_where}
                """

                count = conn.execute(count_query, term_params).fetchone()[0]

                if count > 0:
                    results.append({
                        "category": category,
                        "term": term,
                        "count": count,
                        "percentage": round(count / total_count * 100, 2),
                    })

        df = pd.DataFrame(results)
        if not df.empty:
            df = df.sort_values(["category", "count"], ascending=[True, False])

        return df

    finally:
        if own_conn:
            conn.close()


def get_keyword_trends(
    n_keywords: int = 20,
    manufacturers: Optional[List[str]] = None,
    product_codes: Optional[List[str]] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    conn=None,
) -> pd.DataFrame:
    """
    Extract and count top keywords from narratives.

    Args:
        n_keywords: Number of top keywords to return.
        manufacturers: Filter by manufacturers.
        product_codes: Filter by product codes.
        start_date: Start date filter.
        end_date: End date filter.
        conn: Database connection.

    Returns:
        DataFrame with keyword frequencies.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    try:
        params = []
        where_clauses = []

        if manufacturers:
            placeholders = ", ".join(["?" for _ in manufacturers])
            where_clauses.append(f"m.manufacturer_clean IN ({placeholders})")
            params.extend(manufacturers)

        if product_codes:
            placeholders = ", ".join(["?" for _ in product_codes])
            where_clauses.append(f"m.product_code IN ({placeholders})")
            params.extend(product_codes)

        if start_date:
            where_clauses.append("m.date_received >= ?")
            params.append(start_date)

        if end_date:
            where_clauses.append("m.date_received <= ?")
            params.append(end_date)

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        # Get all text content
        sql = f"""
            SELECT t.text_content
            FROM master_events m
            JOIN mdr_text t ON m.mdr_report_key = t.mdr_report_key
            {where_sql}
        """

        result = conn.execute(sql, params).fetchall()

        # Extract and count keywords
        all_words = []
        for (text,) in result:
            if text:
                words = re.findall(r'\b[a-zA-Z]{4,}\b', text.lower())
                all_words.extend(w for w in words if w not in STOP_WORDS)

        word_counts = Counter(all_words)
        top_keywords = word_counts.most_common(n_keywords)

        return pd.DataFrame(top_keywords, columns=["keyword", "count"])

    finally:
        if own_conn:
            conn.close()


def compare_term_frequency_by_manufacturer(
    term_category: str,
    manufacturers: List[str],
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    conn=None,
) -> pd.DataFrame:
    """
    Compare term frequencies across manufacturers.

    Args:
        term_category: Category of terms to analyze.
        manufacturers: List of manufacturers to compare.
        start_date: Start date filter.
        end_date: End date filter.
        conn: Database connection.

    Returns:
        DataFrame with manufacturer comparison.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    try:
        results = []

        for manufacturer in manufacturers:
            mfr_df = get_term_frequency(
                term_category=term_category,
                manufacturers=[manufacturer],
                start_date=start_date,
                end_date=end_date,
                conn=conn,
            )

            for _, row in mfr_df.iterrows():
                results.append({
                    "manufacturer": manufacturer,
                    "term": row["term"],
                    "count": row["count"],
                    "percentage": row["percentage"],
                })

        return pd.DataFrame(results)

    finally:
        if own_conn:
            conn.close()
