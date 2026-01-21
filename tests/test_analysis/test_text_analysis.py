"""Tests for text analysis module."""

import pytest


class TestTextAnalysis:
    """Tests for text analysis functions."""

    def test_analyze_text_basic(self):
        """Test basic text analysis."""
        from src.analysis.text_analysis import analyze_text

        text = "The patient experienced pain and numbness after device migration."
        result = analyze_text(text)

        assert result.word_count > 0
        assert "pain" in result.term_matches
        assert "numbness" in result.term_matches
        assert "migration" in result.term_matches

    def test_analyze_text_death_terms(self):
        """Test detection of death-related terms."""
        from src.analysis.text_analysis import analyze_text

        text = "The patient died following the procedure. Death was attributed to device failure."
        result = analyze_text(text)

        assert "death" in result.term_matches
        assert "died" in result.term_matches["death"] or "death" in result.term_matches["death"]

    def test_analyze_text_device_issues(self):
        """Test detection of device issue terms."""
        from src.analysis.text_analysis import analyze_text

        text = "The lead fractured and the battery was depleted. Device malfunction reported."
        result = analyze_text(text)

        assert "fracture" in result.term_matches
        assert "battery" in result.term_matches
        assert "malfunction" in result.term_matches

    def test_analyze_text_empty(self):
        """Test analysis of empty text."""
        from src.analysis.text_analysis import analyze_text

        result = analyze_text("")

        assert result.word_count == 0
        assert len(result.term_matches) == 0

    def test_analyze_text_keywords(self):
        """Test keyword extraction."""
        from src.analysis.text_analysis import analyze_text

        text = "The spinal cord stimulator experienced multiple failures. The stimulator was replaced."
        result = analyze_text(text)

        # Should extract common keywords
        assert len(result.keywords) > 0
        keyword_words = [k[0] for k in result.keywords]
        assert "stimulator" in keyword_words

    def test_analyze_text_case_insensitive(self):
        """Test case insensitivity."""
        from src.analysis.text_analysis import analyze_text

        text = "PAIN reported. Patient experienced DEATH."
        result = analyze_text(text)

        assert "pain" in result.term_matches
        assert "death" in result.term_matches


class TestAdverseEventTerms:
    """Tests for adverse event term dictionary."""

    def test_adverse_event_terms_structure(self):
        """Test that ADVERSE_EVENT_TERMS has expected structure."""
        from src.analysis.text_analysis import ADVERSE_EVENT_TERMS

        # Check required categories exist
        assert "death" in ADVERSE_EVENT_TERMS
        assert "injury" in ADVERSE_EVENT_TERMS
        assert "pain" in ADVERSE_EVENT_TERMS
        assert "migration" in ADVERSE_EVENT_TERMS
        assert "malfunction" in ADVERSE_EVENT_TERMS

        # Check each category has terms
        for category, terms in ADVERSE_EVENT_TERMS.items():
            assert isinstance(terms, list)
            assert len(terms) > 0

    def test_stop_words(self):
        """Test that stop words are defined."""
        from src.analysis.text_analysis import STOP_WORDS

        assert "the" in STOP_WORDS
        assert "and" in STOP_WORDS
        assert "patient" in STOP_WORDS  # Domain-specific stop word
