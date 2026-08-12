"""
VortexAI - Test Suite
Tests the two most failure-prone areas: Pydantic validation rules (consumer)
and text-cleaning logic (Silver transform). Run with: pytest tests/ -v
"""

import os
import sys

import pytest
from pydantic import ValidationError

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "storage"))

from schemas import HNStoryEvent  # noqa: E402
from silver_transform import (  # noqa: E402
    build_snippet,
    clean_html_text,
    domain_of,
)


# ---------- HNStoryEvent validation tests ----------

class TestHNStoryEventValidation:

    def test_valid_story_with_url_passes(self):
        event = HNStoryEvent(
            id=1, title="Show HN: My tool", by="alice", time=1234567890,
            type="story", url="https://example.com",
        )
        assert event.title == "Show HN: My tool"

    def test_valid_story_with_text_passes(self):
        event = HNStoryEvent(
            id=2, title="Ask HN: Advice?", by="bob", time=1234567890,
            type="story", text="<p>What do you think?</p>",
        )
        assert event.text is not None

    def test_missing_title_raises_error(self):
        with pytest.raises(ValidationError):
            HNStoryEvent(id=3, by="carol", time=1234567890, type="story", url="https://x.com")

    def test_empty_title_raises_error(self):
        with pytest.raises(ValidationError):
            HNStoryEvent(id=4, title="", by="dave", time=1234567890, type="story", url="https://x.com")

    def test_missing_author_raises_error(self):
        with pytest.raises(ValidationError):
            HNStoryEvent(id=5, title="Some title", time=1234567890, type="story", url="https://x.com")

    def test_no_text_and_no_url_raises_error(self):
        """A story with neither a self-text body nor an external link carries no content."""
        with pytest.raises(ValidationError):
            HNStoryEvent(id=6, title="Empty story", by="eve", time=1234567890, type="story")

    def test_non_integer_time_raises_error(self):
        with pytest.raises(ValidationError):
            HNStoryEvent(
                id=7, title="Bad timestamp", by="frank", time="not-a-number",
                type="story", url="https://x.com",
            )


# ---------- Silver text-cleaning tests ----------

class TestCleanHtmlText:

    def test_strips_html_tags(self):
        assert clean_html_text("<p>Hello <b>world</b></p>") == "Hello world"

    def test_unescapes_html_entities(self):
        assert clean_html_text("Tom &amp; Jerry &quot;quoted&quot;") == 'Tom & Jerry "quoted"'

    def test_collapses_whitespace(self):
        assert clean_html_text("Too    many\n\nspaces") == "Too many spaces"

    def test_none_input_returns_empty_string(self):
        assert clean_html_text(None) == ""

    def test_nan_float_input_returns_empty_string(self):
        # Reproduces the real bug hit when reading missing values back from Parquet
        assert clean_html_text(float("nan")) == ""

    def test_empty_string_returns_empty_string(self):
        assert clean_html_text("") == ""


class TestDomainOf:

    def test_extracts_domain_from_url(self):
        assert domain_of("https://www.example.com/path?x=1") == "example.com"

    def test_none_url_returns_empty_string(self):
        assert domain_of(None) == ""

    def test_nan_url_returns_empty_string(self):
        assert domain_of(float("nan")) == ""


class TestBuildSnippet:

    def test_title_with_text_combines_both(self):
        result = build_snippet("My Post", "<p>Some content</p>", "https://x.com")
        assert result == "My Post: Some content"

    def test_title_with_url_only_shows_domain(self):
        result = build_snippet("Link Post", float("nan"), "https://github.com/foo/bar")
        assert result == "Link Post (link: github.com)"

    def test_title_only_no_text_no_url(self):
        result = build_snippet("Bare Title", None, None)
        assert result == "Bare Title"

    def test_nan_title_does_not_crash(self):
        # Should not raise, even with maximally missing data
        result = build_snippet(float("nan"), float("nan"), float("nan"))
        assert result == ""