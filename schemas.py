"""
VortexAI - Data Schemas
Pydantic models describing valid event shapes. Deliberately has zero
infrastructure dependencies (no Kafka, no Qdrant) so it can be imported
and unit-tested in isolation, and reused by any consumer/producer.
"""

from typing import Optional

from pydantic import BaseModel, Field, model_validator


class HNStoryEvent(BaseModel):
    id: int
    title: str = Field(..., min_length=1)
    by: str = Field(..., min_length=1)
    time: int
    type: str
    text: Optional[str] = None
    url: Optional[str] = None
    score: Optional[int] = None
    descendants: Optional[int] = None

    @model_validator(mode="after")
    def must_have_text_or_url(self):
        # A story needs at least one of: self-post body, or an external link
        if not self.text and not self.url:
            raise ValueError("story has neither text body nor url")
        return self