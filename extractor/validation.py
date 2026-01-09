from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

class GitHubActor(BaseModel):
    id: int
    login: str
    display_login: Optional[str]
    url: str

class GitHubRepo(BaseModel):
    id: int
    name: str
    url: str

class GitHubEvent(BaseModel):
    id: str
    type: str
    actor: GitHubActor
    repo: GitHubRepo
    created_at: datetime

    class Config:
        # payload differs by event type — allow it
        extra = "allow"


def validate_events(raw_events: List[dict]) -> List[dict]:

    valid_events = []
    invalid_count = 0

    for event in raw_events:
        try:
            GitHubEvent(**event)  # schema enforcement
            valid_events.append(event)
        except Exception:
            invalid_count += 1

    if invalid_count > 0:
        print(f"⚠️  Dropped {invalid_count} invalid events")

    return valid_events
