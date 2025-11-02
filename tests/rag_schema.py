import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Literal, cast

from pydantic import BaseModel, Field

from lythonic import utc_now
from lythonic.state import DbModel, Schema, execute_sql, from_multi_model_row

logger = logging.getLogger(__name__)

ActionType = Literal["new", "update", "delete"]


class RagSource(DbModel["RagSource"]):
    source_id: int = Field(default=-1, description="(PK) Unique identifier for the source")
    absolute_path: Path = Field(description="Absolute path to the source file")


class RagAction(DbModel["RagAction"]):
    action_id: int = Field(default=-1, description="(PK) Unique identifier for the attempt")
    source_id: int = Field(
        description="(FK:RagSource.source_id) Reference to the source being vectorized"
    )
    timestamp: datetime = Field(description="When the attempt was made", default_factory=utc_now)
    n_chunks: int = Field(description="Number of chunks created", ge=0)
    error: str | None = Field(default=None, description="Error message if vectorization failed")
    sha256: str = Field(description="SHA256 hash of the original file")


class RagActionCollection(DbModel["RagActionCollection"]):
    action_id: int = Field(
        description="(FK:RagAction.action_id) Reference to the action being vectorized"
    )
    action: ActionType = Field(description="Action taken on the source")
    collection: str = Field(description="Name of the collection the action was applied to")
    timestamp: datetime = Field(description="When the attempt was made", default_factory=utc_now)


class ChatMessage(BaseModel):
    role: Literal["system", "user", "assistant", "tool"] = Field(
        description="The role in the conversation"
    )
    tool_call_id: str | None = Field(
        default=None, description="The tool call ID if the role is 'tool'"
    )
    content: str = Field(description="The content of the message")
    finish_reason: Literal["stop", "length", "content_filter", "null"] = Field(
        default="null", description="The reason the conversation ended"
    )


class ConvoMessage(DbModel["ConvoMessage"], ChatMessage):
    message_id: int = Field(default=-1, description="(PK) The message ID")
    session_id: int = Field(default=-1, description="(FK:ConvoSession.session_id) The session ID")
    captured: datetime = Field(default_factory=utc_now)


class ConvoSession(DbModel["ConvoSession"]):
    session_id: int = Field(default=-1, description="(PK) The session ID")
    created: datetime = Field(default_factory=utc_now)
    updated: datetime = Field(default_factory=utc_now)
    model: str = Field(description="The model id used for the chat session to answer questions")
    user_id: str | None = Field(
        default=None, description="The user ID of the user who created the session"
    )
    session_type: Literal["active", "completed", "failed", "archived"] = "active"


SCHEMA = Schema([RagSource, RagAction, RagActionCollection, ConvoMessage, ConvoSession])


def select_all_active_sources(
    conn: sqlite3.Connection,
) -> list[tuple[RagSource, RagAction, list[RagActionCollection]]]:
    cursor = conn.cursor()
    execute_sql(
        cursor,
        f"WITH last_actions as (SELECT a.source_id, max(a.action_id) as action_id FROM {RagAction.alias('a')} GROUP BY a.source_id) "
        + f" SELECT {RagSource.columns('s')}, {RagAction.columns('a')}, {RagActionCollection.columns('c')} "
        + f"   FROM {RagSource.alias('s')}, {RagAction.alias('a')}, {RagActionCollection.alias('c')}, last_actions l "
        + "   WHERE s.source_id = a.source_id AND l.action_id = a.action_id AND c.action_id = a.action_id ORDER BY a.source_id",
    )
    sources = [
        cast(
            tuple[RagSource, RagAction, RagActionCollection],
            tuple(from_multi_model_row(row, [RagSource, RagAction, RagActionCollection])),
        )
        for row in cursor.fetchall()
    ]
    agg: list[tuple[RagSource, RagAction, list[RagActionCollection]]] = []
    if len(sources) > 0:
        start = 0
        i = 0
        while True:
            prev_a = sources[i][1]
            while True:
                i += 1
                if i >= len(sources) or sources[i][1].action_id != prev_a.action_id:
                    break
            collections = [c for _, _, c in sources[start:i]]
            agg.append((sources[start][0], prev_a, collections))
            if i >= len(sources):
                break
            start = i
    logger.info(f"Selected {len(agg)} sources")
    return agg
