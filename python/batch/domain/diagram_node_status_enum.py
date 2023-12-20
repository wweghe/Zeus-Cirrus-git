from enum import Enum


class DiagramNodeStatusEnum(str, Enum):
    RUNNING = 'running'
    IN_PROGRESS = 'in_progress'
    COMPLETED = 'completed'
    FAILED = 'failed'
    SKIPPED = 'skipped'
    NOT_STARTED = 'not_started'