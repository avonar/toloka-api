from dataclasses import dataclass


@dataclass
class TolokaTaskDateTag:
    CREATED: str = 'created'
    ACCEPTED: str = 'accepted'
    EXPIRED: str = 'expired'
    REJECTED: str = 'rejected'
    SKIPPED: str = 'skipped'
    SUBMITTED: str = 'submitted'


@dataclass
class TolokaTaskStatus:
    IN_PROGRESS: str = 'IN_PROGRESS'
    ACCEPTED: str = 'ACCEPTED'
    ACTIVE: str = 'ACTIVE'
    EXPIRED: str = 'EXPIRED'
    REJECTED: str = 'REJECTED'
    SKIPPED: str = 'SKIPPED'
    SUBMITTED: str = 'SUBMITTED'


@dataclass
class TaskAssigmentResult:
    FOUND: str = 'FOUND'
    NOT_FOUND: str = 'NOT_FOUND'


@dataclass
class PoolStatus:
    OPEN: str = 'OPEN'
    CLOSED: str = 'CLOSED'
    ARCHIVED: str = 'ARCHIVED'


@dataclass
class AssigmentStatus:
    ACTIVE: str = 'ACTIVE'
    SUBMITTED: str = 'SUBMITTED'
    ACCEPTED: str = 'ACCEPTED'
    REJECTED: str = 'REJECTED'
    SKIPPED: str = 'SKIPPED'
    EXPIRED: str = 'EXPIRED'


@dataclass
class PoolType:
    REGULAR: str = 'REGULAR'
    TRAINING: str = 'TRAINING'


@dataclass(frozen=True)
class ApiV1:
    ASSIGMENTS: str = '/api/v1/assignments'
    TASK_SUITES: str = '/api/v1/task-suites'
    POOLS: str = '/api/v1/pools'
    TASKS: str = '/api/v1/tasks'
    MESSAGES: str = '/api/v1/message-threads/compose'
    BONUS: str = '/api/v1/user-bonuses'
    PROJECT: str = '/api/v1/projects'
    WORKERS: str = '/api/new/requester/workers/grid'
    OPERATIONS: str = '/api/v1/operations'
    ANALYTICS: str = '/api/staging/analytics-2'
    AGGREGATE: str = '/api/v1/aggregated-solutions/aggregate-by-pool'
    AGGREGATED_SOLUTIONS: str = '/api/v1/aggregated-solutions/'
    THREADS: str = '/api/v1/message-threads'
    BALANCE: str = '/api/user/requester/balance'


API_V1 = ApiV1()

NOT_BANNED = 'NOT_BANNED'
PROJECT_BAN = 'PROJECT_BAN'
SYSTEM_BAN = 'SYSTEM_BAN'
REQUESTER_BAN = 'REQUESTER_BAN'
TOO_MANY_REQUESTS = 'TOO_MANY_REQUESTS'
TIMEOUT = 60

UNIQUE_SUBMITTERS_COUNT = 'unique_submitters_count'
AVG_SUBMIT_ASSIGNMENT_MILLIS = 'avg_submit_assignment_millis'
REAL_TASKS_COUNT = 'real_tasks_count'
