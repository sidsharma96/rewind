CREATE TABLE projects (
    id bytea PRIMARY KEY CHECK (octet_length(id) BETWEEN 1 AND 256),
    payload_retention_us bigint NOT NULL DEFAULT 604800000000
        CHECK (payload_retention_us BETWEEN 1 AND 9223372036854775),
    marker_window_us bigint NOT NULL DEFAULT 86400000000
        CHECK (marker_window_us BETWEEN 1 AND 9223372036854775)
);
CREATE TABLE runs (
    project bytea NOT NULL REFERENCES projects(id),
    run_id bytea NOT NULL CHECK (octet_length(run_id) BETWEEN 1 AND 256),
    phase text NOT NULL CHECK (phase IN ('running','completed','closed','revoked')),
    completed_at timestamptz,
    until_at timestamptz,
    PRIMARY KEY (project, run_id),
    CHECK (
        (phase = 'running' AND completed_at IS NULL AND until_at IS NULL)
        OR (phase IN ('completed','closed') AND completed_at IS NOT NULL AND until_at IS NOT NULL
            AND isfinite(completed_at) AND isfinite(until_at) AND until_at > completed_at)
        OR (phase = 'revoked' AND (
            (completed_at IS NULL AND until_at IS NULL)
            OR (completed_at IS NOT NULL AND until_at IS NOT NULL
                AND isfinite(completed_at) AND isfinite(until_at) AND until_at > completed_at)))
    )
);
CREATE TABLE creator_grants (
    project bytea NOT NULL,
    trace_id bytea NOT NULL CHECK (octet_length(trace_id) BETWEEN 1 AND 256),
    span_id bytea NOT NULL CHECK (octet_length(span_id) BETWEEN 1 AND 256),
    run_id bytea NOT NULL,
    instance_id bytea NOT NULL CHECK (octet_length(instance_id) BETWEEN 1 AND 256),
    principal_id bytea NOT NULL CHECK (octet_length(principal_id) BETWEEN 1 AND 256),
    PRIMARY KEY (project, trace_id, span_id, run_id, instance_id, principal_id),
    FOREIGN KEY (project, run_id) REFERENCES runs(project, run_id)
);
CREATE TABLE bindings (
    project bytea NOT NULL,
    trace_id bytea NOT NULL CHECK (octet_length(trace_id) BETWEEN 1 AND 256),
    span_id bytea NOT NULL CHECK (octet_length(span_id) BETWEEN 1 AND 256),
    run_id bytea NOT NULL,
    instance_id bytea NOT NULL CHECK (octet_length(instance_id) BETWEEN 1 AND 256),
    registered_by bytea NOT NULL CHECK (octet_length(registered_by) BETWEEN 1 AND 256),
    PRIMARY KEY (project, trace_id, span_id),
    FOREIGN KEY (project, run_id) REFERENCES runs(project, run_id)
);
