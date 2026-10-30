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
    trace_id bytea NOT NULL CHECK (octet_length(trace_id) = 16 AND trace_id <> decode(repeat('00', 16), 'hex')),
    span_id bytea NOT NULL CHECK (octet_length(span_id) = 8 AND span_id <> decode(repeat('00', 8), 'hex')),
    run_id bytea NOT NULL,
    instance_id bytea NOT NULL CHECK (octet_length(instance_id) BETWEEN 1 AND 256),
    principal_id bytea NOT NULL CHECK (octet_length(principal_id) BETWEEN 1 AND 256),
    PRIMARY KEY (project, trace_id, span_id, run_id, instance_id, principal_id),
    FOREIGN KEY (project, run_id) REFERENCES runs(project, run_id)
);
CREATE TABLE bindings (
    project bytea NOT NULL,
    trace_id bytea NOT NULL CHECK (octet_length(trace_id) = 16 AND trace_id <> decode(repeat('00', 16), 'hex')),
    span_id bytea NOT NULL CHECK (octet_length(span_id) = 8 AND span_id <> decode(repeat('00', 8), 'hex')),
    run_id bytea NOT NULL,
    instance_id bytea NOT NULL CHECK (octet_length(instance_id) BETWEEN 1 AND 256),
    registered_by bytea NOT NULL CHECK (octet_length(registered_by) BETWEEN 1 AND 256),
    PRIMARY KEY (project, trace_id, span_id),
    FOREIGN KEY (project, run_id) REFERENCES runs(project, run_id)
);
CREATE TABLE reader_grants (
    project bytea NOT NULL REFERENCES projects(id),
    principal_id bytea NOT NULL CHECK (octet_length(principal_id) BETWEEN 1 AND 256),
    PRIMARY KEY (project, principal_id)
);
CREATE TABLE writer_grants (
    project bytea NOT NULL,
    run_id bytea NOT NULL,
    instance_id bytea NOT NULL CHECK (octet_length(instance_id) BETWEEN 1 AND 256),
    principal_id bytea NOT NULL CHECK (octet_length(principal_id) BETWEEN 1 AND 256),
    PRIMARY KEY (project, run_id, instance_id, principal_id),
    FOREIGN KEY (project, run_id) REFERENCES runs(project, run_id)
);
CREATE TABLE records (
    project bytea NOT NULL,
    trace_id bytea NOT NULL,
    span_id bytea NOT NULL,
    kind text NOT NULL CHECK (kind IN ('span','capture')),
    digest bytea NOT NULL CHECK (octet_length(digest) = 32),
    size bigint NOT NULL CHECK (size >= 0),
    owner_run bytea NOT NULL CHECK (octet_length(owner_run) BETWEEN 1 AND 256),
    owner_instance bytea NOT NULL CHECK (octet_length(owner_instance) BETWEEN 1 AND 256),
    submitted_by bytea NOT NULL CHECK (octet_length(submitted_by) BETWEEN 1 AND 256),
    accepted_at timestamptz NOT NULL CHECK (isfinite(accepted_at)),
    expires_at timestamptz NOT NULL CHECK (isfinite(expires_at)),
    marker_until timestamptz NOT NULL CHECK (isfinite(marker_until)),
    expired boolean NOT NULL DEFAULT false,
    PRIMARY KEY (project, trace_id, span_id, kind),
    FOREIGN KEY (project, trace_id, span_id) REFERENCES bindings(project, trace_id, span_id),
    CHECK (expires_at > accepted_at AND marker_until > expires_at)
);
