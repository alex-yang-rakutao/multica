CREATE TABLE IF NOT EXISTS comment_delivery (
    comment_id UUID NOT NULL,
    workspace_id UUID NOT NULL,
    issue_id UUID NOT NULL,
    targets JSONB NOT NULL,
    steer JSONB NOT NULL,
    next_attempt_at TIMESTAMPTZ,
    lease_id UUID,
    lease_until TIMESTAMPTZ
);
