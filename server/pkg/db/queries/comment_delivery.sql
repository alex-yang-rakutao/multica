-- name: CreateCommentDelivery :exec
INSERT INTO comment_delivery (comment_id, workspace_id, issue_id, targets, steer, next_attempt_at, lease_id, lease_until)
VALUES ($1, $2, $3, $4, $5, now(), $6, now() + interval '2 minutes');

-- name: ListCommentDeliveries :many
SELECT comment_id, targets
FROM comment_delivery
WHERE workspace_id = $1 AND comment_id = ANY(@comment_ids::uuid[]);

-- name: LeaseDueCommentDelivery :one
UPDATE comment_delivery SET lease_id = @lease_id, lease_until = now() + interval '2 minutes'
WHERE comment_id = (
  SELECT comment_id FROM comment_delivery
  WHERE next_attempt_at <= now() AND (lease_until IS NULL OR lease_until <= now())
  ORDER BY next_attempt_at, comment_id
  LIMIT 1 FOR UPDATE SKIP LOCKED
)
RETURNING *;

-- name: LeaseCommentDelivery :one
UPDATE comment_delivery
SET lease_id = @lease_id, lease_until = now() + interval '2 minutes', next_attempt_at = now()
WHERE comment_id = @comment_id AND workspace_id = @workspace_id AND issue_id = @issue_id
  AND (lease_until IS NULL OR lease_until <= now())
RETURNING *;

-- name: SaveCommentDelivery :exec
UPDATE comment_delivery
SET targets = @targets, steer = CASE WHEN @pending::bool THEN steer ELSE 'null'::jsonb END,
    lease_id = NULL, lease_until = NULL,
    next_attempt_at = CASE WHEN @pending::bool THEN now() + interval '30 seconds' END
WHERE comment_id = @comment_id AND lease_id = @lease_id;

-- name: DeleteCommentDelivery :exec
DELETE FROM comment_delivery WHERE comment_id = $1 AND workspace_id = $2;

-- name: DeleteCommentDeliveriesForIssue :exec
DELETE FROM comment_delivery WHERE issue_id = $1 AND workspace_id = $2;

-- name: FindCommentDeliveryRun :one
SELECT id, status, sqlc.arg(comment_id)::uuid = ANY(delivered_comment_ids) AS received
FROM agent_task_queue
WHERE issue_id = @issue_id AND agent_id = @agent_id
  AND EXISTS (SELECT 1 FROM issue WHERE id = @issue_id AND workspace_id = @workspace_id)
  AND (
    sqlc.arg(comment_id)::uuid = ANY(delivered_comment_ids)
    OR trigger_comment_id = sqlc.arg(comment_id)::uuid
    OR sqlc.arg(comment_id)::uuid = ANY(coalesced_comment_ids)
  )
ORDER BY (sqlc.arg(comment_id)::uuid = ANY(delivered_comment_ids)) DESC,
    (status IN ('queued', 'deferred', 'dispatched', 'running', 'waiting_local_directory')) DESC,
    created_at DESC, id DESC
LIMIT 1;

-- name: DeleteWorkspaceCommentDeliveries :exec
DELETE FROM comment_delivery AS delivery
WHERE delivery.workspace_id = $1
  AND delivery.issue_id IN (SELECT i.id FROM issue AS i WHERE i.workspace_id = $1);
