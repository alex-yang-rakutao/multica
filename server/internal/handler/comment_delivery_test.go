package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
)

func TestCommentDeliveryRecoversAnInterruptedDispatch(t *testing.T) {
	ctx := context.Background()
	agentID := createHandlerTestAgent(t, "Recoverable comment delivery", nil)
	issueID := createCommentTriggerPreviewIssue(t, "Recoverable comment delivery", "", "")
	commentID := dbfx.Comment(t, issueID, "[@Agent](mention://agent/"+agentID+") please review", nil)

	// The comment and its intent are committed, but the process stopped before dispatch.
	targets, err := json.Marshal([]commentDeliveryTarget{{
		AgentID:  agentID,
		Explicit: true,
		CommentDelivery: CommentDelivery{CommentTriggerOutcome: CommentTriggerOutcome{
			TargetType: "agent", TargetID: agentID, Status: commentDeliveryPending,
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	dbfx.Exec(t, `
		INSERT INTO comment_delivery (comment_id, workspace_id, issue_id, targets, steer, next_attempt_at)
		VALUES ($1, $2, $3, $4, 'null', now())
	`, commentID, testWorkspaceID, issueID, targets)
	t.Cleanup(func() {
		_, _ = testPool.Exec(ctx, "DELETE FROM comment_delivery WHERE comment_id = $1", commentID)
	})

	retry := func() []CommentDelivery {
		t.Helper()
		record, err := testHandler.Queries.LeaseCommentDelivery(ctx, db.LeaseCommentDeliveryParams{
			CommentID: parseUUID(commentID), WorkspaceID: parseUUID(testWorkspaceID),
			IssueID: parseUUID(issueID), LeaseID: dbid.NewV7(),
		})
		if err != nil {
			t.Fatal(err)
		}
		outcomes, err := testHandler.processCommentDeliveries(ctx, record, "", "", "")
		if err != nil {
			t.Fatal(err)
		}
		return outcomes
	}

	if results := retry(); len(results) != 1 || results[0].Status != DispatchQueued {
		t.Fatalf("recovered outcomes = %+v, want one queued delivery", results)
	}
	if results := retry(); len(results) != 1 || results[0].Status != DispatchQueued || results[0].TaskID == "" {
		t.Fatalf("replayed outcomes = %+v, want the existing queued task", results)
	}
	if got := countQueuedCommentTriggerTasks(t, issueID, agentID); got != 1 {
		t.Fatalf("recovery created %d tasks, want 1", got)
	}
	// An older runtime can complete a run before acknowledging its batched
	// input. A completed input without a receipt still earns a follow-up.
	dbfx.Exec(t, `
		UPDATE agent_task_queue SET status = 'completed'
		WHERE issue_id = $1 AND agent_id = $2 AND status = 'queued'
	`, issueID, agentID)
	if results := retry(); len(results) != 1 || results[0].Status != DispatchQueued {
		t.Fatalf("delivery after a completed run without a receipt = %+v, want a queued follow-up", results)
	}
	if got := countQueuedCommentTriggerTasks(t, issueID, agentID); got != 1 {
		t.Fatalf("queued follow-ups = %d, want 1", got)
	}

	dbfx.Exec(t, `
		UPDATE agent_task_queue SET delivered_comment_ids = ARRAY[$1::uuid]
		WHERE issue_id = $2 AND agent_id = $3
	`, commentID, issueID, agentID)
	if results := retry(); len(results) != 1 || results[0].Status != commentDeliveryReceived {
		t.Fatalf("outcomes after receipt = %+v, want received", results)
	}
}

func TestCommentDeliveryUsesCurrentGuestProjectAccess(t *testing.T) {
	f := newGuestFixture(t)
	ctx := context.Background()
	issue := db.Issue{WorkspaceID: parseUUID(testWorkspaceID)}
	comment := db.Comment{AuthorType: "member", AuthorID: parseUUID(f.guest)}
	check := func(want bool) {
		t.Helper()
		got, err := testHandler.canReplayCommentDelivery(ctx, issue, comment, f.guest)
		if err != nil || got != want {
			t.Fatalf("can replay in project %v = %v, %v; want %v", issue.ProjectID, got, err, want)
		}
	}

	dbfx.Exec(t, `UPDATE member SET role = 'member' WHERE id = $1`, f.guestMember)
	check(true)
	dbfx.Exec(t, `UPDATE member SET role = 'guest' WHERE id = $1`, f.guestMember)
	check(false)
	issue.ProjectID = parseUUID(f.other)
	check(false)
	issue.ProjectID = parseUUID(f.shared)
	check(true)
}

func TestRetryCommentDeliveryKeepsOriginalComment(t *testing.T) {
	agentID := createHandlerTestAgent(t, "Retryable comment delivery", nil)
	issueID := createCommentTriggerPreviewIssue(t, "Retryable comment delivery", "", "")
	dbfx.Exec(t, `UPDATE agent SET runtime_id = NULL WHERE id = $1`, agentID)

	content := "[@Agent](mention://agent/" + agentID + ") please review"
	var posted CommentResponse
	testutil.Call(t, testHandler.CreateComment, withURLParam(
		newRequest(http.MethodPost, "/api/issues/"+issueID+"/comments", map[string]any{"content": content}),
		"id", issueID,
	)).Want(http.StatusCreated).JSON(&posted)
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), "DELETE FROM comment_delivery WHERE comment_id = $1", posted.ID)
	})
	if len(posted.Deliveries) != 1 || !posted.Deliveries[0].Retryable {
		t.Fatalf("delivery with missing runtime = %+v, want one retryable delivery", posted.Deliveries)
	}

	dbfx.Exec(t, `UPDATE agent SET runtime_id = $2 WHERE id = $1`, agentID, handlerTestRuntimeID(t))
	for range 2 {
		var response struct {
			Deliveries []CommentDelivery `json:"deliveries"`
		}
		testutil.Call(t, testHandler.RetryCommentDelivery, testutil.WithURLParams(
			newRequest(http.MethodPost, "/api/issues/"+issueID+"/comments/"+posted.ID+"/deliveries/retry",
				map[string]string{"target_type": "agent", "target_id": agentID}),
			"id", issueID, "commentId", posted.ID),
		).Want(http.StatusOK).JSON(&response)
		if len(response.Deliveries) != 1 || response.Deliveries[0].Status != DispatchQueued {
			t.Fatalf("retry deliveries = %+v, want queued", response.Deliveries)
		}
	}
	var count int
	dbfx.QueryRow(t, `SELECT count(*) FROM comment WHERE issue_id = $1 AND content = $2`, issueID, content).Scan(&count)
	if count != 1 {
		t.Fatalf("matching comments = %d, want only the original comment", count)
	}
	if got := countQueuedCommentTriggerTasks(t, issueID, agentID); got != 1 {
		t.Fatalf("queued runs = %d, want one run after retrying twice", got)
	}
}
