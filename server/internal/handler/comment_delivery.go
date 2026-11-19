package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/projectaccess"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
)

type CommentDelivery struct {
	CommentTriggerOutcome
	TaskID    string `json:"task_id,omitempty"`
	Retryable bool   `json:"retryable,omitempty"`
}

type commentDeliveryTarget struct {
	CommentDelivery
	AgentID  string `json:"agent_id,omitempty"`
	Explicit bool   `json:"explicit,omitempty"`
	Replayed bool   `json:"replayed,omitempty"`
}

const (
	commentDeliveryPending  DispatchStatus     = "pending"
	commentDeliveryReceived DispatchStatus     = "received"
	commentDeliveryRunEnded DispatchReasonCode = "run_ended_before_receipt"
)

func planCommentDeliveries(triggers []commentAgentTrigger, targets []commentMentionTarget) []commentDeliveryTarget {
	var plan []commentDeliveryTarget
	executors := make(map[string]bool, len(triggers))
	for _, trigger := range triggers {
		executors[uuidToString(trigger.Agent.ID)] = true
	}
	named := make(map[string]bool, len(targets))
	for _, target := range targets {
		status := target.Status
		if target.ExecAgentID != "" {
			if !executors[target.ExecAgentID] {
				continue
			}
			named[target.ExecAgentID] = true
			status = commentDeliveryPending
		}
		if status == "" {
			continue
		}
		plan = append(plan, commentDeliveryTarget{
			AgentID:  target.ExecAgentID,
			Explicit: true,
			CommentDelivery: CommentDelivery{
				CommentTriggerOutcome: CommentTriggerOutcome{
					TargetType: target.TargetType, TargetID: target.TargetID, Status: status, ReasonCode: target.ReasonCode,
				},
			},
		})
	}
	for _, trigger := range triggers {
		id := uuidToString(trigger.Agent.ID)
		if named[id] {
			continue
		}
		plan = append(plan, commentDeliveryTarget{
			AgentID: id,
			CommentDelivery: CommentDelivery{
				CommentTriggerOutcome: CommentTriggerOutcome{TargetType: "agent", TargetID: id, Status: commentDeliveryPending},
			},
		})
	}
	return plan
}

func deliveryResponses(targets []commentDeliveryTarget) []CommentDelivery {
	var results []CommentDelivery
	for _, target := range targets {
		if target.ReasonCode == ReasonSelfTriggerSuppressed {
			continue
		}
		receipt := target.CommentDelivery
		receipt.Retryable = receipt.Status == DispatchBlocked
		results = append(results, receipt)
	}
	return results
}

func (h *Handler) saveCommentDeliveries(ctx context.Context, commentID, leaseID pgtype.UUID, targets []commentDeliveryTarget) error {
	payload, err := json.Marshal(targets)
	if err != nil {
		return err
	}
	pending := slices.ContainsFunc(targets, func(target commentDeliveryTarget) bool {
		switch target.Status {
		case commentDeliveryPending, DispatchQueued, DispatchCoalesced, DispatchDeferred, DispatchSteered:
			return true
		default:
			return false
		}
	})
	return h.Queries.SaveCommentDelivery(ctx, db.SaveCommentDeliveryParams{
		CommentID: commentID, LeaseID: leaseID, Targets: payload, Pending: pending,
	})
}

// Only the original author gets delivery details. A different viewer may be
// able to read the issue without access to the private agent named in a receipt.
func (h *Handler) listCommentDeliveries(r *http.Request, workspaceID pgtype.UUID, comments []db.Comment) map[pgtype.UUID][]CommentDelivery {
	kind, id := h.resolveActor(r, requestUserID(r), uuidToString(workspaceID))
	var ids []pgtype.UUID
	for _, comment := range comments {
		if !comment.DeletedAt.Valid && comment.AuthorType == kind && uuidToString(comment.AuthorID) == id {
			ids = append(ids, comment.ID)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	rows, err := h.Queries.ListCommentDeliveries(r.Context(), db.ListCommentDeliveriesParams{WorkspaceID: workspaceID, CommentIds: ids})
	if err != nil {
		slog.Warn("list comment deliveries", "error", err)
		return nil
	}
	results := make(map[pgtype.UUID][]CommentDelivery, len(rows))
	for _, row := range rows {
		var targets []commentDeliveryTarget
		if err := json.Unmarshal(row.Targets, &targets); err == nil {
			results[row.CommentID] = deliveryResponses(targets)
		}
	}
	return results
}

func (h *Handler) SweepCommentDeliveries(ctx context.Context) error {
	var failures []error
	for range 100 {
		record, err := h.Queries.LeaseDueCommentDelivery(ctx, dbid.NewV7())
		if errors.Is(err, pgx.ErrNoRows) {
			break
		}
		if err != nil {
			return errors.Join(append(failures, err)...)
		}
		if _, err := h.processCommentDeliveries(ctx, record, "", "", ""); err != nil {
			failures = append(failures, fmt.Errorf("comment %s: %w", uuidToString(record.CommentID), err))
		}
	}
	return errors.Join(failures...)
}

// Completion and the sweeper share the lease when replaying tracked inputs.
func (h *Handler) resumeTrackedCommentDelivery(ctx context.Context, comment db.Comment, agentID string) bool {
	rows, err := h.Queries.ListCommentDeliveries(ctx, db.ListCommentDeliveriesParams{
		WorkspaceID: comment.WorkspaceID, CommentIds: []pgtype.UUID{comment.ID},
	})
	if err != nil {
		slog.Warn("load tracked comment delivery", "comment_id", uuidToString(comment.ID), "error", err)
		return true
	}
	if len(rows) == 0 {
		return false
	}
	var targets []commentDeliveryTarget
	if err := json.Unmarshal(rows[0].Targets, &targets); err != nil {
		return false
	}
	if !slices.ContainsFunc(targets, func(target commentDeliveryTarget) bool {
		// Steering has its own receipt and completion fallback.
		return target.AgentID == agentID && target.Status != DispatchSteered
	}) {
		return false
	}
	record, err := h.Queries.LeaseCommentDelivery(ctx, db.LeaseCommentDeliveryParams{
		CommentID: comment.ID, WorkspaceID: comment.WorkspaceID, IssueID: comment.IssueID, LeaseID: dbid.NewV7(),
	})
	if err == nil {
		_, err = h.processCommentDeliveries(ctx, record, "", "", agentID)
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		slog.Warn("resume tracked comment delivery", "comment_id", uuidToString(comment.ID), "error", err)
	}
	// A dispatcher holding the lease will finish or leave the row recoverable.
	return true
}

func (h *Handler) processCommentDeliveries(ctx context.Context, record db.CommentDelivery, retryType, retryID, agentFilter string) ([]CommentDelivery, error) {
	comment, err := h.Queries.GetCommentInWorkspace(ctx, db.GetCommentInWorkspaceParams{ID: record.CommentID, WorkspaceID: record.WorkspaceID})
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && comment.DeletedAt.Valid) {
		return nil, h.Queries.DeleteCommentDelivery(ctx, db.DeleteCommentDeliveryParams{CommentID: record.CommentID, WorkspaceID: record.WorkspaceID})
	}
	if err != nil {
		return nil, err
	}
	issue, err := h.Queries.GetIssueInWorkspace(ctx, db.GetIssueInWorkspaceParams{ID: record.IssueID, WorkspaceID: record.WorkspaceID})
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && comment.IssueID != issue.ID) {
		return nil, h.Queries.DeleteCommentDelivery(ctx, db.DeleteCommentDeliveryParams{CommentID: record.CommentID, WorkspaceID: record.WorkspaceID})
	}
	if err != nil {
		return nil, err
	}
	var targets []commentDeliveryTarget
	if err := json.Unmarshal(record.Targets, &targets); err != nil {
		return nil, err
	}
	var steer commentSteerRequest
	if err := json.Unmarshal(record.Steer, &steer); err != nil {
		return nil, err
	}
	actorID := uuidToString(comment.AuthorID)
	originatorID := actorID
	if comment.AuthorType == "agent" {
		originatorID = uuidToString(h.TaskService.ResolveOriginatorFromTriggerComment(ctx, issue.WorkspaceID, comment.ID))
	}
	allowed, err := h.canReplayCommentDelivery(ctx, issue, comment, originatorID)
	if err != nil {
		return nil, err
	}
	var triggers []commentAgentTrigger
	var named []commentMentionTarget
	if allowed && !isNoteComment(comment.Content) {
		var parent *db.Comment
		if comment.ParentID.Valid {
			row, err := h.Queries.GetCommentInWorkspace(ctx, db.GetCommentInWorkspaceParams{ID: comment.ParentID, WorkspaceID: issue.WorkspaceID})
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return nil, err
			}
			if err == nil {
				parent = &row
			}
		}
		triggers, named = h.computeCommentAgentTriggers(ctx, issue, comment.Content, parent, comment.AuthorType, actorID, commentTriggerComputeOptions{
			ExcludeTriggerCommentID: comment.ID, AuthoringTaskID: comment.SourceTaskID, OriginatorUserID: originatorID,
		})
		triggers = filterSuppressedCommentAgentTriggers(triggers, comment.SuppressedAgentIds)
	}
	receipts, err := h.lookupCommentSupplements(ctx, issue.WorkspaceID, []pgtype.UUID{comment.ID})
	if err != nil {
		return nil, err
	}
	supplements := receipts[uuidToString(comment.ID)]
	attempted := make(map[string]commentEnqueueResult)
	for i := range targets {
		target := &targets[i]
		if agentFilter != "" && target.AgentID != agentFilter {
			continue
		}
		retry := target.Status == DispatchBlocked && target.TargetType == retryType && target.TargetID == retryID
		if target.Status == commentDeliveryReceived || (target.Status == DispatchBlocked && !retry) {
			continue
		}
		if err := h.updateCommentDeliveryTarget(ctx, issue, comment, target, triggers, named, supplements, steer, originatorID, attempted, retry); err != nil {
			return nil, err
		}
	}
	if err := h.saveCommentDeliveries(ctx, record.CommentID, record.LeaseID, targets); err != nil {
		return nil, err
	}
	return deliveryResponses(targets), nil
}

func (h *Handler) canReplayCommentDelivery(ctx context.Context, issue db.Issue, comment db.Comment, originatorID string) (bool, error) {
	if comment.AuthorType != "member" && comment.AuthorType != "agent" {
		return false, nil
	}
	originator, parseErr := util.ParseUUID(originatorID)
	if parseErr != nil {
		return false, nil
	}
	member, err := h.Queries.GetMemberByUserAndWorkspace(ctx, db.GetMemberByUserAndWorkspaceParams{UserID: originator, WorkspaceID: issue.WorkspaceID})
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	guest := projectaccess.IsGuestRole(member.Role)
	if !issue.ProjectID.Valid {
		return !guest, nil
	}
	if comment.AuthorType == "member" || guest {
		hidden, err := projectaccess.ForMember(ctx, h.Queries, member)
		if err != nil || !hidden.CanSeeProject(issue.ProjectID) {
			return false, err
		}
		if comment.AuthorType == "member" {
			return true, nil
		}
	}
	hidden, err := h.hiddenProjectsForAgentRun(ctx, issue.WorkspaceID, uuidToString(comment.AuthorID), uuidToString(comment.SourceTaskID))
	return err == nil && hidden.CanSeeProject(issue.ProjectID), err
}

func (h *Handler) updateCommentDeliveryTarget(
	ctx context.Context, issue db.Issue, comment db.Comment, target *commentDeliveryTarget,
	triggers []commentAgentTrigger, named []commentMentionTarget, supplements []CommentSupplementResponse,
	steer commentSteerRequest, originatorID string, attempted map[string]commentEnqueueResult, retry bool,
) error {
	previousStatus := target.Status
	replayUnacknowledged := false
	idx := slices.IndexFunc(named, func(candidate commentMentionTarget) bool {
		return candidate.TargetID == target.TargetID && candidate.TargetType == target.TargetType
	})
	agentID := target.AgentID
	if agentID == "" && idx >= 0 {
		agentID = named[idx].ExecAgentID
	}
	// Respect the original execution target after edits and reassignment.
	triggerIdx := slices.IndexFunc(triggers, func(trigger commentAgentTrigger) bool {
		return uuidToString(trigger.Agent.ID) == agentID
	})
	if target.Explicit && (idx < 0 || named[idx].ExecAgentID != agentID) {
		triggerIdx = -1
	}
	if agentID != "" {
		target.AgentID = agentID
		for _, receipt := range supplements {
			if receipt.AgentID != agentID {
				continue
			}
			switch receipt.Status {
			case "pending", "delivering":
				target.Status, target.TaskID = DispatchSteered, receipt.TaskID
				return nil
			case "delivered":
				target.Status, target.TaskID = commentDeliveryReceived, receipt.TaskID
				return nil
			}
		}
		run, err := h.Queries.FindCommentDeliveryRun(ctx, db.FindCommentDeliveryRunParams{
			CommentID: comment.ID, IssueID: issue.ID, WorkspaceID: issue.WorkspaceID, AgentID: parseUUID(agentID),
		})
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if err == nil {
			if run.Received {
				target.Status, target.TaskID = commentDeliveryReceived, uuidToString(run.ID)
				return nil
			}
			switch run.Status {
			case "queued", "deferred", "dispatched", "running", "waiting_local_directory":
				target.TaskID = uuidToString(run.ID)
				if previousStatus != DispatchDeferred {
					target.Status = DispatchQueued
				}
				if previousStatus != DispatchDeferred || run.Status == "queued" || run.Status == "deferred" {
					return nil
				}
			default:
				// A completed legacy run may have omitted a planned comment from
				// its claim receipt. Keep the automatic follow-up, with a bound
				// in case the next run also ends without a receipt.
				replayUnacknowledged = run.Status == "completed" && !target.Replayed
				if !retry && !replayUnacknowledged && previousStatus != DispatchDeferred && previousStatus != commentDeliveryPending {
					target.Status, target.ReasonCode = DispatchBlocked, commentDeliveryRunEnded
					return nil
				}
			}
		}
	}
	if triggerIdx < 0 {
		target.Status, target.ReasonCode = DispatchBlocked, ReasonTargetUnavailable
		if idx >= 0 && named[idx].ReasonCode != "" {
			target.ReasonCode = named[idx].ReasonCode
		}
		return nil
	}
	trigger := triggers[triggerIdx]
	result, exists := attempted[agentID]
	if !exists {
		if previousStatus == commentDeliveryPending && !steer.empty() {
			_, steered := h.steerCommentAgentTriggers(ctx, issue, comment, comment.AuthorType, uuidToString(comment.AuthorID), originatorID, []commentAgentTrigger{trigger}, steer)
			result, exists = steered[agentID]
		}
		if !exists {
			result.status, result.reason = h.resolveCommentTriggerEnqueue(ctx, issue, trigger, comment.ID)
		}
		attempted[agentID] = result
	}
	target.Status, target.ReasonCode, target.TaskID = result.status, result.reason, ""
	if replayUnacknowledged && (result.status == DispatchQueued || result.status == DispatchCoalesced) {
		target.Replayed = true
	}
	if result.status == DispatchBlocked && result.reason == ReasonInternalError && !retry {
		active, err := h.hasActiveTaskForIssueAndAgent(ctx, issue.ID, trigger.Agent.ID, comment.ID)
		if err != nil {
			return err
		}
		if active {
			target.Status, target.ReasonCode = DispatchDeferred, ReasonDeferred
		}
	}
	return nil
}

func (h *Handler) RetryCommentDelivery(w http.ResponseWriter, r *http.Request) {
	issue, ok := h.loadIssueForUser(w, r, chi.URLParam(r, "id"))
	if !ok {
		return
	}
	id, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "commentId"), "comment_id")
	if !ok {
		return
	}
	var req struct {
		TargetType string `json:"target_type"`
		TargetID   string `json:"target_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || (req.TargetType != "agent" && req.TargetType != "squad") {
		writeError(w, http.StatusBadRequest, "invalid delivery target")
		return
	}
	if _, ok := parseUUIDOrBadRequest(w, req.TargetID, "target_id"); !ok {
		return
	}
	comment, err := h.Queries.GetCommentInWorkspace(r.Context(), db.GetCommentInWorkspaceParams{ID: id, WorkspaceID: issue.WorkspaceID})
	actorType, actorID := h.resolveActor(r, requestUserID(r), uuidToString(issue.WorkspaceID))
	if err != nil || comment.IssueID != issue.ID || comment.DeletedAt.Valid || comment.AuthorType != actorType || uuidToString(comment.AuthorID) != actorID {
		writeError(w, http.StatusNotFound, "comment not found")
		return
	}
	record, err := h.Queries.LeaseCommentDelivery(r.Context(), db.LeaseCommentDeliveryParams{
		CommentID: id, WorkspaceID: issue.WorkspaceID, IssueID: issue.ID, LeaseID: dbid.NewV7(),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusConflict, "comment delivery is unavailable or in progress")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load comment delivery")
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 30*time.Second)
	defer cancel()
	deliveries, err := h.processCommentDeliveries(ctx, record, req.TargetType, req.TargetID, "")
	if err != nil {
		slog.Warn("retry comment delivery", "comment_id", uuidToString(id), "error", err)
		writeError(w, http.StatusInternalServerError, "failed to retry comment delivery")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deliveries": deliveries})
}
