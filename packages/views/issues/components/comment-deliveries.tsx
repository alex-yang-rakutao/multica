"use client";

import { useQuery } from "@tanstack/react-query";
import { useRetryCommentDelivery } from "@multica/core/issues/mutations";
import { issueTasksOptions } from "@multica/core/issues/queries";
import { mentionLabelsByTarget } from "@multica/core/issues/comment-trigger-outcomes";
import type { CommentDelivery, TimelineEntry } from "@multica/core/types";
import { TranscriptButton } from "../../common/task-transcript/transcript-button";
import { Button } from "@multica/ui/components/ui/button";
import { toast } from "sonner";
import { useT } from "../../i18n";
import { blockedReasonLabel } from "../blocked-trigger-copy";

function DeliveryRow({ issueId, entry, delivery, name }: {
  issueId: string;
  entry: TimelineEntry;
  delivery: CommentDelivery;
  name: string;
}) {
  const { t } = useT("issues");
  const retry = useRetryCommentDelivery(issueId);
  const { data: task } = useQuery({
    ...issueTasksOptions(issueId),
    enabled: !!delivery.task_id,
    select: (tasks) => tasks.find((candidate) => candidate.id === delivery.task_id),
  });
  const status = delivery.status;
  const label = status === "blocked"
    ? delivery.reason_code === "run_ended_before_receipt"
      ? t(($) => $.comment_delivery.run_ended)
      : blockedReasonLabel(delivery.reason_code, t)
    : status === "received"
      ? t(($) => $.comment_delivery.received)
      : status === "queued" || status === "coalesced"
        ? t(($) => $.comment_delivery.queued)
        : t(($) => $.comment_delivery.waiting);
  return (
    <div className="flex flex-wrap items-center gap-x-2 text-caption text-muted-foreground">
      <span className="min-w-0 break-words">{name} · {label}</span>
      {task && (
        <TranscriptButton task={task} agentName={name} title={t(($) => $.comment_delivery.view_run)} />
      )}
      {delivery.retryable && (
        <Button
          variant="ghost"
          size="xs"
          disabled={retry.isPending}
          aria-busy={retry.isPending}
          onClick={() => {
            void retry.mutateAsync({
              commentId: entry.id,
              target_type: delivery.target_type,
              target_id: delivery.target_id,
            }).catch(() => toast.error(t(($) => $.comment_delivery.retry_failed)));
          }}
        >
          {t(($) => $.comment_delivery.retry)}
        </Button>
      )}
    </div>
  );
}

export function CommentDeliveries({ issueId, entry, getActorName }: {
  issueId: string;
  entry: TimelineEntry;
  getActorName: (type: string, id: string) => string;
}) {
  const deliveries = entry.deliveries?.filter((delivery) => delivery.status !== "steered" &&
    !entry.supplements?.some((receipt) => receipt.task_id === delivery.task_id && receipt.status === "delivered"));
  if (!deliveries?.length) return null;
  const names = mentionLabelsByTarget(entry.content ?? "");
  return (
    <div className="mt-1 flex flex-col gap-0.5">
      {deliveries.map((delivery) => {
        const key = `${delivery.target_type}:${delivery.target_id}`;
        const name = names.get(key) || getActorName(delivery.target_type, delivery.target_id);
        return <DeliveryRow key={key} issueId={issueId} entry={entry} delivery={delivery} name={name} />;
      })}
    </div>
  );
}
