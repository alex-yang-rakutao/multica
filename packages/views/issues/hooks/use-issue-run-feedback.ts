"use client";

import { useCallback } from "react";
import { toast } from "sonner";
import type { IssueRunOutcome } from "@multica/core/issues/issue-run-outcome";
import { useT } from "../../i18n";
import { blockedReasonLabel } from "../blocked-trigger-copy";

export function useIssueRunFeedback() {
  const { t } = useT("issues");
  return useCallback((outcome: IssueRunOutcome) => {
    switch (outcome.status) {
      case "queued":
        toast.success(t(($) => $.run_feedback.queued));
        break;
      case "coalesced":
        toast.info(t(($) => $.run_feedback.covered));
        break;
      case "deferred":
        toast.info(outcome.reason_code === "already_active"
          ? t(($) => $.run_feedback.covered)
          : t(($) => $.run_feedback.deferred));
        break;
      case "blocked":
        toast.warning(t(($) => $.run_feedback.blocked), {
          description: blockedReasonLabel(outcome.reason_code, t),
        });
        break;
    }
  }, [t]);
}
