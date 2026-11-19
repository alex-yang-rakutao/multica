import { z } from "zod";
import { parseWithFallback } from "../api/schema";

const IssueRunOutcomeSchema = z.object({
  status: z.enum(["queued", "coalesced", "deferred", "blocked"]),
  reason_code: z.string().default(""),
});

export type IssueRunOutcome = z.output<typeof IssueRunOutcomeSchema>;

export function parseIssueRunOutcome(raw: unknown): IssueRunOutcome | undefined {
  return parseWithFallback<IssueRunOutcome | undefined>(
    raw,
    IssueRunOutcomeSchema.nullish().transform((outcome) => outcome ?? undefined),
    undefined,
    { endpoint: "PUT /api/issues/:id (run_outcome)" },
  );
}
