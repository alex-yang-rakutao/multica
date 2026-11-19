// @vitest-environment node
import { describe, expect, it } from "vitest";
import { parseIssueRunOutcome } from "./issue-run-outcome";

describe("parseIssueRunOutcome", () => {
  it("preserves a recognized dispatch result", () => {
    expect(parseIssueRunOutcome({
      target_type: "agent",
      target_id: "agent-1",
      status: "blocked",
      reason_code: "runtime_offline",
    })).toEqual({ status: "blocked", reason_code: "runtime_offline" });
  });

  it.each([undefined, null, { status: "unknown" }, { status: "queued", reason_code: [] }])(
    "ignores absent, unknown and malformed dispatch outcomes (%j)",
    (outcome) => expect(parseIssueRunOutcome(outcome)).toBeUndefined(),
  );
});
