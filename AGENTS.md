# AI Engineering Rules

These rules apply to AI coding agents working in this repository. Preserve any project-specific requirements and architecture documents.

## 1. Start From the Problem (First Principles)
- Before coding, state in one or two sentences the real problem and hard constraints. If unclear, ask instead of guessing.
- Every design decision must answer why it is necessary. “Other libraries do it” and “we might need it later” are not reasons.

## 2. Do Not Do Unnecessary Work
- Implement only explicitly requested requirements. Do not add speculative features, configuration, abstractions, or extension points.
- Do not refactor unrelated code or alter unrelated behavior.
- Reuse existing code; delete what is unnecessary. Code volume is a cost.
- Justify every new dependency or exported API; default to not adding it.

## 3. Minimal Core, Complete Engineering Quality
Minimal means fewer concepts and interfaces, not reduced rigor. Never omit:
- Correctness for valid, boundary, and invalid inputs.
- Actionable errors; never silently swallow failures.
- Timeouts, cancellation, and cleanup of connections, files, and goroutines.
- Concurrency safety and documentation of concurrent-call guarantees.
- Sufficient critical-path logs or metrics for diagnosing failures, without unnecessary instrumentation.
- Tests for success, failure, and edge cases.

## 4. Engineer the Core Carefully
- Document invariants and semantics, including empty inputs, repeated calls, calls after Close (where applicable), and concurrent calls.
- Document public-operation costs: time complexity, allocations, and potential blocking.
- Base hot-path performance claims on benchmarks, not intuition.
- Name precisely. Comments explain why, not merely what.

## 5. Prefer Stability and Reliability Over Cleverness
- Prefer predictable behavior and one clear way of doing things.
- Fail explicitly; never silently degrade or return partially valid results.
- Choose safe defaults and preserve backward compatibility when changing public behavior.
- Make small changes that can be independently verified and reverted.

## 6. Make Trade-offs Explicit
- Explain rejected alternatives and why.
- When uncertain, choose the simpler, more reversible, smaller change.
- Say explicitly when a feature is out of scope and why; do not implement half a feature.

## 7. Verify Facts Before Changing Code
- Read relevant implementation, interfaces, configuration, and tests before modifying code.
- Never infer existing behavior solely from names, documentation, or previous conversations.
- Verify assumptions against the current repository; disclose what cannot be verified. Do not invent facts.

## 8. Report Work and Verification Truthfully
- Never claim tests passed unless they actually ran and passed.
- Never claim work is implemented unless changes exist.
- Distinguish completed work, verified behavior, unverified behavior, and known limitations.
- Disclose failed tests, incomplete changes, and environment constraints.
- Do not claim production readiness without evidence.

## 9. Respect Architectural Decision Boundaries
Routine implementation decisions within approved scope do not require approval.
Explain impact and obtain approval before breaking public APIs, replacing core algorithms or architecture, changing persistent formats or compatibility contracts, adding major dependencies, or expanding scope.
Do not interrupt for routine reversible details.

## Pre-Commit Checklist
- [ ] Could any new code be deleted while still meeting requirements? Delete it.
- [ ] Is every exported symbol, configuration option, and dependency necessary?
- [ ] Are the engineering-quality requirements in Section 3 met?
- [ ] Are edge cases and failure paths tested?
- [ ] Did I change unrelated code?
- [ ] Did I verify existing behavior instead of assuming it?
- [ ] Are completion and test claims backed by actual results?
- [ ] Did I cross an architectural decision boundary without approval?
