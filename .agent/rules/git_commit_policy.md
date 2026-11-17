---
trigger: always_on
---

# Rule: Git Commit Policy & Job-Based Grouping

> **CRITICAL POLICY**: Never perform automatic `git commit`, `git merge`, `git rebase`, or `git push` operations without explicit user instruction and permission.

---

## Directives

1. **Explicit Permission Required**:
   - **Never** execute `git commit` (or run git commit commands) without explicit, direct instruction from the user (e.g., *"commit these changes"*, *"please commit with message X"*).
   - **Never** automatically commit intermediate changes, refactors, test fixes, or completed implementation plans on your own initiative.

2. **Atomic & Job-Based Grouping**:
   - When requested to commit, **group related files by the same job/task/concern** into separate, cohesive commits.
   - **Never** lump unrelated changes into a single bulk commit (e.g., separate documentation, configuration, C++ engine changes, and TypeScript test fixes into their own distinct commits).

3. **Suitable & Conventional Commit Messages**:
   - Provide clear, descriptive commit messages matching standard conventional commit format (`feat(...)`, `fix(...)`, `test(...)`, `refactor(...)`, `docs(...)`, `chore(...)`).
   - The commit message should accurately explain the specific change and its context.
   - **Include the issue number when available**: if the commit addresses a GitHub issue or ticket (given by the user, named in the branch, or referenced by the task / OpenSpec change), append it at the end of the subject in parentheses:
     - Single issue: `<type>(<scope>): <summary> (#<issue>)` (e.g., `fix(sink): drop fast emits when sink is degraded (#12)`)
     - Multiple issues: `<type>(<scope>): <summary> (#<issue1>, #<issue2>)` (e.g., `fix(provider): verify eth_chainId on HTTP providers (#14, #15)`)
     - If no issue number is associated with the change, omit the `(#...)` suffix; never invent one.

4. **Leave Changes for User Review**:
   - Always leave modified files in the working directory (unstaged or staged) so the user can inspect diffs (`git diff`, `git status`) before deciding to commit.

5. **Report and Suggest**:
   - Upon finishing a task or verification, present the summary of changed files grouped by job/concern, and suggest the exact commit commands and messages for the user to review and approve.