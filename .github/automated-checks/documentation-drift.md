Detect when documentation becomes inaccurate or inconsistent with repository reality.

**Problem:** Documentation may become out of sync with the actual repository state for various reasons:
- Files, directories, or workflows are added/removed/renamed
- Content in files changes but descriptions don't update
- Documentation contradicts itself across different files
- Lists become incomplete or contain items that no longer exist

**Your Goal: Maintain documentation accuracy**
- Verify documentation matches repository reality
- Ensure consistency across all documentation files
- Flag factual inaccuracies and contradictions
- Do NOT suggest restructuring or style improvements
- Focus on correctness, not completeness

---

## Detection Strategy

### Phase 1: Discovery

**Discover all documentation files**
- Find all `README.md` and `*.md` files in the repository
- Exclude: `node_modules/`, `vendor/`, `.venv/`, `dist/`, `build/`, `CHANGELOG.md`, `LICENSE.md`
- Identify which are documentation vs automated check prompts or templates

### Phase 2: Claim Extraction

**Extract factual claims from each documentation file**

| Claim Type | Examples |
|------------|----------|
| **Existence claims** | "The `src/` directory contains...", "Run `npm test` to..." |
| **Content claims** | "The config file defines...", "This function returns..." |
| **Reference claims** | Links to files, sections, or external URLs |
| **Behavioral claims** | "The workflow runs on push...", "This command outputs..." |
| **List claims** | Enumerated files, features, dependencies, or components |

### Phase 3: Verification

**For each factual claim, verify against repository reality:**

| Verification Type | Method |
|-------------------|--------|
| File/directory existence | `ls`, `find`, or file read |
| Content accuracy | Read file and compare |
| Link validity | Check target exists (internal) or is reachable (external) |
| Command accuracy | Verify command exists and arguments are valid |
| List completeness | Compare documented list against actual items matching the pattern |

### Phase 4: Cross-Reference Check

**Check for contradictions between documentation files:**
- Do different files describe the same thing differently?
- Are lists in different files inconsistent?
- Do cross-references align?
- Are version numbers consistent across files?

---

## What Constitutes Drift

**IS drift (report these):**
- ✅ Documentation lists something that doesn't exist in the repository
- ✅ Documentation missing something that exists AND matches an established pattern (e.g., a variant directory not listed where all others are)
- ✅ Documentation describes content that differs from actual file contents
- ✅ Documentation contradicts itself across different files
- ✅ Broken internal links or references to non-existent files/sections
- ✅ Inaccurate descriptions of behavior or content
- ✅ Outdated version numbers or dependency versions

**Is NOT drift (ignore these):**
- ❌ Documentation being incomplete (not documenting everything)
- ❌ Style preferences or formatting suggestions
- ❌ Opinions on what should be documented
- ❌ Suggestions for better organization
- ❌ Missing documentation for new features (unless a list claims to be exhaustive)
- ❌ External link validity (these can break without repository changes)

---

## Reporting Protocol

### If drift detected:

1. **Check for existing issue**
   ```bash
   gh issue list --state open --search "Documentation accuracy issues" --json number,title
   ```
   - If an issue with title "Documentation accuracy issues" exists, exit without creating a duplicate

2. **Create GitHub issue**

   **Issue creation constraints (CRITICAL):**
   - You have NO write access - not to `/tmp`, not to the workspace
   - Heredocs, pipes, and file operations are ALL blocked
   - You MUST use a single inline `--body "..."` argument
   - Example pattern:
     ```bash
     gh issue create --title "Documentation accuracy issues" --body "@claude Please fix the documentation accuracy issues:

     ## Accuracy issues found

     ### Issue 1: [Brief description]
     - **File:** README.md:42
     - **Claims:** ...
     ..."
     ```

3. **Include in the issue body EXACTLY as shown below** (this is a template - the issue handler will follow these instructions):
   1. Start with: "@claude Please fix the documentation accuracy issues:"
   2. **Accuracy issues found:** For each issue:
      - Documentation file path and line number (if applicable)
      - What the documentation claims
      - What actually exists or is accurate in the repository
      - Type of issue (missing item, incorrect description, contradiction, broken link, outdated version)
   3. **Fixing instructions:**
      - Update documentation to match repository reality
      - Ensure consistency across all documentation files
      - Fix or remove broken links
      - Verify all factual claims are accurate
      - Verify all files end with trailing newlines
      - Commit documentation updates
      - Push your changes
      - Include these instructions exactly as written:
        > **CRITICAL - CI Verification:** After pushing, run `gh run list --branch <your-branch> --limit 5` to check CI status. Wait for checks to complete. If any checks fail, fix the issues and push again. Do NOT proceed until CI passes.
        > **CRITICAL - PR Creation:** Run `gh pr create` directly. Do NOT provide a link - execute the command. You have the necessary permissions.

### If no drift detected:

- Exit successfully without creating an issue
- Log: "Documentation accuracy check passed - no drift detected"

---

## Edge Cases

### Generated documentation
- Skip files in directories that appear to be generated (e.g., `docs/api/` with only auto-generated content)
- Focus on human-authored documentation

### Monorepo considerations
- Check documentation at both root level and within subdirectories
- Ensure cross-package references are accurate
