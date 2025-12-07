Enforce strict boolean expressions for Python Optional types, equivalent to TypeScript's `@typescript-eslint/strict-boolean-expressions` rule.

**THE CORE RULE: Do not auto-coerce Optional types to boolean.**

This codebase uses explicit boolean checks in TypeScript (enforced by @typescript-eslint/strict-boolean-expressions).
Python must follow the same strict standard.

**What is auto-coercion?**
Auto-coercion happens when a value is used in a context that expects a boolean WITHOUT an explicit operator that returns bool.

**Violations (auto-coercion to bool on Optional types):**
- `if optional_value:` → auto-coerces `Optional[T]` to `bool` ❌
- `if not optional_value:` → auto-coerces `Optional[T]` to `bool` ❌
- `value if optional_value else default` → auto-coerces `Optional[T]` to `bool` ❌

**NOT violations (explicit operators that return bool):**
- `if optional_value is True:` → `is` operator returns `bool` ✅
- `if optional_value is not None:` → `is not` operator returns `bool` ✅
- `if optional_value == something:` → `==` operator returns `bool` ✅
- `if optional_value is False:` → `is` operator returns `bool` ✅
- `if optional_value != something:` → `!=` operator returns `bool` ✅

**NOT violations (truthiness on plain non-Optional types):**
- `if plain_str:` → OK for `str` (not `Optional[str]`) ✅
- `if plain_list:` → OK for `List[T]` (not `Optional[List[T]]`) ✅
- `if plain_bool:` → OK for `bool` (not `Optional[bool]`) ✅

**IMPORTANT:** Only flag violations where the variable has type `Optional[T]` for some T.

**DO NOT rationalize violations as "safe":**
- "This field can never be None in practice" - IRRELEVANT, type is Optional
- "ETags are always non-empty strings" - IRRELEVANT, type is Optional
- "This value is guaranteed to exist" - IRRELEVANT, type is Optional

**CRITICAL: Three-Case Problem - how to fix violations:**

When you find a violation, determine the semantic intent:

**For `if optional_value:` violations:**
The implicit check produces {truthy} vs {falsy, None}. Determine the actual intent:
1. **Check if value is truthy** - Want: {truthy} vs {falsy, None}
2. **Check if field exists** - Want: {truthy, falsy} vs {None}
3. **Check if not explicitly falsy** - Want: {truthy, None} vs {falsy}

**For `if not optional_value:` violations:**
The implicit check produces {falsy, None} vs {truthy}. Determine the actual intent:
1. **Check if value is falsy** - Want: {falsy, None} vs {truthy}
2. **Check if field is missing** - Want: {None} vs {truthy, falsy}
3. **Check if explicitly falsy** - Want: {falsy} vs {truthy, None}

**How to fix each case:**

**For Optional[bool]:**
- Intent 1 (is truthy): `optional_value is True`
- Intent 2 (exists): `optional_value is not None`
- Intent 3 (not False): `optional_value is not False`

**For Optional[int]:**
- Intent 1 (is truthy): `optional_value not in (0, None)`
- Intent 2 (exists): `optional_value is not None`
- Intent 3 (not zero): `optional_value != 0`

**For Optional[str]:**
- Intent 1 (is truthy): `optional_value not in ("", None)`
- Intent 2 (exists): `optional_value is not None`
- Intent 3 (not empty): `optional_value != ""`

**For Optional[List[T]]:**
- Intent 1 (is truthy): `optional_value not in ([], None)`
- Intent 2 (exists): `optional_value is not None`
- Intent 3 (not empty): `optional_value != []`

**Detection Strategy:**

You must identify the TYPE of each variable before flagging it. Only flag if BOTH conditions are true:
1. The variable/expression has type `Optional[T]` (check type annotations, TypedDict definitions, function signatures)
2. It's used in an auto-coercing context (`if var:`, `if not var:`, `x if var else y`)

**Task:**
1. Search Python files for variables with Optional types
2. Check if those variables are used in auto-coercing contexts
3. IGNORE any code using explicit operators (is, is not, ==, !=) - NOT violations
4. For each violation, determine the underlying type (bool, int, str, List, etc.)
5. Create a detailed report (file path, line number, code snippet, field type)

**If violations found:**
- First check if an open issue with title "Python boolean violations detected" already exists
- If such an issue exists, exit without creating a duplicate
- If no such issue exists, create a GitHub issue with title "Python boolean violations detected"
- **Issue creation constraints (CRITICAL):**
  - You have NO write access - not to `/tmp`, not to the workspace
  - Heredocs, pipes, and file operations are ALL blocked
  - You MUST use a single inline `--body "..."` argument
  - Example pattern:
    ```bash
    gh issue create --title "Python boolean violations detected" --body "@claude Please fix these violations:

    ## Violations found

    ### File: python/src/module.py
    - **Line 42:** \`if optional_value:\`
    - **Type:** Optional[str]
    ..."
    ```
- Include in the issue body EXACTLY as shown below (this is a template - the issue handler will follow these instructions):
  1. Start with: "@claude Please fix these violations:"
  2. The detailed violation report (file path, line number, code snippet, field type for each violation)
  3. **CRITICAL fixing instructions - three-case problem:**

     For EACH violation, you MUST:
     a) **Determine the semantic intent** by examining:
        - Surrounding code context
        - What None means for this field (unset vs disabled)
        - Whether falsy values should execute or skip
        - TypeScript implementation if available (match the pattern: === true, !== undefined, !== false)

     b) **Choose the correct explicit form** based on intent:
        - If checking "is truthy" (Intent 1): Use appropriate form from table above
        - If checking "exists" (Intent 2): Use `is not None`
        - If checking "not explicitly falsy" (Intent 3): Use appropriate form from table above

  4. After fixing:
     - Commit and push your changes
     - Include these instructions exactly as written:
       > **CRITICAL - CI Verification:** After pushing, run `gh run list --branch <your-branch> --limit 5` to check CI status. Wait for checks to complete. If any checks fail, fix the issues and push again. Do NOT proceed until CI passes.
       > **CRITICAL - PR Creation:** Run `gh pr create` directly. Do NOT provide a link - execute the command. You have the necessary permissions.

**If no violations found:**
- Exit successfully without creating an issue
- Log that no violations were found
