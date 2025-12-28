-- SQL Filesystem with Version Control (schema: fs)
-- =================================================
--
-- ## High-level model (Git-like, but linear)
--
-- - **Repositories** (`fs.repositories`) are virtual drives/projects.
-- - **Branches** (`fs.branches`) are named pointers to a single head commit
--   (or `NULL` for an empty branch).
-- - **Commits** (`fs.commits`) form a parent-linked tree where each commit has
--   **at most one parent** (`parent_commit_id`). Only the first/root commit in a
--   repository may have `parent_commit_id IS NULL`.
--   Merge commits remain linear by recording the merged source in
--   `merged_from_commit_id`.
-- - **Files** (`fs.files`) are stored as immutable *per-commit deltas*:
--   a commit writes a set of paths; reads/snapshots resolve by walking ancestors
--   to find the most recent version of each path, honoring tombstones.
--
-- Because the history remains linear, merges and rebases are implemented by
-- **replaying net file changes** onto a chosen base commit (or fast-forwarding
-- when possible). Merge commits record the merged source via
-- `merged_from_commit_id`.
--
-- ## Public API surface (stable)
--
-- - **Direct-write tables**: `fs.repositories`, `fs.branches`, `fs.commits`,
--   `fs.files`
-- - **Read helpers**:
--   - `fs.read_file(commit_id, path)` → `TEXT | NULL`
--   - `fs.get_commit_snapshot(commit_id, path_prefix?)` (resolved tree)
--   - `fs.get_commit_delta(commit_id)` (files written *in that commit only*)
--   - `fs.get_file_history(commit_id, path)` (commits that touched a path)
-- - **Merge/rebase helpers**:
--   - `fs.get_merge_base(left_commit_id, right_commit_id)`
--   - `fs.get_conflicts(left_commit_id, right_commit_id)`
-- - **Merge/rebase operations**:
--   - `fs.finalize_commit(commit_id, target_branch_id?)`
--   - `fs.rebase_branch(branch_id, onto_branch_id, message?)`
--
-- ## Internal helpers (unstable)
--
-- Functions prefixed with `fs._*` are internal implementation details. Tests
-- may exercise some of them, but they are not intended as the primary API.
--
-- ## Paths / symlinks / deletions
--
-- - All stored file paths are canonical **absolute** paths (always start with
--   `/`), use `/` separators (Windows `\` is accepted on input), collapse `//`,
--   and remove trailing slashes (except `/` itself).
-- - Paths are validated for cross-platform safety (Windows + Unix).
-- - A row with `is_symlink = TRUE` represents a symlink; `content` stores the
--   normalized absolute target path. `fs.read_file` returns the stored target
--   path; it does **not** dereference symlinks.
-- - A row with `is_deleted = TRUE` is a tombstone. A tombstoned path resolves to
--   "missing" in snapshots and `fs.read_file` returns `NULL`.
-- ========================================
-- SCHEMA
-- ========================================
CREATE SCHEMA IF NOT EXISTS fs;
-- ========================================
-- EXTENSIONS
-- ========================================
-- This project aims to avoid requiring non-core extensions.
-- If your runtime does not provide `gen_random_uuid()`, you can either:
-- - enable an appropriate extension (e.g. `pgcrypto`) in your environment, or
-- - replace the DEFAULTs with a UUID generator available in your runtime.
-- ========================================
-- TABLE DEFINITIONS
-- ========================================
/*
 fs.repositories
 ---------------
 Represents a repository (a virtual drive / project root).
 
 Key ideas:
 - A repository always has a "default branch" (`default_branch_id`).
 - On repository creation, we automatically create the default `main` branch
 (with `head_commit_id = NULL` initially) and set `default_branch_id`.
 
 Columns:
 - id (uuid): Repository identifier (default: gen_random_uuid()).
 - name (text): Human-readable unique name.
 - default_branch_id (uuid, nullable): Points to the default branch row.
 - created_at (timestamptz): Creation timestamp (default: now()).
 */
CREATE TABLE fs.repositories (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL UNIQUE,
  default_branch_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
/*
 fs.commits
 ----------
 Represents a commit node in a repository's commit graph.
 
 Invariants:
 - Each commit belongs to exactly one repository (`repository_id`).
 - Each commit has **at most one parent** (`parent_commit_id`).
 - Only the first/root commit per repository may have `parent_commit_id IS NULL`.
 - Parents must be in the **same repository** (enforced by a composite FK).
 
 Notes:
 - Commits are intended to be append-only. The system assumes commit rows are not
 updated in-place.
 
 Columns:
 - id (uuid): Commit identifier.
 - repository_id (uuid): Owning repository (FK → fs.repositories).
 - parent_commit_id (uuid, nullable): Parent commit in the same repository.
 - message (text): Commit message.
 - created_at (timestamptz): Creation timestamp.
 
 Constraints:
 - commits_id_repository_id_unique:
 Provides a `(id, repository_id)` target for composite foreign keys.
 - commits_parent_same_repo_fk:
 Enforces that `(parent_commit_id, repository_id)` references a commit in the
 same repository.
 */
CREATE TABLE fs.commits (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  repository_id UUID NOT NULL REFERENCES fs.repositories(id) ON DELETE CASCADE,
  parent_commit_id UUID,
  -- Optional pointer to the "other" side of a merge. This keeps history linear
  -- while still remembering which commit was merged.
  merged_from_commit_id UUID,
  message TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT commits_id_repository_id_unique UNIQUE (id, repository_id),
  CONSTRAINT commits_parent_same_repo_fk FOREIGN KEY (parent_commit_id, repository_id) REFERENCES fs.commits (id, repository_id) ON DELETE CASCADE,
  CONSTRAINT commits_merged_from_same_repo_fk FOREIGN KEY (merged_from_commit_id, repository_id) REFERENCES fs.commits (id, repository_id) ON DELETE CASCADE
);
/*
 fs.branches
 -----------
 Represents a named branch pointer within a repository.
 
 Branches are lightweight; they track a single `head_commit_id` representing the
 "current" commit for that branch.
 
 Key ideas:
 - New repositories start with a `main` branch whose `head_commit_id` is `NULL`
 until the first commit is created.
 - When creating a new branch, if `head_commit_id` is omitted, we default it to
 the repository's default branch head (when resolvable).
 
 Columns:
 - id (uuid): Branch identifier.
 - repository_id (uuid): Owning repository.
 - name (text): Branch name, unique per repository.
 - head_commit_id (uuid, nullable): Head commit for this branch. Nullable to
 support empty repositories and empty branches.
 - created_at (timestamptz): Creation timestamp.
 
 Constraints:
 - UNIQUE(repository_id, name): One branch with a given name per repo.
 - branches_head_commit_same_repo_fk:
 Enforces that the head commit belongs to the same repository.
 */
CREATE TABLE fs.branches (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  repository_id UUID NOT NULL REFERENCES fs.repositories(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  -- Nullable: new repositories start with an empty default branch until the first commit is created.
  head_commit_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(repository_id, name),
  -- Each repo can have only one branch with a given name
  CONSTRAINT branches_id_repository_id_unique UNIQUE (id, repository_id),
  CONSTRAINT branches_head_commit_same_repo_fk FOREIGN KEY (head_commit_id, repository_id) REFERENCES fs.commits (id, repository_id)
);
/*
 fs.files
 --------
 Stores file writes (and deletions) that occur in a given commit.
 
 This table is the fundamental "content store":
 - Each commit writes a *delta* (not a full snapshot).
 - Reads and snapshots resolve by walking commit ancestry and selecting the
 newest entry for each path.
 
 File states:
 - Normal file: `is_deleted = FALSE`, `is_symlink = FALSE`, `content` is file text.
 - Tombstone delete: `is_deleted = TRUE` (path is deleted at that commit).
 - Symlink: `is_symlink = TRUE` and `content` is the normalized absolute target.
 
 Important behavior (enforced by triggers):
 - `path` is always stored in canonical form via `fs._normalize_path`.
 - When `is_symlink = TRUE`, `content` is normalized as a path (symlink target).
 - When `is_deleted = TRUE`, we force `is_symlink = FALSE` and `content = ''`.
 
 Columns:
 - id (uuid): File row identifier.
 - commit_id (uuid): Commit containing this file delta (FK → fs.commits).
 - path (text): Canonical absolute path.
 - content (text): File content OR (when symlink) normalized target path.
 - is_deleted (boolean): Tombstone flag.
 - is_symlink (boolean): Symlink flag.
 - created_at (timestamptz): Insert timestamp.
 
 Constraints:
 - UNIQUE(commit_id, path): At most one write per path per commit.
 */
CREATE TABLE fs.files (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  commit_id UUID NOT NULL REFERENCES fs.commits(id) ON DELETE CASCADE,
  path TEXT NOT NULL,
  content TEXT NOT NULL,
  is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
  is_symlink BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(commit_id, path)
);
/*
 Repository ↔ default branch relationship
 --------------------------------------
 We want `fs.repositories.default_branch_id` to reference a branch **in the same
 repository**. We achieve this by:
 - giving `fs.branches` a composite uniqueness target `(id, repository_id)`, and
 - referencing it from repositories using `(default_branch_id, id)`.
 */
ALTER TABLE fs.repositories
ADD CONSTRAINT repositories_default_branch_same_repo_fk FOREIGN KEY (default_branch_id, id) REFERENCES fs.branches (id, repository_id) ON DELETE
SET NULL;
/*
 Common usage patterns (examples)
 --------------------------------
 
 Note: This system intentionally keeps the core tables **direct-write**. That
 means callers typically do two things explicitly:
 - Insert commits/files into the `fs.*` tables, and
 - Move branch heads forward via `UPDATE fs.branches ...`.
 
 Create a repository (auto-creates `main` branch; it starts empty):
 
 INSERT INTO fs.repositories (name) VALUES ('my-repo') RETURNING id, default_branch_id;
 
 Create the first/root commit (parent is NULL by definition):
 
 INSERT INTO fs.commits (repository_id, message, parent_commit_id)
 VALUES ($repo_id, 'Initial commit', NULL)
 RETURNING id;
 
 Advance the branch head to the new commit:
 
 UPDATE fs.branches
 SET head_commit_id = $commit_id
 WHERE id = (SELECT default_branch_id FROM fs.repositories WHERE id = $repo_id);
 
 Write files into the commit (paths are normalized on insert):
 
 INSERT INTO fs.files (commit_id, path, content)
 VALUES ($commit_id, 'src/index.ts', 'console.log(\"hi\")');
 
 Read a file at a commit (resolves through ancestors):
 
 SELECT fs.read_file($commit_id, '/src/index.ts');
 
 Browse content:
 
 -- Files written in a specific commit (delta)
 SELECT path, is_deleted, is_symlink FROM fs.get_commit_delta($commit_id);
 
 -- Fully resolved tree at a commit (snapshot)
 SELECT path, is_symlink FROM fs.get_commit_snapshot($commit_id, '/src/');
 
 Merge / rebase (linear history):
 
 -- Merge source into target:
 -- 1) INSERT INTO fs.commits (repository_id, message, parent_commit_id, merged_from_commit_id) ...
 -- 2) INSERT INTO fs.files (...) for any conflicting paths you want to resolve manually.
 -- 3) SELECT * FROM fs.finalize_commit($merge_commit_id, $target_branch_id);
 
 -- Rebase branch onto another branch head (may fast-forward, noop, or create a single replay commit)
 SELECT * FROM fs.rebase_branch($branch_id, $onto_branch_id, 'Rebase message');
 */
-- ========================================
-- PATHS (VALIDATION + NORMALIZATION)
-- ========================================
/*
 fs._validate_path(p_path text) -> void
 -------------------------------------
 Internal validation routine shared by path normalization and write triggers.
 
 Validation goals:
 - Prevent malformed / dangerous paths (null/empty, too long, control chars).
 - Ensure cross-platform filesystem compatibility:
 - Reject Windows-invalid characters: < > : " | ? *
 - Reject NUL bytes and other control characters (except tab/newline/CR).
 
 Notes:
 - We do not attempt to interpret `..` components; this system treats paths as
 opaque strings after normalization. (If you want to prevent traversal-like
 semantics, enforce that at a higher layer.)
 */
CREATE OR REPLACE FUNCTION fs._validate_path(p_path TEXT) RETURNS VOID AS $$
DECLARE char_code INT;
BEGIN -- Check for null or empty path
IF p_path IS NULL
OR LENGTH(TRIM(p_path)) = 0 THEN RAISE EXCEPTION 'Path cannot be null or empty';
END IF;
-- Check for very long paths (over 4096 characters)
IF LENGTH(p_path) > 4096 THEN RAISE EXCEPTION 'Path is too long (maximum 4096 characters)';
END IF;
-- Check for control characters (invalid on Windows, problematic on Unix)
-- Allow tab (\x09), newline (\x0A), carriage return (\x0D)
FOR i IN 1..LENGTH(p_path) LOOP char_code := ASCII(
  SUBSTRING(
    p_path
    FROM i FOR 1
  )
);
IF char_code < 32
AND char_code NOT IN (9, 10, 13) THEN RAISE EXCEPTION 'Path contains control characters (0x%x)',
LPAD(UPPER(TO_HEX(char_code)), 2, '0');
END IF;
END LOOP;
-- Check for characters invalid on Windows: < > : " | ? *
-- Note: / and \ are allowed as path separators
IF p_path ~ '[<>"|?*:]' THEN RAISE EXCEPTION 'Path contains characters invalid on Windows: < > : " | ? *';
END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
/*
 fs.rebase_branch(p_branch_id uuid, p_onto_branch_id uuid, p_message text default NULL)
 -> TABLE(operation text, repository_id uuid, branch_id uuid, onto_branch_id uuid,
 merge_base_commit_id uuid, previous_branch_head_commit_id uuid,
 onto_head_commit_id uuid, rebased_commit_id uuid,
 new_branch_head_commit_id uuid, applied_file_count int)
 ------------------------------------------------------------------------------------
 Performs a **squash rebase** of one branch onto another branch's head.
 
 Why "squash":
 - This system stores a single-parent commit graph, so we cannot replay per-commit
 changes while preserving a multi-parent merge history.
 - Instead, we compute the branch's **net effect since the merge base** and apply
 that patch as a single new commit on top of `onto`.
 
 Algorithm (conceptual):
 - Resolve `branch_head` and `onto_head` from the provided branch ids.
 - Compute `merge_base := fs.get_merge_base(branch_head, onto_head)`.
 
 Cases:
 - `p_branch_id = p_onto_branch_id` → `operation = 'noop'`.
 - `merge_base = onto_head` → `operation = 'already_up_to_date'`
 (the onto head is already contained in the branch history).
 - `merge_base = branch_head` → `operation = 'fast_forward'`
 (the branch is behind; we can move the branch head to `onto_head`).
 - Otherwise:
 - If `fs.get_conflicts(branch_head, onto_head)` returns any rows, the rebase
 is blocked and this function raises an exception.
 - Create a new commit whose parent is `onto_head`.
 - Apply the minimal patch that transforms the `onto_head` snapshot into:
 "onto snapshot + branch's net changes since merge_base".
 - Update the branch head to the new commit and return `operation = 'rebased'`.
 
 Return values:
 - `rebased_commit_id` is `NULL` for non-creating operations (`noop`,
 `already_up_to_date`, `fast_forward`).
 - `applied_file_count` is the number of rows inserted into `fs.files` for the
 rebased commit (tombstones + writes). It is `0` when no new commit is created.
 
 Preconditions / notes:
 - Both branches must exist and belong to the same repository.
 - Branch heads must be non-NULL for diverged-history rebases (otherwise
 `fs.get_merge_base` will raise `commit_id must be specified`).
 */
CREATE OR REPLACE FUNCTION fs.rebase_branch(
    p_branch_id UUID,
    p_onto_branch_id UUID,
    p_message TEXT DEFAULT NULL
  ) RETURNS TABLE(
    operation TEXT,
    repository_id UUID,
    branch_id UUID,
    onto_branch_id UUID,
    merge_base_commit_id UUID,
    previous_branch_head_commit_id UUID,
    onto_head_commit_id UUID,
    rebased_commit_id UUID,
    new_branch_head_commit_id UUID,
    applied_file_count INT
  ) AS $$
DECLARE v_branch_repo_id UUID;
v_onto_repo_id UUID;
v_branch_head_commit_id UUID;
v_onto_head_commit_id UUID;
v_merge_base_commit_id UUID;
v_conflict_count INT := 0;
v_delete_count INT := 0;
v_write_count INT := 0;
v_rows INT := 0;
v_rebased_commit_id UUID;
v_message TEXT;
BEGIN IF p_branch_id IS NULL
OR p_onto_branch_id IS NULL THEN RAISE EXCEPTION 'branch_id must be specified';
END IF;
SELECT b.repository_id,
  b.head_commit_id INTO v_branch_repo_id,
  v_branch_head_commit_id
FROM fs.branches b
WHERE b.id = p_branch_id;
IF v_branch_repo_id IS NULL THEN RAISE EXCEPTION 'Invalid branch_id: branch does not exist';
END IF;
SELECT b.repository_id,
  b.head_commit_id INTO v_onto_repo_id,
  v_onto_head_commit_id
FROM fs.branches b
WHERE b.id = p_onto_branch_id;
IF v_onto_repo_id IS NULL THEN RAISE EXCEPTION 'Invalid onto_branch_id: branch does not exist';
END IF;
IF v_branch_repo_id <> v_onto_repo_id THEN RAISE EXCEPTION 'Branches must belong to the same repository';
END IF;
repository_id := v_branch_repo_id;
branch_id := p_branch_id;
onto_branch_id := p_onto_branch_id;
previous_branch_head_commit_id := v_branch_head_commit_id;
onto_head_commit_id := v_onto_head_commit_id;
IF p_branch_id = p_onto_branch_id THEN operation := 'noop';
merge_base_commit_id := v_branch_head_commit_id;
rebased_commit_id := NULL;
new_branch_head_commit_id := v_branch_head_commit_id;
applied_file_count := 0;
RETURN NEXT;
RETURN;
END IF;
v_merge_base_commit_id := fs.get_merge_base(v_branch_head_commit_id, v_onto_head_commit_id);
merge_base_commit_id := v_merge_base_commit_id;
-- If onto is already an ancestor of the branch, nothing to do.
IF v_merge_base_commit_id = v_onto_head_commit_id THEN operation := 'already_up_to_date';
rebased_commit_id := NULL;
new_branch_head_commit_id := v_branch_head_commit_id;
applied_file_count := 0;
RETURN NEXT;
RETURN;
END IF;
-- If branch is an ancestor of onto, fast-forward the branch.
IF v_merge_base_commit_id = v_branch_head_commit_id THEN
UPDATE fs.branches
SET head_commit_id = v_onto_head_commit_id
WHERE id = p_branch_id;
operation := 'fast_forward';
rebased_commit_id := NULL;
new_branch_head_commit_id := v_onto_head_commit_id;
applied_file_count := 0;
RETURN NEXT;
RETURN;
END IF;
-- Abort on conflicts.
SELECT COUNT(*)::INT INTO v_conflict_count
FROM fs.get_conflicts(v_branch_head_commit_id, v_onto_head_commit_id);
IF v_conflict_count > 0 THEN RAISE EXCEPTION 'Rebase blocked by % conflicts. Call fs.get_conflicts(%, %) to inspect.',
v_conflict_count,
v_branch_head_commit_id,
v_onto_head_commit_id;
END IF;
-- Compute the minimal patch to apply onto the onto head (no redundant writes).
WITH base_snapshot AS (
  SELECT s.path,
    s.content,
    s.is_symlink
  FROM fs._get_commit_snapshot_with_content(v_merge_base_commit_id) s
),
onto_snapshot AS (
  SELECT s.path,
    s.content,
    s.is_symlink
  FROM fs._get_commit_snapshot_with_content(v_onto_head_commit_id) s
),
branch_snapshot AS (
  SELECT s.path,
    s.content,
    s.is_symlink
  FROM fs._get_commit_snapshot_with_content(v_branch_head_commit_id) s
),
paths AS (
  SELECT b.path AS file_path
  FROM base_snapshot b
  UNION
  SELECT o.path AS file_path
  FROM onto_snapshot o
  UNION
  SELECT br.path AS file_path
  FROM branch_snapshot br
),
states AS (
  SELECT p.file_path,
    (b.path IS NOT NULL) AS base_exists,
    b.is_symlink AS base_is_symlink,
    b.content AS base_content,
    (o.path IS NOT NULL) AS onto_exists,
    o.is_symlink AS onto_is_symlink,
    o.content AS onto_content,
    (br.path IS NOT NULL) AS branch_exists,
    br.is_symlink AS branch_is_symlink,
    br.content AS branch_content
  FROM paths p
    LEFT JOIN base_snapshot b ON b.path = p.file_path
    LEFT JOIN onto_snapshot o ON o.path = p.file_path
    LEFT JOIN branch_snapshot br ON br.path = p.file_path
),
diffs AS (
  SELECT st.*,
    (
      st.branch_exists IS DISTINCT
      FROM st.base_exists
        OR st.branch_is_symlink IS DISTINCT
      FROM st.base_is_symlink
        OR st.branch_content IS DISTINCT
      FROM st.base_content
    ) AS branch_changed
  FROM states st
),
desired AS (
  SELECT d.file_path,
    CASE
      WHEN d.branch_changed THEN d.branch_exists
      ELSE d.onto_exists
    END AS desired_exists,
    CASE
      WHEN d.branch_changed THEN d.branch_is_symlink
      ELSE d.onto_is_symlink
    END AS desired_is_symlink,
    CASE
      WHEN d.branch_changed THEN d.branch_content
      ELSE d.onto_content
    END AS desired_content,
    d.onto_exists,
    d.onto_is_symlink,
    d.onto_content
  FROM diffs d
),
patch AS (
  SELECT de.file_path,
    de.desired_exists,
    de.desired_is_symlink,
    de.desired_content,
    (
      NOT de.desired_exists
      AND de.onto_exists
    ) AS need_delete,
    (
      de.desired_exists
      AND (
        (NOT de.onto_exists)
        OR de.onto_is_symlink IS DISTINCT
        FROM de.desired_is_symlink
          OR de.onto_content IS DISTINCT
        FROM de.desired_content
      )
    ) AS need_write
  FROM desired de
)
SELECT COALESCE(
    SUM(
      CASE
        WHEN p.need_delete THEN 1
        ELSE 0
      END
    ),
    0
  )::INT,
  COALESCE(
    SUM(
      CASE
        WHEN p.need_write THEN 1
        ELSE 0
      END
    ),
    0
  )::INT INTO v_delete_count,
  v_write_count
FROM patch p;
-- If rebasing would not change the onto snapshot, just move the branch to onto.
IF (v_delete_count + v_write_count) = 0 THEN
UPDATE fs.branches
SET head_commit_id = v_onto_head_commit_id
WHERE id = p_branch_id;
operation := 'fast_forward';
rebased_commit_id := NULL;
new_branch_head_commit_id := v_onto_head_commit_id;
applied_file_count := 0;
RETURN NEXT;
RETURN;
END IF;
v_message := COALESCE(p_message, 'Rebase');
INSERT INTO fs.commits (repository_id, parent_commit_id, message)
VALUES (
    v_branch_repo_id,
    v_onto_head_commit_id,
    v_message
  )
RETURNING id INTO v_rebased_commit_id;
-- Apply deletions
WITH base_snapshot AS (
  SELECT s.path,
    s.content,
    s.is_symlink
  FROM fs._get_commit_snapshot_with_content(v_merge_base_commit_id) s
),
onto_snapshot AS (
  SELECT s.path,
    s.content,
    s.is_symlink
  FROM fs._get_commit_snapshot_with_content(v_onto_head_commit_id) s
),
branch_snapshot AS (
  SELECT s.path,
    s.content,
    s.is_symlink
  FROM fs._get_commit_snapshot_with_content(v_branch_head_commit_id) s
),
paths AS (
  SELECT b.path AS file_path
  FROM base_snapshot b
  UNION
  SELECT o.path AS file_path
  FROM onto_snapshot o
  UNION
  SELECT br.path AS file_path
  FROM branch_snapshot br
),
states AS (
  SELECT p.file_path,
    (b.path IS NOT NULL) AS base_exists,
    b.is_symlink AS base_is_symlink,
    b.content AS base_content,
    (o.path IS NOT NULL) AS onto_exists,
    o.is_symlink AS onto_is_symlink,
    o.content AS onto_content,
    (br.path IS NOT NULL) AS branch_exists,
    br.is_symlink AS branch_is_symlink,
    br.content AS branch_content
  FROM paths p
    LEFT JOIN base_snapshot b ON b.path = p.file_path
    LEFT JOIN onto_snapshot o ON o.path = p.file_path
    LEFT JOIN branch_snapshot br ON br.path = p.file_path
),
diffs AS (
  SELECT st.*,
    (
      st.branch_exists IS DISTINCT
      FROM st.base_exists
        OR st.branch_is_symlink IS DISTINCT
      FROM st.base_is_symlink
        OR st.branch_content IS DISTINCT
      FROM st.base_content
    ) AS branch_changed
  FROM states st
),
desired AS (
  SELECT d.file_path,
    CASE
      WHEN d.branch_changed THEN d.branch_exists
      ELSE d.onto_exists
    END AS desired_exists,
    CASE
      WHEN d.branch_changed THEN d.branch_is_symlink
      ELSE d.onto_is_symlink
    END AS desired_is_symlink,
    CASE
      WHEN d.branch_changed THEN d.branch_content
      ELSE d.onto_content
    END AS desired_content,
    d.onto_exists,
    d.onto_is_symlink,
    d.onto_content
  FROM diffs d
),
patch AS (
  SELECT de.file_path,
    de.desired_exists,
    de.desired_is_symlink,
    de.desired_content,
    (
      NOT de.desired_exists
      AND de.onto_exists
    ) AS need_delete,
    (
      de.desired_exists
      AND (
        (NOT de.onto_exists)
        OR de.onto_is_symlink IS DISTINCT
        FROM de.desired_is_symlink
          OR de.onto_content IS DISTINCT
        FROM de.desired_content
      )
    ) AS need_write
  FROM desired de
)
INSERT INTO fs.files (commit_id, path, content, is_deleted, is_symlink)
SELECT v_rebased_commit_id,
  p.file_path,
  '',
  TRUE,
  FALSE
FROM patch p
WHERE p.need_delete;
GET DIAGNOSTICS v_rows = ROW_COUNT;
applied_file_count := v_rows;
-- Apply writes
WITH base_snapshot AS (
  SELECT s.path,
    s.content,
    s.is_symlink
  FROM fs._get_commit_snapshot_with_content(v_merge_base_commit_id) s
),
onto_snapshot AS (
  SELECT s.path,
    s.content,
    s.is_symlink
  FROM fs._get_commit_snapshot_with_content(v_onto_head_commit_id) s
),
branch_snapshot AS (
  SELECT s.path,
    s.content,
    s.is_symlink
  FROM fs._get_commit_snapshot_with_content(v_branch_head_commit_id) s
),
paths AS (
  SELECT b.path AS file_path
  FROM base_snapshot b
  UNION
  SELECT o.path AS file_path
  FROM onto_snapshot o
  UNION
  SELECT br.path AS file_path
  FROM branch_snapshot br
),
states AS (
  SELECT p.file_path,
    (b.path IS NOT NULL) AS base_exists,
    b.is_symlink AS base_is_symlink,
    b.content AS base_content,
    (o.path IS NOT NULL) AS onto_exists,
    o.is_symlink AS onto_is_symlink,
    o.content AS onto_content,
    (br.path IS NOT NULL) AS branch_exists,
    br.is_symlink AS branch_is_symlink,
    br.content AS branch_content
  FROM paths p
    LEFT JOIN base_snapshot b ON b.path = p.file_path
    LEFT JOIN onto_snapshot o ON o.path = p.file_path
    LEFT JOIN branch_snapshot br ON br.path = p.file_path
),
diffs AS (
  SELECT st.*,
    (
      st.branch_exists IS DISTINCT
      FROM st.base_exists
        OR st.branch_is_symlink IS DISTINCT
      FROM st.base_is_symlink
        OR st.branch_content IS DISTINCT
      FROM st.base_content
    ) AS branch_changed
  FROM states st
),
desired AS (
  SELECT d.file_path,
    CASE
      WHEN d.branch_changed THEN d.branch_exists
      ELSE d.onto_exists
    END AS desired_exists,
    CASE
      WHEN d.branch_changed THEN d.branch_is_symlink
      ELSE d.onto_is_symlink
    END AS desired_is_symlink,
    CASE
      WHEN d.branch_changed THEN d.branch_content
      ELSE d.onto_content
    END AS desired_content,
    d.onto_exists,
    d.onto_is_symlink,
    d.onto_content
  FROM diffs d
),
patch AS (
  SELECT de.file_path,
    de.desired_exists,
    de.desired_is_symlink,
    de.desired_content,
    (
      NOT de.desired_exists
      AND de.onto_exists
    ) AS need_delete,
    (
      de.desired_exists
      AND (
        (NOT de.onto_exists)
        OR de.onto_is_symlink IS DISTINCT
        FROM de.desired_is_symlink
          OR de.onto_content IS DISTINCT
        FROM de.desired_content
      )
    ) AS need_write
  FROM desired de
)
INSERT INTO fs.files (commit_id, path, content, is_deleted, is_symlink)
SELECT v_rebased_commit_id,
  p.file_path,
  p.desired_content,
  FALSE,
  COALESCE(p.desired_is_symlink, FALSE)
FROM patch p
WHERE p.need_write;
GET DIAGNOSTICS v_rows = ROW_COUNT;
applied_file_count := applied_file_count + v_rows;
UPDATE fs.branches
SET head_commit_id = v_rebased_commit_id
WHERE id = p_branch_id;
operation := 'rebased';
rebased_commit_id := v_rebased_commit_id;
new_branch_head_commit_id := v_rebased_commit_id;
RETURN NEXT;
END;
$$ LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE;
/*
 fs._normalize_path(p_path text) -> text
 --------------------------------------
 Internal helper that canonicalizes a file path for storage and lookup.
 
 Normalization rules:
 - Validate using `fs._validate_path`.
 - Accept Windows-style separators (`\`) on input but store canonical `/`.
 - Ensure the path is absolute (prefix `/` if missing).
 - Collapse duplicate slashes (`//` → `/`).
 - Remove trailing slash unless the path is exactly `/`.
 
 Examples:
 - `src/main.ts` → `/src/main.ts`
 - `//src//main.ts` → `/src/main.ts`
 - `/src/main.ts/` → `/src/main.ts`
 - `target.txt` → `/target.txt`
 
 This function is used by write triggers and read helpers so callers may use a
 variety of input styles while the database stores a single canonical form.
 */
CREATE OR REPLACE FUNCTION fs._normalize_path(p_path TEXT) RETURNS TEXT AS $$
DECLARE normalized_path TEXT;
BEGIN -- First validate the path
PERFORM fs._validate_path(p_path);
-- Normalize path separators (accept Windows-style "\" input, store canonical "/" paths)
normalized_path := REPLACE(p_path, E'\\', '/');
-- Ensure path starts with /
normalized_path := CASE
  WHEN normalized_path LIKE '/%' THEN normalized_path
  ELSE '/' || normalized_path
END;
-- Remove duplicate slashes
WHILE normalized_path LIKE '%//%' LOOP normalized_path := REPLACE(normalized_path, '//', '/');
END LOOP;
-- Remove trailing slash unless it's just "/"
IF LENGTH(normalized_path) > 1
AND normalized_path LIKE '%/' THEN normalized_path := LEFT(normalized_path, LENGTH(normalized_path) - 1);
END IF;
-- Ensure the normalized output stays within our max length budget
IF LENGTH(normalized_path) > 4096 THEN RAISE EXCEPTION 'Path is too long (maximum 4096 characters)';
END IF;
RETURN normalized_path;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
/*
 fs._normalize_path_prefix(p_path_prefix text) -> text
 ----------------------------------------------------
 Internal helper to normalize prefix strings used for prefix searches (e.g.
 `fs.get_commit_snapshot(commit_id, '/src/')`).
 
 Why this differs from `fs._normalize_path`:
 - For prefix searches we preserve the meaning of an explicit trailing separator.
 For example, an intent of `/src/` usually means "paths under src/", not "paths
 starting with /src" (which would also match `/src-old/...`).
 
 Rules:
 - Validate using `fs._validate_path`.
 - Accept `\` on input but store canonical `/`.
 - Ensure the prefix is absolute (prefix `/` if missing).
 - Collapse duplicate slashes.
 - If the input ended with `/` or `\`, preserve a trailing `/` in the normalized
 output (except for `/` itself).
 */
CREATE OR REPLACE FUNCTION fs._normalize_path_prefix(p_path_prefix TEXT) RETURNS TEXT AS $$
DECLARE normalized_prefix TEXT;
DECLARE has_trailing_slash BOOLEAN;
BEGIN PERFORM fs._validate_path(p_path_prefix);
has_trailing_slash := RIGHT(p_path_prefix, 1) = '/'
OR RIGHT(p_path_prefix, 1) = E'\\';
normalized_prefix := REPLACE(p_path_prefix, E'\\', '/');
-- Ensure prefix starts with /
normalized_prefix := CASE
  WHEN normalized_prefix LIKE '/%' THEN normalized_prefix
  ELSE '/' || normalized_prefix
END;
-- Remove duplicate slashes
WHILE normalized_prefix LIKE '%//%' LOOP normalized_prefix := REPLACE(normalized_prefix, '//', '/');
END LOOP;
-- Preserve explicit trailing slash (directory-style prefix matching)
IF has_trailing_slash
AND normalized_prefix <> '/'
AND RIGHT(normalized_prefix, 1) <> '/' THEN normalized_prefix := normalized_prefix || '/';
END IF;
IF LENGTH(normalized_prefix) > 4096 THEN RAISE EXCEPTION 'Path is too long (maximum 4096 characters)';
END IF;
RETURN normalized_prefix;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
-- ========================================
-- WRITE-TIME HELPERS (TRIGGER FUNCTIONS)
-- ========================================
/*
 fs._commits_before_insert_trigger() -> trigger
 ---------------------------------------------
 BEFORE INSERT trigger on `fs.commits`.
 
 Primary responsibilities:
 - If `NEW.parent_commit_id` is omitted/NULL, default it to the repository's
 default branch head (when available).
 - This keeps the commit graph well-formed while still allowing the first/root
 commit of a repository to have a NULL parent.
 - Provide clearer error messages than raw FK failures:
 - If commits already exist but we cannot resolve a default parent, require an
 explicit `parent_commit_id`.
 - If a parent is provided, ensure it belongs to the same repository.
 
 Important: This trigger does **not** advance any branch heads. Branch heads are
 explicit pointers and should be updated intentionally by the caller.
 */
CREATE OR REPLACE FUNCTION fs._commits_before_insert_trigger() RETURNS TRIGGER AS $$
DECLARE resolved_parent_commit_id UUID;
BEGIN -- Validate repository exists (FK would catch this too, but keep a clearer error message).
IF NOT EXISTS (
  SELECT 1
  FROM fs.repositories
  WHERE id = NEW.repository_id
) THEN RAISE EXCEPTION 'Invalid repository_id: repository does not exist';
END IF;
IF NEW.parent_commit_id IS NULL THEN
SELECT b.head_commit_id INTO resolved_parent_commit_id
FROM fs.repositories r
  JOIN fs.branches b ON b.id = r.default_branch_id
WHERE r.id = NEW.repository_id;
IF resolved_parent_commit_id IS NOT NULL THEN NEW.parent_commit_id := resolved_parent_commit_id;
ELSE -- If commits already exist, we should have been able to resolve a parent.
IF EXISTS (
  SELECT 1
  FROM fs.commits
  WHERE repository_id = NEW.repository_id
) THEN RAISE EXCEPTION 'parent_commit_id must be specified (repository default branch head could not be resolved)';
END IF;
END IF;
END IF;
-- Enforce that parent_commit_id (if provided) belongs to the same repository.
-- (Redundant with commits_parent_same_repo_fk, but yields a nicer error.)
IF NEW.parent_commit_id IS NOT NULL
AND NOT EXISTS (
  SELECT 1
  FROM fs.commits
  WHERE id = NEW.parent_commit_id
    AND repository_id = NEW.repository_id
) THEN RAISE EXCEPTION 'Invalid parent_commit_id: must reference a commit in the same repository';
END IF;
RETURN NEW;
END;
$$ LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE;
-- ========================================
-- TRIGGERS
-- ========================================
/*
 fs._files_before_insert_trigger() -> trigger
 -------------------------------------------
 BEFORE INSERT trigger on `fs.files`.
 
 Responsibilities:
 - Normalize `NEW.path` to the canonical absolute form (via `fs._normalize_path`).
 - Default flags (`is_deleted`, `is_symlink`) to FALSE when NULL is supplied.
 - Enforce file-state invariants:
 - If `is_deleted = TRUE`: force `is_symlink = FALSE` and coalesce content to ''.
 - If `is_deleted = FALSE`: require non-NULL content.
 - If `is_symlink = TRUE`: normalize `content` as an absolute path (the target).
 */
CREATE OR REPLACE FUNCTION fs._files_before_insert_trigger() RETURNS TRIGGER AS $$ BEGIN -- Enforce canonical paths even if someone inserts directly into fs.files
  NEW.path := fs._normalize_path(NEW.path);
NEW.is_deleted := COALESCE(NEW.is_deleted, FALSE);
NEW.is_symlink := COALESCE(NEW.is_symlink, FALSE);
IF NEW.is_deleted THEN NEW.is_symlink := FALSE;
NEW.content := COALESCE(NEW.content, '');
ELSE IF NEW.content IS NULL THEN RAISE EXCEPTION 'content must be specified when inserting a non-deleted file';
END IF;
IF NEW.is_symlink THEN NEW.content := fs._normalize_path(NEW.content);
END IF;
END IF;
RETURN NEW;
END;
$$ LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE;
/*
 fs._repositories_after_insert() -> trigger
 -----------------------------------------
 AFTER INSERT trigger on `fs.repositories`.
 
 Creates the default `main` branch for the new repository and records it in
 `fs.repositories.default_branch_id`.
 
 The created branch starts with `head_commit_id = NULL` so that the first/root
 commit can be created with `parent_commit_id = NULL`.
 */
CREATE OR REPLACE FUNCTION fs._repositories_after_insert() RETURNS TRIGGER AS $$
DECLARE branch_id UUID;
BEGIN -- Create the default branch with no head yet. The first commit can be created with parent_commit_id = NULL.
INSERT INTO fs.branches (repository_id, name, head_commit_id)
VALUES (NEW.id, 'main', NULL)
RETURNING id INTO branch_id;
-- Set the default branch on the repository
UPDATE fs.repositories
SET default_branch_id = branch_id
WHERE id = NEW.id;
RETURN NEW;
END;
$$ LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE;
/*
 fs._branches_before_insert() -> trigger
 --------------------------------------
 BEFORE INSERT trigger on `fs.branches`.
 
 If `NEW.head_commit_id` is omitted/NULL:
 - Default it to the repository default branch head (when resolvable).
 - If the repository already has commits but the default branch head cannot be
 resolved (e.g. the default branch still has a NULL head), require an explicit
 `head_commit_id`.
 - Otherwise allow NULL (empty branch in a repository that has no commits yet).
 */
CREATE OR REPLACE FUNCTION fs._branches_before_insert() RETURNS TRIGGER AS $$
DECLARE resolved_head_commit_id UUID;
BEGIN IF NEW.head_commit_id IS NULL THEN
SELECT b.head_commit_id INTO resolved_head_commit_id
FROM fs.repositories r
  JOIN fs.branches b ON b.id = r.default_branch_id
WHERE r.id = NEW.repository_id;
IF resolved_head_commit_id IS NULL THEN -- If the repository already has commits, we should be able to default to something. Require explicit head_commit_id.
IF EXISTS (
  SELECT 1
  FROM fs.commits
  WHERE repository_id = NEW.repository_id
) THEN RAISE EXCEPTION 'head_commit_id must be specified when creating a branch';
END IF;
-- Otherwise allow NULL head_commit_id (empty branch before first commit).
RETURN NEW;
END IF;
NEW.head_commit_id := resolved_head_commit_id;
END IF;
RETURN NEW;
END;
$$ LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE;
/*
 Trigger wiring
 --------------
 We use triggers to enforce canonical storage and provide convenience defaults,
 while keeping the direct-write table API intact.
 */
-- Create trigger on fs.commits table
CREATE TRIGGER commits_before_insert_trigger BEFORE
INSERT ON fs.commits FOR EACH ROW EXECUTE FUNCTION fs._commits_before_insert_trigger();
-- Create trigger on fs.files table
CREATE TRIGGER files_before_insert_trigger BEFORE
INSERT ON fs.files FOR EACH ROW EXECUTE FUNCTION fs._files_before_insert_trigger();
-- Create trigger on fs.repositories table
CREATE TRIGGER repositories_after_insert_trigger
AFTER
INSERT ON fs.repositories FOR EACH ROW EXECUTE FUNCTION fs._repositories_after_insert();
-- Create trigger on branches table
CREATE TRIGGER branches_before_insert_trigger BEFORE
INSERT ON fs.branches FOR EACH ROW EXECUTE FUNCTION fs._branches_before_insert();
-- ========================================
-- INDEXES
-- ========================================
/*
 Indexes
 -------
 These indexes support common traversals and invariants:
 - (repository_id, parent_commit_id): accelerates ancestor walking and merge-base
 queries.
 - (repository_id, merged_from_commit_id): accelerates merge-base lookups across
 linear merge commits.
 - "one root per repo": guarantees only one commit per repository can have a NULL
 parent.
 */
CREATE INDEX idx_commits_repository_parent ON fs.commits(repository_id, parent_commit_id);
CREATE INDEX idx_commits_repository_merged_from ON fs.commits(repository_id, merged_from_commit_id);
-- Only the first/root commit in a repository may have a NULL parent
CREATE UNIQUE INDEX commits_one_root_per_repo_idx ON fs.commits(repository_id)
WHERE parent_commit_id IS NULL;
-- ========================================
-- READ HELPERS
-- ========================================
/*
 fs.get_commit_delta(p_commit_id uuid) -> TABLE(...)
 --------------------------------------------------
 Returns the **commit delta**: all file rows written *in that commit only*,
 joined with repository and commit metadata.
 
 This does **not** resolve ancestors. It is primarily useful for:
 - auditing what a commit changed (including deletions and symlinks),
 - computing diffs at the commit layer.
 
 Notes:
 - If a commit wrote no files, this returns 0 rows.
 - File content is intentionally omitted from the returned table shape; use
 `fs.read_file(commit_id, path)` when you need content.
 */
CREATE OR REPLACE FUNCTION fs.get_commit_delta(p_commit_id UUID) RETURNS TABLE(
    repository_id UUID,
    repository_name TEXT,
    commit_id UUID,
    path TEXT,
    is_deleted BOOLEAN,
    is_symlink BOOLEAN,
    file_created_at TIMESTAMPTZ,
    commit_created_at TIMESTAMPTZ,
    commit_message TEXT
  ) AS $$ BEGIN RETURN QUERY
SELECT r.id as repository_id,
  r.name as repository_name,
  c.id as commit_id,
  f.path,
  f.is_deleted,
  f.is_symlink,
  f.created_at as file_created_at,
  c.created_at as commit_created_at,
  c.message as commit_message
FROM fs.commits c
  JOIN fs.repositories r ON c.repository_id = r.id
  JOIN fs.files f ON c.id = f.commit_id
WHERE c.id = p_commit_id;
END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE;
/*
 fs.get_commit_snapshot(p_commit_id uuid, p_path_prefix text default NULL) -> TABLE(...)
 -------------------------------------------------------------------------------------
 Public snapshot helper that returns the resolved file tree at a commit **without**
 file content.
 
 Rationale:
 - Snapshots are often used for browsing/listing; omitting content keeps result
 sets smaller and avoids accidentally fetching large blobs.
 - Use `fs.read_file(commit_id, path)` to fetch content for specific paths.
 
 Implementation detail:
 - This function intentionally avoids materializing file `content` so that callers
 can browse large repositories cheaply.
 */
CREATE OR REPLACE FUNCTION fs.get_commit_snapshot(
    p_commit_id UUID,
    p_path_prefix TEXT DEFAULT NULL
  ) RETURNS TABLE(
    repository_id UUID,
    repository_name TEXT,
    commit_id UUID,
    path TEXT,
    is_symlink BOOLEAN,
    commit_created_at TIMESTAMPTZ,
    commit_message TEXT
  ) AS $$
DECLARE normalized_prefix TEXT := NULL;
BEGIN IF p_commit_id IS NULL THEN RAISE EXCEPTION 'commit_id must be specified';
END IF;
IF NOT EXISTS (
  SELECT 1
  FROM fs.commits
  WHERE id = p_commit_id
) THEN RAISE EXCEPTION 'Invalid commit_id: commit does not exist';
END IF;
IF p_path_prefix IS NOT NULL THEN normalized_prefix := fs._normalize_path_prefix(p_path_prefix);
END IF;
RETURN QUERY WITH RECURSIVE commit_tree AS (
  -- Start with the given commit
  SELECT id,
    parent_commit_id,
    0 as depth
  FROM fs.commits
  WHERE id = p_commit_id
  UNION ALL
  -- Recursively add parent commits
  SELECT c.id,
    c.parent_commit_id,
    ct.depth + 1
  FROM fs.commits c
    INNER JOIN commit_tree ct ON c.id = ct.parent_commit_id
),
all_files AS (
  -- Get all files from commits in the tree, preferring newer versions.
  -- Note: we do NOT select `content` here. This prevents materializing large
  -- blobs when callers only need to browse paths/metadata.
  SELECT f.path,
    f.is_deleted,
    f.is_symlink,
    ROW_NUMBER() OVER (
      PARTITION BY f.path
      ORDER BY ct.depth ASC
    ) as rn
  FROM commit_tree ct
    JOIN fs.files f ON ct.id = f.commit_id
  WHERE (
      normalized_prefix IS NULL
      OR starts_with(f.path, normalized_prefix)
    )
),
snapshot_files AS (
  SELECT af.path,
    af.is_symlink
  FROM all_files af
  WHERE af.rn = 1
    AND NOT af.is_deleted
)
SELECT r.id as repository_id,
  r.name as repository_name,
  c.id as commit_id,
  sf.path,
  sf.is_symlink,
  c.created_at as commit_created_at,
  c.message as commit_message
FROM fs.commits c
  JOIN fs.repositories r ON c.repository_id = r.id
  JOIN snapshot_files sf ON TRUE
WHERE c.id = p_commit_id
ORDER BY sf.path;
END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE;
/*
 fs._get_commit_snapshot_with_content(p_commit_id uuid, p_path_prefix text default NULL)
 -> TABLE(repository_id uuid, repository_name text, commit_id uuid,
 path text, content text, is_symlink boolean,
 commit_created_at timestamptz, commit_message text)
 -------------------------------------------------------------------------------------
 Internal helper that returns the resolved file snapshot at a commit, including
 file `content`.
 
 Implementation detail:
 - We first compute the resolved snapshot *without* content via `fs.get_commit_snapshot`.
 - We then populate `content` via `fs.read_file(commit_id, path)` per returned path.
 
 This avoids materializing all historical file contents during snapshot resolution
 (which can be very large). The trade-off is that it may perform more work per
 path because `fs.read_file` resolves by walking ancestry.
 
 Notes:
 - Symlinks are not dereferenced; `content` for a symlink is the stored target.
 - This function is used by conflict detection, merges, and rebases where content
 comparisons are required.
 */
CREATE OR REPLACE FUNCTION fs._get_commit_snapshot_with_content(
    p_commit_id UUID,
    p_path_prefix TEXT DEFAULT NULL
  ) RETURNS TABLE(
    repository_id UUID,
    repository_name TEXT,
    commit_id UUID,
    path TEXT,
    content TEXT,
    is_symlink BOOLEAN,
    commit_created_at TIMESTAMPTZ,
    commit_message TEXT
  ) AS $$ BEGIN RETURN QUERY
SELECT s.repository_id,
  s.repository_name,
  s.commit_id,
  s.path,
  fs.read_file(s.commit_id, s.path) AS content,
  s.is_symlink,
  s.commit_created_at,
  s.commit_message
FROM fs.get_commit_snapshot(p_commit_id, p_path_prefix) s
ORDER BY s.path;
END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE;
-- ========================================
-- ESSENTIAL FUNCTIONS
-- ========================================
/*
 fs.read_file(p_commit_id uuid, p_file_path text) -> text | NULL
 --------------------------------------------------------------
 Reads a file as of a given commit by resolving through the commit's ancestry.
 
 Resolution rules:
 - Normalize `p_file_path` using `fs._normalize_path`.
 - Starting at `p_commit_id`, walk `parent_commit_id` pointers upward until a row
 is found for the normalized path.
 - If the first row found is a tombstone (`is_deleted = TRUE`), return `NULL`.
 - Otherwise return the stored `content`.
 
 Notes:
 - Symlinks are not dereferenced. For `is_symlink = TRUE`, `content` is the
 normalized absolute symlink target path and is returned as-is.
 - This function includes a large traversal step limit as a safety guard in case
 of an unexpected commit-parent cycle.
 */
CREATE OR REPLACE FUNCTION fs.read_file(p_commit_id UUID, p_file_path TEXT) RETURNS TEXT AS $$
DECLARE normalized_path TEXT;
DECLARE result_content TEXT := NULL;
DECLARE result_is_deleted BOOLEAN := FALSE;
DECLARE current_commit UUID := p_commit_id;
DECLARE step_count INT := 0;
BEGIN -- Walk up the commit tree to find the file
IF p_commit_id IS NULL THEN RAISE EXCEPTION 'commit_id must be specified';
END IF;
IF NOT EXISTS (
  SELECT 1
  FROM fs.commits
  WHERE id = p_commit_id
) THEN RAISE EXCEPTION 'Invalid commit_id: commit does not exist';
END IF;
normalized_path := fs._normalize_path(p_file_path);
WHILE current_commit IS NOT NULL LOOP step_count := step_count + 1;
IF step_count > 100000 THEN RAISE EXCEPTION 'Commit history traversal exceeded % steps (cycle?)',
step_count;
END IF;
SELECT content,
  is_deleted INTO result_content,
  result_is_deleted
FROM fs.files
WHERE commit_id = current_commit
  AND path = normalized_path;
-- If found, return it (tombstones delete the file)
IF FOUND THEN IF result_is_deleted THEN RETURN NULL;
END IF;
RETURN result_content;
END IF;
-- Move to parent commit
SELECT parent_commit_id INTO current_commit
FROM fs.commits
WHERE id = current_commit;
END LOOP;
-- File not found in any ancestor commit
RETURN NULL;
END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE;
/*
 fs.get_file_history(p_commit_id uuid, p_file_path text)
 -> TABLE(commit_id uuid, content text | NULL, is_deleted boolean, is_symlink boolean)
 --------------------------------------------------------------------------------------
 Returns the history of a single path by walking from `p_commit_id` up through parents
 and returning the commits that have an explicit row for that path.
 
 Meaning:
 - Each returned row corresponds to a commit that *touched* the path (write, delete,
 or symlink change).
 - If the row is a tombstone, `content` is returned as NULL.
 
 Ordering:
 - The function does not guarantee output ordering. Callers should add an `ORDER BY`
 clause (e.g. join `fs.commits` to order by commit time) based on their needs.
 */
CREATE OR REPLACE FUNCTION fs.get_file_history(p_commit_id UUID, p_file_path TEXT) RETURNS TABLE(
    commit_id UUID,
    content TEXT,
    is_deleted BOOLEAN,
    is_symlink BOOLEAN
  ) AS $$
DECLARE normalized_path TEXT;
BEGIN IF p_commit_id IS NULL THEN RAISE EXCEPTION 'commit_id must be specified';
END IF;
IF NOT EXISTS (
  SELECT 1
  FROM fs.commits
  WHERE id = p_commit_id
) THEN RAISE EXCEPTION 'Invalid commit_id: commit does not exist';
END IF;
normalized_path := fs._normalize_path(p_file_path);
RETURN QUERY WITH RECURSIVE commit_tree AS (
  -- Start with the given commit
  SELECT id,
    parent_commit_id,
    created_at
  FROM fs.commits
  WHERE id = p_commit_id
  UNION ALL
  -- Recursively add parent commits
  SELECT c.id,
    c.parent_commit_id,
    c.created_at
  FROM fs.commits c
    INNER JOIN commit_tree ct ON c.id = ct.parent_commit_id
)
SELECT ct.id,
  CASE
    WHEN f.is_deleted THEN NULL
    ELSE f.content
  END as content,
  f.is_deleted,
  f.is_symlink
FROM commit_tree ct
  LEFT JOIN fs.files f ON ct.id = f.commit_id
  AND f.path = normalized_path
WHERE f.id IS NOT NULL;
-- Only include commits that have this file
END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE;
-- ========================================
-- MERGE / REBASE HELPERS (CONFLICT DETECTION)
-- ========================================
/*
 fs.get_merge_base(p_left_commit_id uuid, p_right_commit_id uuid) -> uuid
 -----------------------------------------------------------------------
 Computes the **merge base** (lowest common ancestor) between two commits in the
 same repository.
 
 Implementation notes:
 - We build two ancestor sets (including each input commit) with depths measured
 as "number of parent steps".
 - We pick the common ancestor with minimal `left_depth + right_depth`.
 
 Errors:
 - Raises if either commit id is NULL, does not exist, or belongs to a different
 repository than the other.
 */
CREATE OR REPLACE FUNCTION fs.get_merge_base(p_left_commit_id UUID, p_right_commit_id UUID) RETURNS UUID AS $$
DECLARE v_left_repo_id UUID;
v_right_repo_id UUID;
v_base_commit_id UUID;
BEGIN IF p_left_commit_id IS NULL
OR p_right_commit_id IS NULL THEN RAISE EXCEPTION 'commit_id must be specified';
END IF;
SELECT repository_id INTO v_left_repo_id
FROM fs.commits
WHERE id = p_left_commit_id;
IF v_left_repo_id IS NULL THEN RAISE EXCEPTION 'Invalid commit_id (left): commit does not exist';
END IF;
SELECT repository_id INTO v_right_repo_id
FROM fs.commits
WHERE id = p_right_commit_id;
IF v_right_repo_id IS NULL THEN RAISE EXCEPTION 'Invalid commit_id (right): commit does not exist';
END IF;
IF v_left_repo_id <> v_right_repo_id THEN RAISE EXCEPTION 'Commits must belong to the same repository';
END IF;
WITH RECURSIVE left_ancestors AS (
  SELECT id,
    parent_commit_id,
    merged_from_commit_id,
    0 AS depth
  FROM fs.commits
  WHERE id = p_left_commit_id
  UNION ALL
  SELECT c.id,
    c.parent_commit_id,
    c.merged_from_commit_id,
    la.depth + 1
  FROM fs.commits c
    JOIN left_ancestors la ON c.id = la.parent_commit_id
    OR c.id = la.merged_from_commit_id
),
right_ancestors AS (
  SELECT id,
    parent_commit_id,
    merged_from_commit_id,
    0 AS depth
  FROM fs.commits
  WHERE id = p_right_commit_id
  UNION ALL
  SELECT c.id,
    c.parent_commit_id,
    c.merged_from_commit_id,
    ra.depth + 1
  FROM fs.commits c
    JOIN right_ancestors ra ON c.id = ra.parent_commit_id
    OR c.id = ra.merged_from_commit_id
),
common AS (
  SELECT l.id,
    MIN(l.depth + r.depth) AS total_depth
  FROM left_ancestors l
    JOIN right_ancestors r USING (id)
  GROUP BY l.id
)
SELECT id INTO v_base_commit_id
FROM common
ORDER BY total_depth ASC
LIMIT 1;
IF v_base_commit_id IS NULL THEN RAISE EXCEPTION 'No common ancestor found (unexpected)';
END IF;
RETURN v_base_commit_id;
END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE;
/*
 fs.get_conflicts(p_left_commit_id uuid, p_right_commit_id uuid) -> TABLE(...)
 ----------------------------------------------------------------------------
 Performs conservative file-level 3-way conflict detection between two commits.
 
 Definitions:
 - For any path, the "state" at a commit is:
 - exists (boolean)
 - is_symlink (boolean)
 - content (text; for symlinks this is the stored target path)
 - Missing/deleted is treated as `exists = FALSE` (and content/is_symlink NULL).
 
 A path is considered in **conflict** when:
 1) The path changed on both sides since the merge base, and
 2) The final resolved states for left and right differ.
 
 Return:
 - One row per conflicting path with base/left/right state details and a
 coarse `conflict_kind` classification:
 - 'delete/modify', 'add/add', or 'modify/modify'.
 
 Usage:
 - Convention: 0 rows means "safe to proceed" for `fs.finalize_commit` and
 `fs.rebase_branch` (from a file-level perspective).
 */
CREATE OR REPLACE FUNCTION fs.get_conflicts(p_left_commit_id UUID, p_right_commit_id UUID) RETURNS TABLE(
    merge_base_commit_id UUID,
    path TEXT,
    base_exists BOOLEAN,
    base_is_symlink BOOLEAN,
    base_content TEXT,
    left_exists BOOLEAN,
    left_is_symlink BOOLEAN,
    left_content TEXT,
    right_exists BOOLEAN,
    right_is_symlink BOOLEAN,
    right_content TEXT,
    conflict_kind TEXT
  ) AS $$
DECLARE v_base_commit_id UUID;
BEGIN v_base_commit_id := fs.get_merge_base(p_left_commit_id, p_right_commit_id);
RETURN QUERY WITH base_snapshot AS (
  SELECT s.path,
    s.content,
    s.is_symlink
  FROM fs._get_commit_snapshot_with_content(v_base_commit_id) s
),
left_snapshot AS (
  SELECT s.path,
    s.content,
    s.is_symlink
  FROM fs._get_commit_snapshot_with_content(p_left_commit_id) s
),
right_snapshot AS (
  SELECT s.path,
    s.content,
    s.is_symlink
  FROM fs._get_commit_snapshot_with_content(p_right_commit_id) s
),
paths AS (
  SELECT b.path AS file_path
  FROM base_snapshot b
  UNION
  SELECT l.path AS file_path
  FROM left_snapshot l
  UNION
  SELECT r.path AS file_path
  FROM right_snapshot r
),
states AS (
  SELECT v_base_commit_id AS merge_base_commit_id,
    p.file_path AS path,
    (b.path IS NOT NULL) AS base_exists,
    b.is_symlink AS base_is_symlink,
    b.content AS base_content,
    (l.path IS NOT NULL) AS left_exists,
    l.is_symlink AS left_is_symlink,
    l.content AS left_content,
    (r.path IS NOT NULL) AS right_exists,
    r.is_symlink AS right_is_symlink,
    r.content AS right_content
  FROM paths p
    LEFT JOIN base_snapshot b ON b.path = p.file_path
    LEFT JOIN left_snapshot l ON l.path = p.file_path
    LEFT JOIN right_snapshot r ON r.path = p.file_path
),
diffs AS (
  SELECT s.*,
    (
      s.left_exists IS DISTINCT
      FROM s.base_exists
        OR s.left_is_symlink IS DISTINCT
      FROM s.base_is_symlink
        OR s.left_content IS DISTINCT
      FROM s.base_content
    ) AS left_changed,
    (
      s.right_exists IS DISTINCT
      FROM s.base_exists
        OR s.right_is_symlink IS DISTINCT
      FROM s.base_is_symlink
        OR s.right_content IS DISTINCT
      FROM s.base_content
    ) AS right_changed,
    (
      s.left_exists IS DISTINCT
      FROM s.right_exists
        OR s.left_is_symlink IS DISTINCT
      FROM s.right_is_symlink
        OR s.left_content IS DISTINCT
      FROM s.right_content
    ) AS sides_differ
  FROM states s
)
SELECT d.merge_base_commit_id,
  d.path,
  d.base_exists,
  COALESCE(d.base_is_symlink, FALSE) AS base_is_symlink,
  d.base_content,
  d.left_exists,
  COALESCE(d.left_is_symlink, FALSE) AS left_is_symlink,
  d.left_content,
  d.right_exists,
  COALESCE(d.right_is_symlink, FALSE) AS right_is_symlink,
  d.right_content,
  CASE
    WHEN d.base_exists
    AND (
      NOT d.left_exists
      OR NOT d.right_exists
    ) THEN 'delete/modify'
    WHEN NOT d.base_exists
    AND d.left_exists
    AND d.right_exists THEN 'add/add'
    ELSE 'modify/modify'
  END AS conflict_kind
FROM diffs d
WHERE d.left_changed
  AND d.right_changed
  AND d.sides_differ
ORDER BY d.path;
END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE;
-- ========================================
-- MERGE / REBASE OPERATIONS (LINEAR HISTORY)
-- ========================================
/*
 fs.finalize_commit(p_commit_id uuid, p_target_branch_id uuid DEFAULT NULL)
 -> TABLE(operation text, repository_id uuid, target_branch_id uuid,
 merge_base_commit_id uuid, previous_target_head_commit_id uuid,
 source_commit_id uuid, merge_commit_id uuid,
 new_target_head_commit_id uuid, applied_file_count int)
 --------------------------------------------------------------------
 Finalizes a merge using a **pre-created** merge commit that already points to
 the target via `parent_commit_id` and to the source via `merged_from_commit_id`.
 
 Workflow:
 - Caller inserts the merge commit row and (optionally) resolves conflicts by
 writing rows into `fs.files` for that commit.
 - This function verifies resolutions for all conflicting paths, applies the
 remaining non-conflicting changes from the source onto the merge commit, and
 optionally advances a target branch head to the merge commit.
 
 Preconditions / notes:
 - The merge commit must have `parent_commit_id` populated.
 - `merged_from_commit_id` is optional:
 - When present: full merge flow (conflict checks, patching). All conflict
 paths must have rows in `fs.files` for the merge commit or the function
 raises.
 - When NULL: treated as a fast-forward finalize; no conflict or patch work is
 performed.
 - `p_target_branch_id` is optional:
 - When provided: validates branch, ensures branch head matches the merge
 commit's parent, and advances the branch head.
 - When NULL: branch pointers are untouched.
 */
CREATE OR REPLACE FUNCTION fs.finalize_commit(
    p_commit_id UUID,
    p_target_branch_id UUID DEFAULT NULL
  ) RETURNS TABLE(
    operation TEXT,
    repository_id UUID,
    target_branch_id UUID,
    merge_base_commit_id UUID,
    previous_target_head_commit_id UUID,
    source_commit_id UUID,
    merge_commit_id UUID,
    new_target_head_commit_id UUID,
    applied_file_count INT
  ) AS $$
DECLARE v_target_repo_id UUID;
v_branch_head_commit_id UUID;
v_merge_repo_id UUID;
v_parent_commit_id UUID;
v_merged_from_commit_id UUID;
v_merge_base_commit_id UUID;
v_conflict_count INT := 0;
v_missing_resolutions INT := 0;
v_rows INT := 0;
BEGIN IF p_commit_id IS NULL THEN RAISE EXCEPTION 'merge_commit_id must be specified';
END IF;
SELECT c.repository_id,
  c.parent_commit_id,
  c.merged_from_commit_id INTO v_merge_repo_id,
  v_parent_commit_id,
  v_merged_from_commit_id
FROM fs.commits c
WHERE c.id = p_commit_id;
IF v_merge_repo_id IS NULL THEN RAISE EXCEPTION 'Invalid merge_commit_id: commit does not exist';
END IF;
IF v_parent_commit_id IS NULL THEN RAISE EXCEPTION 'merge_commit_id must have parent_commit_id set';
END IF;
repository_id := v_merge_repo_id;
target_branch_id := p_target_branch_id;
merge_commit_id := p_commit_id;
-- Resolve branch context if provided
IF p_target_branch_id IS NOT NULL THEN
SELECT b.repository_id,
  b.head_commit_id INTO v_target_repo_id,
  v_branch_head_commit_id
FROM fs.branches b
WHERE b.id = p_target_branch_id;
IF v_target_repo_id IS NULL THEN RAISE EXCEPTION 'Invalid target_branch_id: branch does not exist';
END IF;
IF v_target_repo_id <> v_merge_repo_id THEN RAISE EXCEPTION 'Branch and merge commit must belong to the same repository';
END IF;
IF v_branch_head_commit_id IS NULL THEN RAISE EXCEPTION 'Branch head is NULL; cannot finalize merge';
END IF;
IF v_parent_commit_id <> v_branch_head_commit_id THEN RAISE EXCEPTION 'merge_commit_id must be based on the current branch head';
END IF;
ELSE -- No branch context; treat parent as the target snapshot
v_branch_head_commit_id := v_parent_commit_id;
END IF;
previous_target_head_commit_id := v_branch_head_commit_id;
source_commit_id := v_merged_from_commit_id;
-- Fast-forward finalize when merged_from_commit_id is NULL
IF v_merged_from_commit_id IS NULL THEN v_merge_base_commit_id := v_parent_commit_id;
merge_base_commit_id := v_merge_base_commit_id;
applied_file_count := 0;
IF p_target_branch_id IS NOT NULL THEN
UPDATE fs.branches
SET head_commit_id = p_commit_id
WHERE id = p_target_branch_id;
previous_target_head_commit_id := v_branch_head_commit_id;
new_target_head_commit_id := p_commit_id;
operation := 'fast_forward';
ELSE previous_target_head_commit_id := v_branch_head_commit_id;
new_target_head_commit_id := NULL;
operation := 'fast_forward';
END IF;
RETURN NEXT;
RETURN;
END IF;
v_merge_base_commit_id := fs.get_merge_base(v_branch_head_commit_id, v_merged_from_commit_id);
merge_base_commit_id := v_merge_base_commit_id;
SELECT COUNT(*)::INT INTO v_conflict_count
FROM fs.get_conflicts(v_branch_head_commit_id, v_merged_from_commit_id);
IF v_conflict_count > 0 THEN
SELECT COUNT(*)::INT INTO v_missing_resolutions
FROM fs.get_conflicts(v_branch_head_commit_id, v_merged_from_commit_id) c
WHERE NOT EXISTS (
    SELECT 1
    FROM fs.files f
    WHERE f.commit_id = p_commit_id
      AND f.path = c.path
  );
IF v_missing_resolutions > 0 THEN RAISE EXCEPTION 'Merge requires resolutions for % conflict paths; insert rows into fs.files for commit %',
v_missing_resolutions,
p_commit_id;
END IF;
END IF;
applied_file_count := 0;
-- Apply deletions that the user has not already written into the merge commit.
WITH base_snapshot AS (
  SELECT s.path,
    s.content,
    s.is_symlink
  FROM fs._get_commit_snapshot_with_content(v_merge_base_commit_id) s
),
target_snapshot AS (
  SELECT s.path,
    s.content,
    s.is_symlink
  FROM fs._get_commit_snapshot_with_content(v_branch_head_commit_id) s
),
source_snapshot AS (
  SELECT s.path,
    s.content,
    s.is_symlink
  FROM fs._get_commit_snapshot_with_content(v_merged_from_commit_id) s
),
user_writes AS (
  SELECT f.path
  FROM fs.files f
  WHERE f.commit_id = p_commit_id
),
paths AS (
  SELECT b.path AS file_path
  FROM base_snapshot b
  UNION
  SELECT t.path AS file_path
  FROM target_snapshot t
  UNION
  SELECT s.path AS file_path
  FROM source_snapshot s
  UNION
  SELECT u.path AS file_path
  FROM user_writes u
),
states AS (
  SELECT p.file_path,
    (b.path IS NOT NULL) AS base_exists,
    b.is_symlink AS base_is_symlink,
    b.content AS base_content,
    (t.path IS NOT NULL) AS target_exists,
    t.is_symlink AS target_is_symlink,
    t.content AS target_content,
    (s.path IS NOT NULL) AS source_exists,
    s.is_symlink AS source_is_symlink,
    s.content AS source_content
  FROM paths p
    LEFT JOIN base_snapshot b ON b.path = p.file_path
    LEFT JOIN target_snapshot t ON t.path = p.file_path
    LEFT JOIN source_snapshot s ON s.path = p.file_path
),
diffs AS (
  SELECT st.*,
    (
      st.source_exists IS DISTINCT
      FROM st.base_exists
        OR st.source_is_symlink IS DISTINCT
      FROM st.base_is_symlink
        OR st.source_content IS DISTINCT
      FROM st.base_content
    ) AS source_changed
  FROM states st
),
desired AS (
  SELECT d.file_path,
    CASE
      WHEN d.source_changed THEN d.source_exists
      ELSE d.target_exists
    END AS desired_exists,
    CASE
      WHEN d.source_changed THEN d.source_is_symlink
      ELSE d.target_is_symlink
    END AS desired_is_symlink,
    CASE
      WHEN d.source_changed THEN d.source_content
      ELSE d.target_content
    END AS desired_content,
    d.target_exists,
    d.target_is_symlink,
    d.target_content
  FROM diffs d
),
patch AS (
  SELECT de.file_path,
    de.desired_exists,
    de.desired_is_symlink,
    de.desired_content,
    (
      NOT de.desired_exists
      AND de.target_exists
    ) AS need_delete,
    (
      de.desired_exists
      AND (
        (NOT de.target_exists)
        OR de.target_is_symlink IS DISTINCT
        FROM de.desired_is_symlink
          OR de.target_content IS DISTINCT
        FROM de.desired_content
      )
    ) AS need_write
  FROM desired de
)
INSERT INTO fs.files (commit_id, path, content, is_deleted, is_symlink)
SELECT p_commit_id,
  p.file_path,
  '',
  TRUE,
  FALSE
FROM patch p
WHERE p.need_delete
  AND NOT EXISTS (
    SELECT 1
    FROM fs.files f
    WHERE f.commit_id = p_commit_id
      AND f.path = p.file_path
  );
GET DIAGNOSTICS v_rows = ROW_COUNT;
applied_file_count := applied_file_count + v_rows;
-- Apply writes that the user has not already provided.
WITH base_snapshot AS (
  SELECT s.path,
    s.content,
    s.is_symlink
  FROM fs._get_commit_snapshot_with_content(v_merge_base_commit_id) s
),
target_snapshot AS (
  SELECT s.path,
    s.content,
    s.is_symlink
  FROM fs._get_commit_snapshot_with_content(v_branch_head_commit_id) s
),
source_snapshot AS (
  SELECT s.path,
    s.content,
    s.is_symlink
  FROM fs._get_commit_snapshot_with_content(v_merged_from_commit_id) s
),
user_writes AS (
  SELECT f.path
  FROM fs.files f
  WHERE f.commit_id = p_commit_id
),
paths AS (
  SELECT b.path AS file_path
  FROM base_snapshot b
  UNION
  SELECT t.path AS file_path
  FROM target_snapshot t
  UNION
  SELECT s.path AS file_path
  FROM source_snapshot s
  UNION
  SELECT u.path AS file_path
  FROM user_writes u
),
states AS (
  SELECT p.file_path,
    (b.path IS NOT NULL) AS base_exists,
    b.is_symlink AS base_is_symlink,
    b.content AS base_content,
    (t.path IS NOT NULL) AS target_exists,
    t.is_symlink AS target_is_symlink,
    t.content AS target_content,
    (s.path IS NOT NULL) AS source_exists,
    s.is_symlink AS source_is_symlink,
    s.content AS source_content
  FROM paths p
    LEFT JOIN base_snapshot b ON b.path = p.file_path
    LEFT JOIN target_snapshot t ON t.path = p.file_path
    LEFT JOIN source_snapshot s ON s.path = p.file_path
),
diffs AS (
  SELECT st.*,
    (
      st.source_exists IS DISTINCT
      FROM st.base_exists
        OR st.source_is_symlink IS DISTINCT
      FROM st.base_is_symlink
        OR st.source_content IS DISTINCT
      FROM st.base_content
    ) AS source_changed
  FROM states st
),
desired AS (
  SELECT d.file_path,
    CASE
      WHEN d.source_changed THEN d.source_exists
      ELSE d.target_exists
    END AS desired_exists,
    CASE
      WHEN d.source_changed THEN d.source_is_symlink
      ELSE d.target_is_symlink
    END AS desired_is_symlink,
    CASE
      WHEN d.source_changed THEN d.source_content
      ELSE d.target_content
    END AS desired_content,
    d.target_exists,
    d.target_is_symlink,
    d.target_content
  FROM diffs d
),
patch AS (
  SELECT de.file_path,
    de.desired_exists,
    de.desired_is_symlink,
    de.desired_content,
    (
      NOT de.desired_exists
      AND de.target_exists
    ) AS need_delete,
    (
      de.desired_exists
      AND (
        (NOT de.target_exists)
        OR de.target_is_symlink IS DISTINCT
        FROM de.desired_is_symlink
          OR de.target_content IS DISTINCT
        FROM de.desired_content
      )
    ) AS need_write
  FROM desired de
)
INSERT INTO fs.files (commit_id, path, content, is_deleted, is_symlink)
SELECT p_commit_id,
  p.file_path,
  p.desired_content,
  FALSE,
  COALESCE(p.desired_is_symlink, FALSE)
FROM patch p
WHERE p.need_write
  AND NOT EXISTS (
    SELECT 1
    FROM fs.files f
    WHERE f.commit_id = p_commit_id
      AND f.path = p.file_path
  );
GET DIAGNOSTICS v_rows = ROW_COUNT;
applied_file_count := applied_file_count + v_rows;
IF p_target_branch_id IS NOT NULL THEN
UPDATE fs.branches
SET head_commit_id = p_commit_id
WHERE id = p_target_branch_id;
END IF;
IF v_merge_base_commit_id = v_merged_from_commit_id THEN operation := 'already_up_to_date';
ELSIF v_conflict_count > 0 THEN operation := 'merged_with_conflicts_resolved';
ELSE operation := 'merged';
END IF;
new_target_head_commit_id := CASE
  WHEN p_target_branch_id IS NULL THEN NULL
  ELSE p_commit_id
END;
RETURN NEXT;
END;
$$ LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE;