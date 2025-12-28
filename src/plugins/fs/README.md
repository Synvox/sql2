# SQL Filesystem with Version Control

A complete filesystem with Git-like version control implemented entirely in PostgreSQL. Built for AI pair-coding tools where fs.repositories act as virtual drives with full version control capabilities.

## Features

- **Git-like Version Control**: Commits, fs.branches, and file history
- **Automatic Branch Management**: Branches automatically track their head fs.commits
- **File Versioning**: Immutable file versions with cascading reads
- **Path Normalization**: Automatic path cleaning and validation
- **Cross-platform Compatibility**: Rejects invalid paths for Windows/Unix filesystems
- **Direct Table Access**: Clean API with direct access to core tables
- **ACID Transactions**: Full PostgreSQL transactional guarantees

## Installation

```bash
npm install sql2
```

## Quick Start

```javascript
import { PGlite } from "@electric-sql/pglite";
import { QueryableStatement, type Interpolable } from "sql2";
import { fsPlugin } from "sql2/fs";

// Initialize database
const db = new PGlite();

// Create a custom statement class for PGlite
class PGliteStatement extends QueryableStatement {
  async exec() {
    await db.exec(this.compile());
  }
  async query<T>(): Promise<{ rows: T[] }> {
    return db.query(this.compile(), this.values);
  }
}

// Create the sql template function
const sql = (strings: TemplateStringsArray, ...values: Interpolable[]) =>
  new PGliteStatement(strings, values);

// Install filesystem schema
await fsPlugin(sql);

// Create a repository (auto-creates main branch)
const repoResult = await sql`
  INSERT INTO fs.repositories (name) VALUES ('my-project')
  RETURNING id, default_branch_id
`.query<{ id: string; default_branch_id: string }>();

const { id: repoId, default_branch_id: mainBranchId } = repoResult.rows[0];

// Root commit (only commit where parent_commit_id can be NULL)
const rootCommitResult = await sql`
  INSERT INTO fs.commits (repository_id, parent_commit_id, message)
  VALUES (${repoId}, NULL, 'Initial commit')
  RETURNING id
`.query<{ id: string }>();

const rootCommitId = rootCommitResult.rows[0].id;

await sql`
  INSERT INTO fs.files (commit_id, path, content)
  VALUES (${rootCommitId}, '/README.md', '# My Project\n')
`.exec();

// Set the branch head for the first commit (finalize requires a parent)
await sql`
  UPDATE fs.branches SET head_commit_id = ${rootCommitId}
  WHERE id = ${mainBranchId}
`.exec();

// Next commits: set parent to current head + finalize to advance the branch
const branchResult = await sql`
  SELECT head_commit_id FROM fs.branches WHERE id = ${mainBranchId}
`.query<{ head_commit_id: string }>();

const parentCommitId = branchResult.rows[0].head_commit_id;

const newCommitResult = await sql`
  INSERT INTO fs.commits (repository_id, parent_commit_id, message)
  VALUES (${repoId}, ${parentCommitId}, 'Add utils')
  RETURNING id
`.query<{ id: string }>();

const newCommitId = newCommitResult.rows[0].id;

await sql`
  INSERT INTO fs.files (commit_id, path, content)
  VALUES (${newCommitId}, '/src/utils.js', 'export const ok = true;')
`.exec();

// Applies non-conflicting changes and advances the branch head
await sql`SELECT * FROM fs.finalize_commit(${newCommitId}, ${mainBranchId})`.exec();
```

Use `fs.finalize_commit` any time you want to apply a commit and move a branch
head (fast-forward or merge). Only the very first commit on a branch uses a
direct `UPDATE` because `parent_commit_id` is `NULL`.

## Core Concepts

### Repositories

- Contain fs.branches and fs.commits
- Auto-create a 'main' branch (with `head_commit_id = NULL` until the first commit is created)
- Direct table access: `fs.repositories`

### Branches

- Point to head fs.commits
- Must point to an existing commit (defaults to the repository default branch head if `head_commit_id` is omitted)
- Direct table access: `fs.branches`

### Commits

- Immutable snapshots of repository state
- Chain together via parent relationships (linear history)
- Merge metadata stored via `merged_from_commit_id` while keeping a single parent
- Direct table access: `fs.commits`

### Files

- Versioned content with commit relationships
- Direct table access: `fs.files`
- Automatic path normalization and validation
- Optional symlinks: set `is_symlink = TRUE` and store the (absolute) target path in `content`

## Comprehensive Examples

### 1. Repository Management

```sql
-- Create a repository (auto-creates main branch; it starts empty)
INSERT INTO fs.repositories (name) VALUES ('my-web-app')
RETURNING id;

-- List all fs.repositories
SELECT id, name, created_at FROM fs.repositories;

-- Get repository with default branch info
SELECT r.*, b.name as default_branch_name, b.head_commit_id
FROM fs.repositories r
LEFT JOIN fs.branches b ON r.default_branch_id = b.id;
```

### 2. Branch Management

```sql
-- Create a feature branch (defaults to starting at the repository default branch head)
INSERT INTO fs.branches (repository_id, name)
VALUES ('repo-id', 'feature/user-auth')
RETURNING id;

-- Create a feature branch starting from a specific commit
INSERT INTO fs.branches (repository_id, name, head_commit_id)
VALUES ('repo-id', 'feature/user-auth', 'commit-id')
RETURNING id;

-- List fs.branches for a repository
SELECT id, name, head_commit_id, created_at
FROM fs.branches
WHERE repository_id = 'repo-id';

-- Switch default branch (conceptually)
UPDATE fs.repositories
SET default_branch_id = (SELECT id FROM fs.branches WHERE name = 'main')
WHERE id = 'repo-id';
```

### 3. Making Commits

```sql
-- Create a commit on main (fast-forward finalize)
WITH main_branch AS (
  SELECT id AS branch_id, head_commit_id
  FROM fs.branches
  WHERE repository_id = 'repo-id' AND name = 'main'
)
INSERT INTO fs.commits (repository_id, parent_commit_id, message)
SELECT 'repo-id', head_commit_id, 'Add user authentication'
FROM main_branch
RETURNING id INTO commit_id;

-- Write files for that commit
INSERT INTO fs.files (commit_id, path, content)
VALUES (commit_id, '/src/auth.js', 'export const auth = true;');

-- Apply changes and advance branch head
SELECT * FROM fs.finalize_commit(
  commit_id,
  (SELECT branch_id FROM main_branch)
);

-- Create a commit on a feature branch based on its head
WITH feature_branch AS (
  SELECT id AS branch_id, head_commit_id
  FROM fs.branches
  WHERE repository_id = 'repo-id' AND name = 'feature/user-auth'
)
INSERT INTO fs.commits (repository_id, parent_commit_id, message)
SELECT 'repo-id', head_commit_id, 'Implement login form'
FROM feature_branch
RETURNING id INTO commit_id;

INSERT INTO fs.files (commit_id, path, content)
VALUES (commit_id, '/src/login.js', 'export function login() { return true; }');

SELECT * FROM fs.finalize_commit(
  commit_id,
  (SELECT branch_id FROM feature_branch)
);

-- List commit history/tree for a repository
WITH RECURSIVE commit_history AS (
  -- Start with current branch heads
  SELECT c.id, c.parent_commit_id, c.message, c.created_at,
         b.name as branch_name, 0 as depth
  FROM fs.commits c
  LEFT JOIN fs.branches b ON c.id = b.head_commit_id
  WHERE c.repository_id = 'repo-id'

  UNION ALL

  -- Recursively follow parent commit chain
  SELECT c.id, c.parent_commit_id, c.message, c.created_at,
         ch.branch_name, ch.depth + 1
  FROM fs.commits c
  JOIN commit_history ch ON c.id = ch.parent_commit_id
)
SELECT id, message, created_at, branch_name,
       REPEAT('  ', depth) || '- ' || LEFT(message, 50) as tree_view
FROM commit_history
ORDER BY depth ASC, created_at DESC;
```

### 4. File Operations

```sql
-- First create a commit anchored to the branch head
WITH main_branch AS (
  SELECT id AS branch_id, head_commit_id
  FROM fs.branches
  WHERE repository_id = 'repo-id' AND name = 'main'
)
INSERT INTO fs.commits (repository_id, parent_commit_id, message)
SELECT 'repo-id', head_commit_id, 'Add source files'
FROM main_branch
RETURNING id INTO commit_id;

-- Add files to the commit
INSERT INTO fs.files (commit_id, path, content)
VALUES
  (commit_id, '/src/index.js', 'console.log("Hello World");'),
  (commit_id, '/src/utils.js', 'export function helper() { return true; }'),
  (commit_id, '/README.md', '# My Project\n\nA cool project.');

-- Update a file (creates new version)
INSERT INTO fs.files (commit_id, path, content)
VALUES (commit_id, '/src/index.js', 'console.log("Hello, updated world!");');

-- Apply the commit and advance the branch
SELECT * FROM fs.finalize_commit(
  commit_id,
  (SELECT branch_id FROM main_branch)
);

-- Read current file content
SELECT fs.read_file('commit-id', '/src/index.js') as content;

-- List all files in a commit
SELECT path, is_symlink FROM fs.get_commit_snapshot('commit-id');
```

### 5. Version Control Operations

```sql
-- Get file history across fs.commits
SELECT * FROM fs.get_file_history('latest-commit-id', '/src/index.js');

-- Browse repository contents (equivalent to default branch)
SELECT * FROM fs.get_commit_snapshot((
  SELECT head_commit_id FROM fs.branches
  WHERE id = (SELECT default_branch_id FROM fs.repositories WHERE id = 'repo-id')
));

-- Compare file versions
SELECT
  c1.message as commit_message,
  fs.read_file(c1.id, '/src/index.js') as old_content,
  fs.read_file(c2.id, '/src/index.js') as new_content
FROM fs.commits c1, fs.commits c2
WHERE c1.parent_commit_id IS NULL  -- root commit
  AND c2.parent_commit_id = c1.id;  -- next commit
```

### 6. Advanced Branching Workflow

```sql
-- Create feature branch from current main
INSERT INTO fs.branches (repository_id, name)
VALUES ('repo-id', 'feature/dark-mode')
RETURNING id INTO feature_branch_id;

-- Work on feature branch
WITH feature_branch AS (
  SELECT id AS branch_id, head_commit_id
  FROM fs.branches
  WHERE id = feature_branch_id
)
INSERT INTO fs.commits (repository_id, parent_commit_id, message)
SELECT 'repo-id', head_commit_id, 'Add dark mode styles'
FROM feature_branch
RETURNING id INTO feature_commit_id;

INSERT INTO fs.files (commit_id, path, content)
VALUES (feature_commit_id, '/src/theme.css', '.dark { background: black; color: white; }');

SELECT * FROM fs.finalize_commit(
  feature_commit_id,
  (SELECT branch_id FROM feature_branch)
);

-- Continue working on main branch
WITH main_branch AS (
  SELECT id AS branch_id, head_commit_id
  FROM fs.branches
  WHERE repository_id = 'repo-id' AND name = 'main'
)
INSERT INTO fs.commits (repository_id, parent_commit_id, message)
SELECT 'repo-id', head_commit_id, 'Add user preferences'
FROM main_branch
RETURNING id INTO main_commit_id;

INSERT INTO fs.files (commit_id, path, content)
VALUES (main_commit_id, '/src/preferences.js', 'const theme = localStorage.getItem("theme");');

SELECT * FROM fs.finalize_commit(
  main_commit_id,
  (SELECT branch_id FROM main_branch)
);
****
-- View different branch contents
SELECT 'Main branch:' as branch, path, LEFT(content, 50) as content_preview
FROM fs.get_commit_snapshot((
  SELECT head_commit_id FROM fs.branches
  WHERE id = (SELECT default_branch_id FROM fs.repositories WHERE id = 'repo-id')
))

UNION ALL

SELECT 'Feature branch:' as branch, path, LEFT(content, 50) as content_preview
FROM fs.get_commit_snapshot((SELECT head_commit_id FROM fs.branches WHERE id = feature_branch_id));
```

### 7. Repository Browsing

```sql
-- Browse current repository contents (default branch)
SELECT gcc.repository_name, gcc.path, LEFT(gcc.content, 100) as content_preview
FROM fs.get_commit_snapshot((
  SELECT head_commit_id FROM fs.branches
  WHERE id = (SELECT default_branch_id FROM fs.repositories WHERE name = 'my-project')
)) gcc
ORDER BY gcc.path;

-- Get detailed file info including commit metadata
SELECT
  gcc.repository_name as repo_name,
  b.name as branch_name,
  gcc.path,
  gcc.content,
  gcc.commit_message as snapshot_commit_message,
  gcc.commit_created_at as snapshot_created_at
FROM fs.get_commit_snapshot((
  SELECT head_commit_id FROM fs.branches
  WHERE id = (SELECT default_branch_id FROM fs.repositories WHERE id = 'repo-id')
)) gcc
CROSS JOIN fs.branches b
WHERE b.id = (SELECT default_branch_id FROM fs.repositories WHERE id = 'repo-id')
ORDER BY gcc.path;

-- Compare file sizes across fs.branches
SELECT
  'main' as branch,
  COUNT(*) as file_count,
  SUM(LENGTH(gcc.content)) as total_bytes
FROM fs.get_commit_snapshot((
  SELECT head_commit_id FROM fs.branches
  WHERE repository_id = 'repo-id' AND name = 'main'
)) gcc

UNION ALL

SELECT
  'feature',
  COUNT(*),
  SUM(LENGTH(gcc.content))
FROM fs.get_commit_snapshot((
  SELECT head_commit_id FROM fs.branches
  WHERE repository_id = 'repo-id' AND name = 'feature'
)) gcc;
```
```

## Merging with Linear History

This system keeps history linear (each commit has one parent) while still tracking merges via `merged_from_commit_id`. A merge is a two-phase flow:

1) **Prepare the merge commit** (you do this):
   - Create a commit whose `parent_commit_id` is the target branch head and whose `merged_from_commit_id` is the source head.
   - Optional: add rows to `fs.files` for any conflict paths you want to resolve manually.

   ```sql
   INSERT INTO fs.commits (repository_id, message, parent_commit_id, merged_from_commit_id)
   VALUES ($repo_id, 'Merge feature into main', $target_head, $source_head)
   RETURNING id INTO merge_commit_id;

   -- Optional conflict resolution authored by the user
   INSERT INTO fs.files (commit_id, path, content)
   VALUES (merge_commit_id, '/conflicted.txt', 'resolved content');
   ```

2) **Finalize the merge** (system-assisted):
   - Applies non-conflicting changes from the source onto the merge commit.
   - Verifies every conflicting path has a user-supplied row in `fs.files` for the merge commit (otherwise raises).
   - Advances the target branch head to the merge commit.

   ```sql
   SELECT * FROM fs.finalize_commit(merge_commit_id, $target_branch_id);
   ```

Conflict workflow:
- Discover conflicts: `SELECT * FROM fs.get_conflicts($target_head, $source_head);`
- If conflicts exist, insert resolutions on the merge commit before calling `fs.finalize_commit`.
- If no conflicts, `fs.finalize_commit` applies remaining changes and (optionally) moves the branch head.

`fs.finalize_commit` specifics:
- Inputs: `commit_id` (required) and `target_branch_id` (optional).
  - When `target_branch_id` is provided: it must belong to the same repo and its head must equal the merge commit’s `parent_commit_id`; the branch head is advanced to the merge commit.
  - When omitted: files are applied but branch pointers do not move.
- Validations:
  - `parent_commit_id` is required.
  - If `merged_from_commit_id` is present: full merge flow with conflict checks; all conflict paths must have user-authored rows in `fs.files` for the merge commit or the call raises.
  - If `merged_from_commit_id` is NULL: treated as fast-forward finalize; no conflict/patch work runs.
- Returns: `operation` (`merged`, `merged_with_conflicts_resolved`, `already_up_to_date`, or `fast_forward`), `applied_file_count`, and `new_target_head_commit_id` (NULL when no branch was provided).

Fast-forward / already up to date:
- If the source is already contained in the target, `fs.finalize_commit` returns `operation = 'already_up_to_date'` and leaves files unchanged.

Rebase (still linear):
- `SELECT * FROM fs.rebase_branch($branch_id, $onto_branch_id, 'Rebase message');`
- Conflicts are detected with `fs.get_conflicts` and will raise if present.

## Reference Merge Workflows

### Fast-forward-like merge (no conflicts)
```sql
-- Prepare merge commit
INSERT INTO fs.commits (repository_id, message, parent_commit_id, merged_from_commit_id)
VALUES ($repo_id, 'Merge feature', $target_head, $source_head)
RETURNING id INTO merge_commit_id;

-- Finalize
SELECT * FROM fs.finalize_commit(merge_commit_id, $target_branch_id);
```

### Merge with conflicts
```sql
-- Inspect conflicts
SELECT * FROM fs.get_conflicts($target_head, $source_head);

-- Prepare merge commit
INSERT INTO fs.commits (repository_id, message, parent_commit_id, merged_from_commit_id)
VALUES ($repo_id, 'Merge feature with conflicts', $target_head, $source_head)
RETURNING id INTO merge_commit_id;

-- Provide resolutions on the merge commit
INSERT INTO fs.files (commit_id, path, content)
VALUES (merge_commit_id, '/conflicted.txt', 'resolved content');

-- Finalize (raises if any conflict path lacks a resolution)
SELECT * FROM fs.finalize_commit(merge_commit_id, $target_branch_id);
```

## API Reference

### Tables (Direct Access)

- **`fs.repositories`** - Repository metadata
- **`fs.branches`** - Branch information and head fs.commits
- **`fs.commits`** - Commits (parent defaults to repository default branch head when omitted); merge metadata can be recorded with `merged_from_commit_id`
- **`fs.files`** - Files written in commits (validated/normalized on insert)
  - Set `is_deleted = TRUE` to tombstone-delete a file in a commit
  - Set `is_symlink = TRUE` to create a symlink whose `content` is the normalized absolute target path

### Functions

- **`fs.read_file(commit_id, path)`** - Read file content from specific commit
- **`fs.get_file_history(commit_id, path)`** - Get file version history (`commit_id`, `content`, `is_deleted`, `is_symlink`)
- **`fs.get_commit_delta(commit_id)`** - Get per-commit delta (files written in that commit only)
- **`fs.get_commit_snapshot(commit_id, path_prefix?)`** - Get resolved snapshot at a commit
- **`fs.get_merge_base(left_commit_id, right_commit_id)`** - Lowest common ancestor (traverses `parent_commit_id` and `merged_from_commit_id`)
- **`fs.get_conflicts(left_commit_id, right_commit_id)`** - 3-way conflict detection across commits
- **`fs.finalize_commit(commit_id, target_branch_id NULL)`** - Apply non-conflicting changes for a pre-created merge commit; caller supplies the merge commit (`parent_commit_id` + `merged_from_commit_id`) and any conflict resolutions; optionally advance a branch head when `target_branch_id` is provided
- **`fs.rebase_branch(branch_id, onto_branch_id, message?)`** - Replay branch changes onto another head, keeping linear history

**Returns columns:**
- `repository_id`, `repository_name` - Repository info
- `commit_id` - Commit identifier
- `path` - File path
- `is_deleted`, `is_symlink` - File state flags (delta only; snapshot returns `is_symlink`)
- `file_created_at`, `commit_created_at`, `commit_message` - Timestamps and commit details

**Note:** `fs.get_commit_delta` and `fs.get_commit_snapshot` intentionally do **not** return file `content`. Use `fs.read_file(commit_id, path)` for content lookups.

**Usage:**
```sql
-- Get file delta for any commit
SELECT * FROM fs.get_commit_delta('commit-id');

-- Get contents of branch head (resolve branch to commit first)
SELECT gcc.*, b.name as branch_name
FROM fs.get_commit_delta((SELECT head_commit_id FROM fs.branches WHERE id = 'branch-id')) gcc
CROSS JOIN fs.branches b WHERE b.id = 'branch-id';

-- Get contents of default branch
SELECT gcc.*, b.name as branch_name
FROM fs.get_commit_delta((
  SELECT head_commit_id FROM fs.branches
  WHERE id = (SELECT default_branch_id FROM fs.repositories WHERE id = 'repo-id')
)) gcc
CROSS JOIN fs.branches b WHERE b.id = (SELECT default_branch_id FROM fs.repositories WHERE id = 'repo-id');
```

## Path Validation

All file paths are automatically validated and normalized:

- **Invalid on Windows**: `< > : " | ? *`
- **Invalid on Unix**: null bytes, control characters
- **Normalization**: absolute paths, remove duplicate slashes, trim trailing slashes
- **Length limit**: 4096 characters max

## Development

```bash
# Install dependencies
npm install

# Run tests
npm test

# Run specific test
npm test -- --grep "repository"
```

## Architecture

- **Pure PostgreSQL**: No external dependencies
- **ACID Compliant**: Full transactional guarantees
- **Immutable Files**: File versions never change
- **Cascading Reads**: Parent commit lookup for missing files
- **Branch Head Tracking**: Automatic branch pointer updates

Built with modern PostgreSQL features including CTEs, window functions, and advanced triggers for a complete version control system in SQL.
