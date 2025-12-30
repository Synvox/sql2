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
create schema if not exists fs;

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
represents a repository (a virtual drive / project root).

key ideas:
- a repository always has a "default branch" (`default_branch_id`).
- on repository creation, we automatically create the default `main` branch
(with `head_commit_id = null` initially) and set `default_branch_id`.

columns:
- id (uuid): repository identifier (default: gen_random_uuid()).
- name (text): human-readable unique name.
- default_branch_id (uuid, nullable): points to the default branch row.
- created_at (timestamptz): creation timestamp (default: now()).
*/
create table fs.repositories (
  id uuid primary key default gen_random_uuid(),
  name text not null unique,
  default_branch_id uuid,
  created_at timestamptz not null default now()
);

/*
fs.commits
----------
represents a commit node in a repository's commit graph.

invariants:
- each commit belongs to exactly one repository (`repository_id`).
- each commit has **at most one parent** (`parent_commit_id`).
- only the first/root commit per repository may have `parent_commit_id is null`.
- parents must be in the **same repository** (enforced by a composite fk).

notes:
- commits are intended to be append-only. the system assumes commit rows are not
updated in-place.

columns:
- id (uuid): commit identifier.
- repository_id (uuid): owning repository (fk → fs.repositories).
- parent_commit_id (uuid, nullable): parent commit in the same repository.
- message (text): commit message.
- created_at (timestamptz): creation timestamp.

constraints:
- commits_id_repository_id_unique:
provides a `(id, repository_id)` target for composite foreign keys.
- commits_parent_same_repo_fk:
enforces that `(parent_commit_id, repository_id)` references a commit in the
same repository.
*/
create table fs.commits (
  id uuid primary key default gen_random_uuid(),
  repository_id uuid not null references fs.repositories (id) on delete cascade,
  parent_commit_id uuid,
  -- Optional pointer to the "other" side of a merge. This keeps history linear
  -- while still remembering which commit was merged.
  merged_from_commit_id uuid,
  message text not null,
  created_at timestamptz not null default now(),
  constraint commits_id_repository_id_unique unique (id, repository_id),
  constraint commits_parent_same_repo_fk foreign key (parent_commit_id, repository_id) references fs.commits (id, repository_id) on delete cascade,
  constraint commits_merged_from_same_repo_fk foreign key (merged_from_commit_id, repository_id) references fs.commits (id, repository_id) on delete cascade
);

/*
fs.branches
-----------
represents a named branch pointer within a repository.

branches are lightweight; they track a single `head_commit_id` representing the
"current" commit for that branch.

key ideas:
- new repositories start with a `main` branch whose `head_commit_id` is `null`
until the first commit is created.
- when creating a new branch, if `head_commit_id` is omitted, we default it to
the repository's default branch head (when resolvable).

columns:
- id (uuid): branch identifier.
- repository_id (uuid): owning repository.
- name (text): branch name, unique per repository.
- head_commit_id (uuid, nullable): head commit for this branch. nullable to
support empty repositories and empty branches.
- created_at (timestamptz): creation timestamp.

constraints:
- unique(repository_id, name): one branch with a given name per repo.
- branches_head_commit_same_repo_fk:
enforces that the head commit belongs to the same repository.
*/
create table fs.branches (
  id uuid primary key default gen_random_uuid(),
  repository_id uuid not null references fs.repositories (id) on delete cascade,
  name text not null,
  -- Nullable: new repositories start with an empty default branch until the first commit is created.
  head_commit_id uuid,
  created_at timestamptz not null default now(),
  unique (repository_id, name),
  -- Each repo can have only one branch with a given name
  constraint branches_id_repository_id_unique unique (id, repository_id),
  constraint branches_head_commit_same_repo_fk foreign key (head_commit_id, repository_id) references fs.commits (id, repository_id)
);

/*
fs.files
--------
stores file writes (and deletions) that occur in a given commit.

this table is the fundamental "content store":
- each commit writes a *delta* (not a full snapshot).
- reads and snapshots resolve by walking commit ancestry and selecting the
newest entry for each path.

file states:
- normal file: `is_deleted = false`, `is_symlink = false`, `content` is file text.
- tombstone delete: `is_deleted = true` (path is deleted at that commit).
- symlink: `is_symlink = true` and `content` is the normalized absolute target.

important behavior (enforced by triggers):
- `path` is always stored in canonical form via `fs._normalize_path`.
- when `is_symlink = true`, `content` is normalized as a path (symlink target).
- when `is_deleted = true`, we force `is_symlink = false` and `content = ''`.

columns:
- id (uuid): file row identifier.
- commit_id (uuid): commit containing this file delta (fk → fs.commits).
- path (text): canonical absolute path.
- content (text): file content or (when symlink) normalized target path.
- is_deleted (boolean): tombstone flag.
- is_symlink (boolean): symlink flag.
- created_at (timestamptz): insert timestamp.

constraints:
- unique(commit_id, path): at most one write per path per commit.
*/
create table fs.files (
  id uuid primary key default gen_random_uuid(),
  commit_id uuid not null references fs.commits (id) on delete cascade,
  path text not null,
  content text not null,
  is_deleted boolean not null default false,
  is_symlink boolean not null default false,
  created_at timestamptz not null default now(),
  unique (commit_id, path)
);

/*
repository ↔ default branch relationship
--------------------------------------
we want `fs.repositories.default_branch_id` to reference a branch **in the same
repository**. we achieve this by:
- giving `fs.branches` a composite uniqueness target `(id, repository_id)`, and
- referencing it from repositories using `(default_branch_id, id)`.
*/
alter table fs.repositories
add constraint repositories_default_branch_same_repo_fk foreign key (default_branch_id, id) references fs.branches (id, repository_id) on delete set null;

/*
common usage patterns (examples)
--------------------------------

note: this system intentionally keeps the core tables **direct-write**. that
means callers typically do two things explicitly:
- insert commits/files into the `fs.*` tables, and
- move branch heads forward via `update fs.branches ...`.

create a repository (auto-creates `main` branch; it starts empty):

insert into fs.repositories (name) values ('my-repo') returning id, default_branch_id;

create the first/root commit (parent is null by definition):

insert into fs.commits (repository_id, message, parent_commit_id)
values ($repo_id, 'initial commit', null)
returning id;

advance the branch head to the new commit:

update fs.branches
set head_commit_id = $commit_id
where id = (select default_branch_id from fs.repositories where id = $repo_id);

write files into the commit (paths are normalized on insert):

insert into fs.files (commit_id, path, content)
values ($commit_id, 'src/index.ts', 'console.log(\"hi\")');

read a file at a commit (resolves through ancestors):

select fs.read_file($commit_id, '/src/index.ts');

browse content:

-- Files written in a specific commit (delta)
select path, is_deleted, is_symlink from fs.get_commit_delta($commit_id);

-- Fully resolved tree at a commit (snapshot)
select path, is_symlink from fs.get_commit_snapshot($commit_id, '/src/');

merge / rebase (linear history):

-- Merge source into target:
-- 1) INSERT INTO fs.commits (repository_id, message, parent_commit_id, merged_from_commit_id) ...
-- 2) INSERT INTO fs.files (...) for any conflicting paths you want to resolve manually.
-- 3) SELECT * FROM fs.finalize_commit($merge_commit_id, $target_branch_id);

-- Rebase branch onto another branch head (may fast-forward, noop, or create a single replay commit)
select * from fs.rebase_branch($branch_id, $onto_branch_id, 'rebase message');
*/
-- ========================================
-- PATHS (VALIDATION + NORMALIZATION)
-- ========================================
/*
fs._validate_path(p_path text) -> void
-------------------------------------
internal validation routine shared by path normalization and write triggers.

validation goals:
- prevent malformed / dangerous paths (null/empty, too long, control chars).
- ensure cross-platform filesystem compatibility:
- reject windows-invalid characters: < > : " | ? *
- reject nul bytes and other control characters (except tab/newline/cr).

notes:
- we do not attempt to interpret `..` components; this system treats paths as
opaque strings after normalization. (if you want to prevent traversal-like
semantics, enforce that at a higher layer.)
*/
create or replace function fs._validate_path (p_path text) returns void as $$
declare char_code int;
begin -- check for null or empty path
if p_path is null
or length(trim(p_path)) = 0 then raise exception 'Path cannot be null or empty';
end if;
-- Check for very long paths (over 4096 characters)
if length(p_path) > 4096 then raise exception 'Path is too long (maximum 4096 characters)';
end if;
-- Check for control characters (invalid on Windows, problematic on Unix)
-- Allow tab (\x09), newline (\x0A), carriage return (\x0D)
for i in 1..length(p_path) loop char_code := ascii(
  substring(
    p_path
    from i for 1
  )
);
if char_code < 32
and char_code not in (9, 10, 13) then raise exception 'Path contains control characters (0x%x)',
lpad(upper(to_hex(char_code)), 2, '0');
end if;
end loop;
-- Check for characters invalid on Windows: < > : " | ? *
-- Note: / and \ are allowed as path separators
if p_path ~ '[<>"|?*:]' then raise exception 'Path contains characters invalid on Windows: < > : " | ? *';
end if;
end;
$$ language plpgsql immutable parallel safe;

/*
fs.rebase_branch(p_branch_id uuid, p_onto_branch_id uuid, p_message text default null)
-> table(operation text, repository_id uuid, branch_id uuid, onto_branch_id uuid,
merge_base_commit_id uuid, previous_branch_head_commit_id uuid,
onto_head_commit_id uuid, rebased_commit_id uuid,
new_branch_head_commit_id uuid, applied_file_count int)
------------------------------------------------------------------------------------
performs a **squash rebase** of one branch onto another branch's head.

why "squash":
- this system stores a single-parent commit graph, so we cannot replay per-commit
changes while preserving a multi-parent merge history.
- instead, we compute the branch's **net effect since the merge base** and apply
that patch as a single new commit on top of `onto`.

algorithm (conceptual):
- resolve `branch_head` and `onto_head` from the provided branch ids.
- compute `merge_base := fs.get_merge_base(branch_head, onto_head)`.

cases:
- `p_branch_id = p_onto_branch_id` → `operation = 'noop'`.
- `merge_base = onto_head` → `operation = 'already_up_to_date'`
(the onto head is already contained in the branch history).
- `merge_base = branch_head` → `operation = 'fast_forward'`
(the branch is behind; we can move the branch head to `onto_head`).
- otherwise:
- if `fs.get_conflicts(branch_head, onto_head)` returns any rows, the rebase
is blocked and this function raises an exception.
- create a new commit whose parent is `onto_head`.
- apply the minimal patch that transforms the `onto_head` snapshot into:
"onto snapshot + branch's net changes since merge_base".
- update the branch head to the new commit and return `operation = 'rebased'`.

return values:
- `rebased_commit_id` is `null` for non-creating operations (`noop`,
`already_up_to_date`, `fast_forward`).
- `applied_file_count` is the number of rows inserted into `fs.files` for the
rebased commit (tombstones + writes). it is `0` when no new commit is created.

preconditions / notes:
- both branches must exist and belong to the same repository.
- branch heads must be non-null for diverged-history rebases (otherwise
`fs.get_merge_base` will raise `commit_id must be specified`).
*/
create or replace function fs.rebase_branch (
  p_branch_id uuid,
  p_onto_branch_id uuid,
  p_message text default null
) returns table (
  operation text,
  repository_id uuid,
  branch_id uuid,
  onto_branch_id uuid,
  merge_base_commit_id uuid,
  previous_branch_head_commit_id uuid,
  onto_head_commit_id uuid,
  rebased_commit_id uuid,
  new_branch_head_commit_id uuid,
  applied_file_count int
) as $$
declare v_branch_repo_id uuid;
v_onto_repo_id uuid;
v_branch_head_commit_id uuid;
v_onto_head_commit_id uuid;
v_merge_base_commit_id uuid;
v_conflict_count int := 0;
v_delete_count int := 0;
v_write_count int := 0;
v_rows int := 0;
v_rebased_commit_id uuid;
v_message text;
begin if p_branch_id is null
or p_onto_branch_id is null then raise exception 'Branch_id must be specified';
end if;
select b.repository_id,
  b.head_commit_id into v_branch_repo_id,
  v_branch_head_commit_id
from fs.branches b
where b.id = p_branch_id;
if v_branch_repo_id is null then raise exception 'Invalid branch_id: branch does not exist';
end if;
select b.repository_id,
  b.head_commit_id into v_onto_repo_id,
  v_onto_head_commit_id
from fs.branches b
where b.id = p_onto_branch_id;
if v_onto_repo_id is null then raise exception 'Invalid onto_branch_id: branch does not exist';
end if;
if v_branch_repo_id <> v_onto_repo_id then raise exception 'Branches must belong to the same repository';
end if;
repository_id := v_branch_repo_id;
branch_id := p_branch_id;
onto_branch_id := p_onto_branch_id;
previous_branch_head_commit_id := v_branch_head_commit_id;
onto_head_commit_id := v_onto_head_commit_id;
if p_branch_id = p_onto_branch_id then operation := 'noop';
merge_base_commit_id := v_branch_head_commit_id;
rebased_commit_id := null;
new_branch_head_commit_id := v_branch_head_commit_id;
applied_file_count := 0;
return next;
return;
end if;
v_merge_base_commit_id := fs.get_merge_base(v_branch_head_commit_id, v_onto_head_commit_id);
merge_base_commit_id := v_merge_base_commit_id;
-- If onto is already an ancestor of the branch, nothing to do.
if v_merge_base_commit_id = v_onto_head_commit_id then operation := 'already_up_to_date';
rebased_commit_id := null;
new_branch_head_commit_id := v_branch_head_commit_id;
applied_file_count := 0;
return next;
return;
end if;
-- If branch is an ancestor of onto, fast-forward the branch.
if v_merge_base_commit_id = v_branch_head_commit_id then
update fs.branches
set head_commit_id = v_onto_head_commit_id
where id = p_branch_id;
operation := 'fast_forward';
rebased_commit_id := null;
new_branch_head_commit_id := v_onto_head_commit_id;
applied_file_count := 0;
return next;
return;
end if;
-- Abort on conflicts.
select count(*)::int into v_conflict_count
from fs.get_conflicts(v_branch_head_commit_id, v_onto_head_commit_id);
if v_conflict_count > 0 then raise exception 'Rebase blocked by % conflicts. call fs.get_conflicts(%, %) to inspect.',
v_conflict_count,
v_branch_head_commit_id,
v_onto_head_commit_id;
end if;
-- Compute the minimal patch to apply onto the onto head (no redundant writes).
with base_snapshot as (
  select s.path,
    s.content,
    s.is_symlink
  from fs._get_commit_snapshot_with_content(v_merge_base_commit_id) s
),
onto_snapshot as (
  select s.path,
    s.content,
    s.is_symlink
  from fs._get_commit_snapshot_with_content(v_onto_head_commit_id) s
),
branch_snapshot as (
  select s.path,
    s.content,
    s.is_symlink
  from fs._get_commit_snapshot_with_content(v_branch_head_commit_id) s
),
paths as (
  select b.path as file_path
  from base_snapshot b
  union
  select o.path as file_path
  from onto_snapshot o
  union
  select br.path as file_path
  from branch_snapshot br
),
states as (
  select p.file_path,
    (b.path is not null) as base_exists,
    b.is_symlink as base_is_symlink,
    b.content as base_content,
    (o.path is not null) as onto_exists,
    o.is_symlink as onto_is_symlink,
    o.content as onto_content,
    (br.path is not null) as branch_exists,
    br.is_symlink as branch_is_symlink,
    br.content as branch_content
  from paths p
    left join base_snapshot b on b.path = p.file_path
    left join onto_snapshot o on o.path = p.file_path
    left join branch_snapshot br on br.path = p.file_path
),
diffs as (
  select st.*,
    (
      st.branch_exists is distinct
      from st.base_exists
        or st.branch_is_symlink is distinct
      from st.base_is_symlink
        or st.branch_content is distinct
      from st.base_content
    ) as branch_changed
  from states st
),
desired as (
  select d.file_path,
    case
      when d.branch_changed then d.branch_exists
      else d.onto_exists
    end as desired_exists,
    case
      when d.branch_changed then d.branch_is_symlink
      else d.onto_is_symlink
    end as desired_is_symlink,
    case
      when d.branch_changed then d.branch_content
      else d.onto_content
    end as desired_content,
    d.onto_exists,
    d.onto_is_symlink,
    d.onto_content
  from diffs d
),
patch as (
  select de.file_path,
    de.desired_exists,
    de.desired_is_symlink,
    de.desired_content,
    (
      not de.desired_exists
      and de.onto_exists
    ) as need_delete,
    (
      de.desired_exists
      and (
        (not de.onto_exists)
        or de.onto_is_symlink is distinct
        from de.desired_is_symlink
          or de.onto_content is distinct
        from de.desired_content
      )
    ) as need_write
  from desired de
)
select coalesce(
    sum(
      case
        when p.need_delete then 1
        else 0
      end
    ),
    0
  )::int,
  coalesce(
    sum(
      case
        when p.need_write then 1
        else 0
      end
    ),
    0
  )::int into v_delete_count,
  v_write_count
from patch p;
-- If rebasing would not change the onto snapshot, just move the branch to onto.
if (v_delete_count + v_write_count) = 0 then
update fs.branches
set head_commit_id = v_onto_head_commit_id
where id = p_branch_id;
operation := 'fast_forward';
rebased_commit_id := null;
new_branch_head_commit_id := v_onto_head_commit_id;
applied_file_count := 0;
return next;
return;
end if;
v_message := coalesce(p_message, 'rebase');
insert into fs.commits (repository_id, parent_commit_id, message)
values (
    v_branch_repo_id,
    v_onto_head_commit_id,
    v_message
  )
returning id into v_rebased_commit_id;
-- Apply deletions
with base_snapshot as (
  select s.path,
    s.content,
    s.is_symlink
  from fs._get_commit_snapshot_with_content(v_merge_base_commit_id) s
),
onto_snapshot as (
  select s.path,
    s.content,
    s.is_symlink
  from fs._get_commit_snapshot_with_content(v_onto_head_commit_id) s
),
branch_snapshot as (
  select s.path,
    s.content,
    s.is_symlink
  from fs._get_commit_snapshot_with_content(v_branch_head_commit_id) s
),
paths as (
  select b.path as file_path
  from base_snapshot b
  union
  select o.path as file_path
  from onto_snapshot o
  union
  select br.path as file_path
  from branch_snapshot br
),
states as (
  select p.file_path,
    (b.path is not null) as base_exists,
    b.is_symlink as base_is_symlink,
    b.content as base_content,
    (o.path is not null) as onto_exists,
    o.is_symlink as onto_is_symlink,
    o.content as onto_content,
    (br.path is not null) as branch_exists,
    br.is_symlink as branch_is_symlink,
    br.content as branch_content
  from paths p
    left join base_snapshot b on b.path = p.file_path
    left join onto_snapshot o on o.path = p.file_path
    left join branch_snapshot br on br.path = p.file_path
),
diffs as (
  select st.*,
    (
      st.branch_exists is distinct
      from st.base_exists
        or st.branch_is_symlink is distinct
      from st.base_is_symlink
        or st.branch_content is distinct
      from st.base_content
    ) as branch_changed
  from states st
),
desired as (
  select d.file_path,
    case
      when d.branch_changed then d.branch_exists
      else d.onto_exists
    end as desired_exists,
    case
      when d.branch_changed then d.branch_is_symlink
      else d.onto_is_symlink
    end as desired_is_symlink,
    case
      when d.branch_changed then d.branch_content
      else d.onto_content
    end as desired_content,
    d.onto_exists,
    d.onto_is_symlink,
    d.onto_content
  from diffs d
),
patch as (
  select de.file_path,
    de.desired_exists,
    de.desired_is_symlink,
    de.desired_content,
    (
      not de.desired_exists
      and de.onto_exists
    ) as need_delete,
    (
      de.desired_exists
      and (
        (not de.onto_exists)
        or de.onto_is_symlink is distinct
        from de.desired_is_symlink
          or de.onto_content is distinct
        from de.desired_content
      )
    ) as need_write
  from desired de
)
insert into fs.files (commit_id, path, content, is_deleted, is_symlink)
select v_rebased_commit_id,
  p.file_path,
  '',
  true,
  false
from patch p
where p.need_delete;
get diagnostics v_rows = row_count;
applied_file_count := v_rows;
-- Apply writes
with base_snapshot as (
  select s.path,
    s.content,
    s.is_symlink
  from fs._get_commit_snapshot_with_content(v_merge_base_commit_id) s
),
onto_snapshot as (
  select s.path,
    s.content,
    s.is_symlink
  from fs._get_commit_snapshot_with_content(v_onto_head_commit_id) s
),
branch_snapshot as (
  select s.path,
    s.content,
    s.is_symlink
  from fs._get_commit_snapshot_with_content(v_branch_head_commit_id) s
),
paths as (
  select b.path as file_path
  from base_snapshot b
  union
  select o.path as file_path
  from onto_snapshot o
  union
  select br.path as file_path
  from branch_snapshot br
),
states as (
  select p.file_path,
    (b.path is not null) as base_exists,
    b.is_symlink as base_is_symlink,
    b.content as base_content,
    (o.path is not null) as onto_exists,
    o.is_symlink as onto_is_symlink,
    o.content as onto_content,
    (br.path is not null) as branch_exists,
    br.is_symlink as branch_is_symlink,
    br.content as branch_content
  from paths p
    left join base_snapshot b on b.path = p.file_path
    left join onto_snapshot o on o.path = p.file_path
    left join branch_snapshot br on br.path = p.file_path
),
diffs as (
  select st.*,
    (
      st.branch_exists is distinct
      from st.base_exists
        or st.branch_is_symlink is distinct
      from st.base_is_symlink
        or st.branch_content is distinct
      from st.base_content
    ) as branch_changed
  from states st
),
desired as (
  select d.file_path,
    case
      when d.branch_changed then d.branch_exists
      else d.onto_exists
    end as desired_exists,
    case
      when d.branch_changed then d.branch_is_symlink
      else d.onto_is_symlink
    end as desired_is_symlink,
    case
      when d.branch_changed then d.branch_content
      else d.onto_content
    end as desired_content,
    d.onto_exists,
    d.onto_is_symlink,
    d.onto_content
  from diffs d
),
patch as (
  select de.file_path,
    de.desired_exists,
    de.desired_is_symlink,
    de.desired_content,
    (
      not de.desired_exists
      and de.onto_exists
    ) as need_delete,
    (
      de.desired_exists
      and (
        (not de.onto_exists)
        or de.onto_is_symlink is distinct
        from de.desired_is_symlink
          or de.onto_content is distinct
        from de.desired_content
      )
    ) as need_write
  from desired de
)
insert into fs.files (commit_id, path, content, is_deleted, is_symlink)
select v_rebased_commit_id,
  p.file_path,
  p.desired_content,
  false,
  coalesce(p.desired_is_symlink, false)
from patch p
where p.need_write;
get diagnostics v_rows = row_count;
applied_file_count := applied_file_count + v_rows;
update fs.branches
set head_commit_id = v_rebased_commit_id
where id = p_branch_id;
operation := 'rebased';
rebased_commit_id := v_rebased_commit_id;
new_branch_head_commit_id := v_rebased_commit_id;
return next;
end;
$$ language plpgsql volatile parallel unsafe;

/*
fs._normalize_path(p_path text) -> text
--------------------------------------
internal helper that canonicalizes a file path for storage and lookup.

normalization rules:
- validate using `fs._validate_path`.
- accept windows-style separators (`\`) on input but store canonical `/`.
- ensure the path is absolute (prefix `/` if missing).
- collapse duplicate slashes (`//` → `/`).
- remove trailing slash unless the path is exactly `/`.

examples:
- `src/main.ts` → `/src/main.ts`
- `//src//main.ts` → `/src/main.ts`
- `/src/main.ts/` → `/src/main.ts`
- `target.txt` → `/target.txt`

this function is used by write triggers and read helpers so callers may use a
variety of input styles while the database stores a single canonical form.
*/
create or replace function fs._normalize_path (p_path text) returns text as $$
declare normalized_path text;
begin -- first validate the path
perform fs._validate_path(p_path);
-- Normalize path separators (accept Windows-style "\" input, store canonical "/" paths)
normalized_path := replace(p_path, e'\\', '/');
-- Ensure path starts with /
normalized_path := case
  when normalized_path like '/%' then normalized_path
  else '/' || normalized_path
end;
-- Remove duplicate slashes
while normalized_path like '%//%' loop normalized_path := replace(normalized_path, '//', '/');
end loop;
-- Remove trailing slash unless it's just "/"
if length(normalized_path) > 1
and normalized_path like '%/' then normalized_path := left(normalized_path, length(normalized_path) - 1);
end if;
-- Ensure the normalized output stays within our max length budget
if length(normalized_path) > 4096 then raise exception 'Path is too long (maximum 4096 characters)';
end if;
return normalized_path;
end;
$$ language plpgsql immutable parallel safe;

/*
fs._normalize_path_prefix(p_path_prefix text) -> text
----------------------------------------------------
internal helper to normalize prefix strings used for prefix searches (e.g.
`fs.get_commit_snapshot(commit_id, '/src/')`).

why this differs from `fs._normalize_path`:
- for prefix searches we preserve the meaning of an explicit trailing separator.
for example, an intent of `/src/` usually means "paths under src/", not "paths
starting with /src" (which would also match `/src-old/...`).

rules:
- validate using `fs._validate_path`.
- accept `\` on input but store canonical `/`.
- ensure the prefix is absolute (prefix `/` if missing).
- collapse duplicate slashes.
- if the input ended with `/` or `\`, preserve a trailing `/` in the normalized
output (except for `/` itself).
*/
create or replace function fs._normalize_path_prefix (p_path_prefix text) returns text as $$
declare normalized_prefix text;
declare has_trailing_slash boolean;
begin perform fs._validate_path(p_path_prefix);
has_trailing_slash := right(p_path_prefix, 1) = '/'
or right(p_path_prefix, 1) = e'\\';
normalized_prefix := replace(p_path_prefix, e'\\', '/');
-- Ensure prefix starts with /
normalized_prefix := case
  when normalized_prefix like '/%' then normalized_prefix
  else '/' || normalized_prefix
end;
-- Remove duplicate slashes
while normalized_prefix like '%//%' loop normalized_prefix := replace(normalized_prefix, '//', '/');
end loop;
-- Preserve explicit trailing slash (directory-style prefix matching)
if has_trailing_slash
and normalized_prefix <> '/'
and right(normalized_prefix, 1) <> '/' then normalized_prefix := normalized_prefix || '/';
end if;
if length(normalized_prefix) > 4096 then raise exception 'Path is too long (maximum 4096 characters)';
end if;
return normalized_prefix;
end;
$$ language plpgsql immutable parallel safe;

-- ========================================
-- WRITE-TIME HELPERS (TRIGGER FUNCTIONS)
-- ========================================
/*
fs._commits_before_insert_trigger() -> trigger
---------------------------------------------
before insert trigger on `fs.commits`.

primary responsibilities:
- if `new.parent_commit_id` is omitted/null, default it to the repository's
default branch head (when available).
- this keeps the commit graph well-formed while still allowing the first/root
commit of a repository to have a null parent.
- provide clearer error messages than raw fk failures:
- if commits already exist but we cannot resolve a default parent, require an
explicit `parent_commit_id`.
- if a parent is provided, ensure it belongs to the same repository.

important: this trigger does **not** advance any branch heads. branch heads are
explicit pointers and should be updated intentionally by the caller.
*/
create or replace function fs._commits_before_insert_trigger () returns trigger as $$
declare resolved_parent_commit_id uuid;
begin -- validate repository exists (fk would catch this too, but keep a clearer error message).
if not exists (
  select 1
  from fs.repositories
  where id = new.repository_id
) then raise exception 'Invalid repository_id: repository does not exist';
end if;
if new.parent_commit_id is null then
select b.head_commit_id into resolved_parent_commit_id
from fs.repositories r
  join fs.branches b on b.id = r.default_branch_id
where r.id = new.repository_id;
if resolved_parent_commit_id is not null then new.parent_commit_id := resolved_parent_commit_id;
else -- if commits already exist, we should have been able to resolve a parent.
if exists (
  select 1
  from fs.commits
  where repository_id = new.repository_id
) then raise exception 'Parent_commit_id must be specified (repository default branch head could not be resolved)';
end if;
end if;
end if;
-- Enforce that parent_commit_id (if provided) belongs to the same repository.
-- (Redundant with commits_parent_same_repo_fk, but yields a nicer error.)
if new.parent_commit_id is not null
and not exists (
  select 1
  from fs.commits
  where id = new.parent_commit_id
    and repository_id = new.repository_id
) then raise exception 'Invalid parent_commit_id: must reference a commit in the same repository';
end if;
return new;
end;
$$ language plpgsql volatile parallel unsafe;

-- ========================================
-- TRIGGERS
-- ========================================
/*
fs._files_before_insert_trigger() -> trigger
-------------------------------------------
before insert trigger on `fs.files`.

responsibilities:
- normalize `new.path` to the canonical absolute form (via `fs._normalize_path`).
- default flags (`is_deleted`, `is_symlink`) to false when null is supplied.
- enforce file-state invariants:
- if `is_deleted = true`: force `is_symlink = false` and coalesce content to ''.
- if `is_deleted = false`: require non-null content.
- if `is_symlink = true`: normalize `content` as an absolute path (the target).
*/
create or replace function fs._files_before_insert_trigger () returns trigger as $$ begin -- enforce canonical paths even if someone inserts directly into fs.files
  new.path := fs._normalize_path(new.path);
new.is_deleted := coalesce(new.is_deleted, false);
new.is_symlink := coalesce(new.is_symlink, false);
if new.is_deleted then new.is_symlink := false;
new.content := coalesce(new.content, '');
else if new.content is null then raise exception 'Content must be specified when inserting a non-deleted file';
end if;
if new.is_symlink then new.content := fs._normalize_path(new.content);
end if;
end if;
return new;
end;
$$ language plpgsql volatile parallel unsafe;

/*
fs._repositories_after_insert() -> trigger
-----------------------------------------
after insert trigger on `fs.repositories`.

creates the default `main` branch for the new repository and records it in
`fs.repositories.default_branch_id`.

the created branch starts with `head_commit_id = null` so that the first/root
commit can be created with `parent_commit_id = null`.
*/
create or replace function fs._repositories_after_insert () returns trigger as $$
declare branch_id uuid;
begin -- create the default branch with no head yet. the first commit can be created with parent_commit_id = null.
insert into fs.branches (repository_id, name, head_commit_id)
values (new.id, 'main', null)
returning id into branch_id;
-- Set the default branch on the repository
update fs.repositories
set default_branch_id = branch_id
where id = new.id;
return new;
end;
$$ language plpgsql volatile parallel unsafe;

/*
fs._branches_before_insert() -> trigger
--------------------------------------
before insert trigger on `fs.branches`.

if `new.head_commit_id` is omitted/null:
- default it to the repository default branch head (when resolvable).
- if the repository already has commits but the default branch head cannot be
resolved (e.g. the default branch still has a null head), require an explicit
`head_commit_id`.
- otherwise allow null (empty branch in a repository that has no commits yet).
*/
create or replace function fs._branches_before_insert () returns trigger as $$
declare resolved_head_commit_id uuid;
begin if new.head_commit_id is null then
select b.head_commit_id into resolved_head_commit_id
from fs.repositories r
  join fs.branches b on b.id = r.default_branch_id
where r.id = new.repository_id;
if resolved_head_commit_id is null then -- if the repository already has commits, we should be able to default to something. require explicit head_commit_id.
if exists (
  select 1
  from fs.commits
  where repository_id = new.repository_id
) then raise exception 'Head_commit_id must be specified when creating a branch';
end if;
-- Otherwise allow NULL head_commit_id (empty branch before first commit).
return new;
end if;
new.head_commit_id := resolved_head_commit_id;
end if;
return new;
end;
$$ language plpgsql volatile parallel unsafe;

/*
trigger wiring
--------------
we use triggers to enforce canonical storage and provide convenience defaults,
while keeping the direct-write table api intact.
*/
-- Create trigger on fs.commits table
create trigger commits_before_insert_trigger before insert on fs.commits for each row
execute function fs._commits_before_insert_trigger ();

-- Create trigger on fs.files table
create trigger files_before_insert_trigger before insert on fs.files for each row
execute function fs._files_before_insert_trigger ();

-- Create trigger on fs.repositories table
create trigger repositories_after_insert_trigger
after insert on fs.repositories for each row
execute function fs._repositories_after_insert ();

-- Create trigger on branches table
create trigger branches_before_insert_trigger before insert on fs.branches for each row
execute function fs._branches_before_insert ();

-- ========================================
-- INDEXES
-- ========================================
/*
indexes
-------
these indexes support common traversals and invariants:
- (repository_id, parent_commit_id): accelerates ancestor walking and merge-base
queries.
- (repository_id, merged_from_commit_id): accelerates merge-base lookups across
linear merge commits.
- "one root per repo": guarantees only one commit per repository can have a null
parent.
*/
create index idx_commits_repository_parent on fs.commits (repository_id, parent_commit_id);

create index idx_commits_repository_merged_from on fs.commits (repository_id, merged_from_commit_id);

-- Only the first/root commit in a repository may have a NULL parent
create unique index commits_one_root_per_repo_idx on fs.commits (repository_id)
where
  parent_commit_id is null;

-- ========================================
-- READ HELPERS
-- ========================================
/*
fs.get_commit_delta(p_commit_id uuid) -> table(...)
--------------------------------------------------
returns the **commit delta**: all file rows written *in that commit only*,
joined with repository and commit metadata.

this does **not** resolve ancestors. it is primarily useful for:
- auditing what a commit changed (including deletions and symlinks),
- computing diffs at the commit layer.

notes:
- if a commit wrote no files, this returns 0 rows.
- file content is intentionally omitted from the returned table shape; use
`fs.read_file(commit_id, path)` when you need content.
*/
create or replace function fs.get_commit_delta (p_commit_id uuid) returns table (
  repository_id uuid,
  repository_name text,
  commit_id uuid,
  path text,
  is_deleted boolean,
  is_symlink boolean,
  file_created_at timestamptz,
  commit_created_at timestamptz,
  commit_message text
) as $$ begin return query
select r.id as repository_id,
  r.name as repository_name,
  c.id as commit_id,
  f.path,
  f.is_deleted,
  f.is_symlink,
  f.created_at as file_created_at,
  c.created_at as commit_created_at,
  c.message as commit_message
from fs.commits c
  join fs.repositories r on c.repository_id = r.id
  join fs.files f on c.id = f.commit_id
where c.id = p_commit_id;
end;
$$ language plpgsql stable parallel safe;

/*
fs.get_commit_snapshot(p_commit_id uuid, p_path_prefix text default null) -> table(...)
-------------------------------------------------------------------------------------
public snapshot helper that returns the resolved file tree at a commit **without**
file content.

rationale:
- snapshots are often used for browsing/listing; omitting content keeps result
sets smaller and avoids accidentally fetching large blobs.
- use `fs.read_file(commit_id, path)` to fetch content for specific paths.

implementation detail:
- this function intentionally avoids materializing file `content` so that callers
can browse large repositories cheaply.
*/
create or replace function fs.get_commit_snapshot (p_commit_id uuid, p_path_prefix text default null) returns table (
  repository_id uuid,
  repository_name text,
  commit_id uuid,
  path text,
  is_symlink boolean,
  commit_created_at timestamptz,
  commit_message text
) as $$
declare normalized_prefix text := null;
begin if p_commit_id is null then raise exception 'Commit_id must be specified';
end if;
if not exists (
  select 1
  from fs.commits
  where id = p_commit_id
) then raise exception 'Invalid commit_id: commit does not exist';
end if;
if p_path_prefix is not null then normalized_prefix := fs._normalize_path_prefix(p_path_prefix);
end if;
return query with recursive commit_tree as (
  -- Start with the given commit
  select id,
    parent_commit_id,
    0 as depth
  from fs.commits
  where id = p_commit_id
  union all
  -- Recursively add parent commits
  select c.id,
    c.parent_commit_id,
    ct.depth + 1
  from fs.commits c
    inner join commit_tree ct on c.id = ct.parent_commit_id
),
all_files as (
  -- Get all files from commits in the tree, preferring newer versions.
  -- Note: we do NOT select `content` here. This prevents materializing large
  -- blobs when callers only need to browse paths/metadata.
  select f.path,
    f.is_deleted,
    f.is_symlink,
    row_number() over (
      partition by f.path
      order by ct.depth asc
    ) as rn
  from commit_tree ct
    join fs.files f on ct.id = f.commit_id
  where (
      normalized_prefix is null
      or starts_with(f.path, normalized_prefix)
    )
),
snapshot_files as (
  select af.path,
    af.is_symlink
  from all_files af
  where af.rn = 1
    and not af.is_deleted
)
select r.id as repository_id,
  r.name as repository_name,
  c.id as commit_id,
  sf.path,
  sf.is_symlink,
  c.created_at as commit_created_at,
  c.message as commit_message
from fs.commits c
  join fs.repositories r on c.repository_id = r.id
  join snapshot_files sf on true
where c.id = p_commit_id
order by sf.path;
end;
$$ language plpgsql stable parallel safe;

/*
fs._get_commit_snapshot_with_content(p_commit_id uuid, p_path_prefix text default null)
-> table(repository_id uuid, repository_name text, commit_id uuid,
path text, content text, is_symlink boolean,
commit_created_at timestamptz, commit_message text)
-------------------------------------------------------------------------------------
internal helper that returns the resolved file snapshot at a commit, including
file `content`.

implementation detail:
- we first compute the resolved snapshot *without* content via `fs.get_commit_snapshot`.
- we then populate `content` via `fs.read_file(commit_id, path)` per returned path.

this avoids materializing all historical file contents during snapshot resolution
(which can be very large). the trade-off is that it may perform more work per
path because `fs.read_file` resolves by walking ancestry.

notes:
- symlinks are not dereferenced; `content` for a symlink is the stored target.
- this function is used by conflict detection, merges, and rebases where content
comparisons are required.
*/
create or replace function fs._get_commit_snapshot_with_content (p_commit_id uuid, p_path_prefix text default null) returns table (
  repository_id uuid,
  repository_name text,
  commit_id uuid,
  path text,
  content text,
  is_symlink boolean,
  commit_created_at timestamptz,
  commit_message text
) as $$ begin return query
select s.repository_id,
  s.repository_name,
  s.commit_id,
  s.path,
  fs.read_file(s.commit_id, s.path) as content,
  s.is_symlink,
  s.commit_created_at,
  s.commit_message
from fs.get_commit_snapshot(p_commit_id, p_path_prefix) s
order by s.path;
end;
$$ language plpgsql stable parallel safe;

-- ========================================
-- ESSENTIAL FUNCTIONS
-- ========================================
/*
fs.read_file(p_commit_id uuid, p_file_path text) -> text | null
--------------------------------------------------------------
reads a file as of a given commit by resolving through the commit's ancestry.

resolution rules:
- normalize `p_file_path` using `fs._normalize_path`.
- starting at `p_commit_id`, walk `parent_commit_id` pointers upward until a row
is found for the normalized path.
- if the first row found is a tombstone (`is_deleted = true`), return `null`.
- otherwise return the stored `content`.

notes:
- symlinks are not dereferenced. for `is_symlink = true`, `content` is the
normalized absolute symlink target path and is returned as-is.
- this function includes a large traversal step limit as a safety guard in case
of an unexpected commit-parent cycle.
*/
create or replace function fs.read_file (p_commit_id uuid, p_file_path text) returns text as $$
declare normalized_path text;
declare result_content text := null;
declare result_is_deleted boolean := false;
declare current_commit uuid := p_commit_id;
declare step_count int := 0;
begin -- walk up the commit tree to find the file
if p_commit_id is null then raise exception 'Commit_id must be specified';
end if;
if not exists (
  select 1
  from fs.commits
  where id = p_commit_id
) then raise exception 'Invalid commit_id: commit does not exist';
end if;
normalized_path := fs._normalize_path(p_file_path);
while current_commit is not null loop step_count := step_count + 1;
if step_count > 100000 then raise exception 'Commit history traversal exceeded % steps (cycle?)',
step_count;
end if;
select content,
  is_deleted into result_content,
  result_is_deleted
from fs.files
where commit_id = current_commit
  and path = normalized_path;
-- If found, return it (tombstones delete the file)
if found then if result_is_deleted then return null;
end if;
return result_content;
end if;
-- Move to parent commit
select parent_commit_id into current_commit
from fs.commits
where id = current_commit;
end loop;
-- File not found in any ancestor commit
return null;
end;
$$ language plpgsql stable parallel safe;

/*
fs.get_file_history(p_commit_id uuid, p_file_path text)
-> table(commit_id uuid, content text | null, is_deleted boolean, is_symlink boolean)
--------------------------------------------------------------------------------------
returns the history of a single path by walking from `p_commit_id` up through parents
and returning the commits that have an explicit row for that path.

meaning:
- each returned row corresponds to a commit that *touched* the path (write, delete,
or symlink change).
- if the row is a tombstone, `content` is returned as null.

ordering:
- the function does not guarantee output ordering. callers should add an `order by`
clause (e.g. join `fs.commits` to order by commit time) based on their needs.
*/
create or replace function fs.get_file_history (p_commit_id uuid, p_file_path text) returns table (
  commit_id uuid,
  content text,
  is_deleted boolean,
  is_symlink boolean
) as $$
declare normalized_path text;
begin if p_commit_id is null then raise exception 'Commit_id must be specified';
end if;
if not exists (
  select 1
  from fs.commits
  where id = p_commit_id
) then raise exception 'Invalid commit_id: commit does not exist';
end if;
normalized_path := fs._normalize_path(p_file_path);
return query with recursive commit_tree as (
  -- Start with the given commit
  select id,
    parent_commit_id,
    created_at
  from fs.commits
  where id = p_commit_id
  union all
  -- Recursively add parent commits
  select c.id,
    c.parent_commit_id,
    c.created_at
  from fs.commits c
    inner join commit_tree ct on c.id = ct.parent_commit_id
)
select ct.id,
  case
    when f.is_deleted then null
    else f.content
  end as content,
  f.is_deleted,
  f.is_symlink
from commit_tree ct
  left join fs.files f on ct.id = f.commit_id
  and f.path = normalized_path
where f.id is not null;
-- Only include commits that have this file
end;
$$ language plpgsql stable parallel safe;

-- ========================================
-- MERGE / REBASE HELPERS (CONFLICT DETECTION)
-- ========================================
/*
fs.get_merge_base(p_left_commit_id uuid, p_right_commit_id uuid) -> uuid
-----------------------------------------------------------------------
computes the **merge base** (lowest common ancestor) between two commits in the
same repository.

implementation notes:
- we build two ancestor sets (including each input commit) with depths measured
as "number of parent steps".
- we pick the common ancestor with minimal `left_depth + right_depth`.

errors:
- raises if either commit id is null, does not exist, or belongs to a different
repository than the other.
*/
create or replace function fs.get_merge_base (p_left_commit_id uuid, p_right_commit_id uuid) returns uuid as $$
declare v_left_repo_id uuid;
v_right_repo_id uuid;
v_base_commit_id uuid;
begin if p_left_commit_id is null
or p_right_commit_id is null then raise exception 'Commit_id must be specified';
end if;
select repository_id into v_left_repo_id
from fs.commits
where id = p_left_commit_id;
if v_left_repo_id is null then raise exception 'Invalid commit_id (left): commit does not exist';
end if;
select repository_id into v_right_repo_id
from fs.commits
where id = p_right_commit_id;
if v_right_repo_id is null then raise exception 'Invalid commit_id (right): commit does not exist';
end if;
if v_left_repo_id <> v_right_repo_id then raise exception 'Commits must belong to the same repository';
end if;
with recursive left_ancestors as (
  select id,
    parent_commit_id,
    merged_from_commit_id,
    0 as depth
  from fs.commits
  where id = p_left_commit_id
  union all
  select c.id,
    c.parent_commit_id,
    c.merged_from_commit_id,
    la.depth + 1
  from fs.commits c
    join left_ancestors la on c.id = la.parent_commit_id
    or c.id = la.merged_from_commit_id
),
right_ancestors as (
  select id,
    parent_commit_id,
    merged_from_commit_id,
    0 as depth
  from fs.commits
  where id = p_right_commit_id
  union all
  select c.id,
    c.parent_commit_id,
    c.merged_from_commit_id,
    ra.depth + 1
  from fs.commits c
    join right_ancestors ra on c.id = ra.parent_commit_id
    or c.id = ra.merged_from_commit_id
),
common as (
  select l.id,
    min(l.depth + r.depth) as total_depth
  from left_ancestors l
    join right_ancestors r using (id)
  group by l.id
)
select id into v_base_commit_id
from common
order by total_depth asc
limit 1;
if v_base_commit_id is null then raise exception 'No common ancestor found (unexpected)';
end if;
return v_base_commit_id;
end;
$$ language plpgsql stable parallel safe;

/*
fs.get_conflicts(p_left_commit_id uuid, p_right_commit_id uuid) -> table(...)
----------------------------------------------------------------------------
performs conservative file-level 3-way conflict detection between two commits.

definitions:
- for any path, the "state" at a commit is:
- exists (boolean)
- is_symlink (boolean)
- content (text; for symlinks this is the stored target path)
- missing/deleted is treated as `exists = false` (and content/is_symlink null).

a path is considered in **conflict** when:
1) the path changed on both sides since the merge base, and
2) the final resolved states for left and right differ.

return:
- one row per conflicting path with base/left/right state details and a
coarse `conflict_kind` classification:
- 'delete/modify', 'add/add', or 'modify/modify'.

usage:
- convention: 0 rows means "safe to proceed" for `fs.finalize_commit` and
`fs.rebase_branch` (from a file-level perspective).
*/
create or replace function fs.get_conflicts (p_left_commit_id uuid, p_right_commit_id uuid) returns table (
  merge_base_commit_id uuid,
  path text,
  base_exists boolean,
  base_is_symlink boolean,
  base_content text,
  left_exists boolean,
  left_is_symlink boolean,
  left_content text,
  right_exists boolean,
  right_is_symlink boolean,
  right_content text,
  conflict_kind text
) as $$
declare v_base_commit_id uuid;
begin v_base_commit_id := fs.get_merge_base(p_left_commit_id, p_right_commit_id);
return query with base_snapshot as (
  select s.path,
    s.content,
    s.is_symlink
  from fs._get_commit_snapshot_with_content(v_base_commit_id) s
),
left_snapshot as (
  select s.path,
    s.content,
    s.is_symlink
  from fs._get_commit_snapshot_with_content(p_left_commit_id) s
),
right_snapshot as (
  select s.path,
    s.content,
    s.is_symlink
  from fs._get_commit_snapshot_with_content(p_right_commit_id) s
),
paths as (
  select b.path as file_path
  from base_snapshot b
  union
  select l.path as file_path
  from left_snapshot l
  union
  select r.path as file_path
  from right_snapshot r
),
states as (
  select v_base_commit_id as merge_base_commit_id,
    p.file_path as path,
    (b.path is not null) as base_exists,
    b.is_symlink as base_is_symlink,
    b.content as base_content,
    (l.path is not null) as left_exists,
    l.is_symlink as left_is_symlink,
    l.content as left_content,
    (r.path is not null) as right_exists,
    r.is_symlink as right_is_symlink,
    r.content as right_content
  from paths p
    left join base_snapshot b on b.path = p.file_path
    left join left_snapshot l on l.path = p.file_path
    left join right_snapshot r on r.path = p.file_path
),
diffs as (
  select s.*,
    (
      s.left_exists is distinct
      from s.base_exists
        or s.left_is_symlink is distinct
      from s.base_is_symlink
        or s.left_content is distinct
      from s.base_content
    ) as left_changed,
    (
      s.right_exists is distinct
      from s.base_exists
        or s.right_is_symlink is distinct
      from s.base_is_symlink
        or s.right_content is distinct
      from s.base_content
    ) as right_changed,
    (
      s.left_exists is distinct
      from s.right_exists
        or s.left_is_symlink is distinct
      from s.right_is_symlink
        or s.left_content is distinct
      from s.right_content
    ) as sides_differ
  from states s
)
select d.merge_base_commit_id,
  d.path,
  d.base_exists,
  coalesce(d.base_is_symlink, false) as base_is_symlink,
  d.base_content,
  d.left_exists,
  coalesce(d.left_is_symlink, false) as left_is_symlink,
  d.left_content,
  d.right_exists,
  coalesce(d.right_is_symlink, false) as right_is_symlink,
  d.right_content,
  case
    when d.base_exists
    and (
      not d.left_exists
      or not d.right_exists
    ) then 'delete/modify'
    when not d.base_exists
    and d.left_exists
    and d.right_exists then 'add/add'
    else 'modify/modify'
  end as conflict_kind
from diffs d
where d.left_changed
  and d.right_changed
  and d.sides_differ
order by d.path;
end;
$$ language plpgsql stable parallel safe;

-- ========================================
-- MERGE / REBASE OPERATIONS (LINEAR HISTORY)
-- ========================================
/*
fs.finalize_commit(p_commit_id uuid, p_target_branch_id uuid default null)
-> table(operation text, repository_id uuid, target_branch_id uuid,
merge_base_commit_id uuid, previous_target_head_commit_id uuid,
source_commit_id uuid, merge_commit_id uuid,
new_target_head_commit_id uuid, applied_file_count int)
--------------------------------------------------------------------
finalizes a merge using a **pre-created** merge commit that already points to
the target via `parent_commit_id` and to the source via `merged_from_commit_id`.

workflow:
- caller inserts the merge commit row and (optionally) resolves conflicts by
writing rows into `fs.files` for that commit.
- this function verifies resolutions for all conflicting paths, applies the
remaining non-conflicting changes from the source onto the merge commit, and
optionally advances a target branch head to the merge commit.

preconditions / notes:
- the merge commit must have `parent_commit_id` populated.
- `merged_from_commit_id` is optional:
- when present: full merge flow (conflict checks, patching). all conflict
paths must have rows in `fs.files` for the merge commit or the function
raises.
- when null: treated as a fast-forward finalize; no conflict or patch work is
performed.
- `p_target_branch_id` is optional:
- when provided: validates branch, ensures branch head matches the merge
commit's parent, and advances the branch head.
- when null: branch pointers are untouched.
*/
create or replace function fs.finalize_commit (
  p_commit_id uuid,
  p_target_branch_id uuid default null
) returns table (
  operation text,
  repository_id uuid,
  target_branch_id uuid,
  merge_base_commit_id uuid,
  previous_target_head_commit_id uuid,
  source_commit_id uuid,
  merge_commit_id uuid,
  new_target_head_commit_id uuid,
  applied_file_count int
) as $$
declare v_target_repo_id uuid;
v_branch_head_commit_id uuid;
v_merge_repo_id uuid;
v_parent_commit_id uuid;
v_merged_from_commit_id uuid;
v_merge_base_commit_id uuid;
v_conflict_count int := 0;
v_missing_resolutions int := 0;
v_rows int := 0;
begin if p_commit_id is null then raise exception 'Merge_commit_id must be specified';
end if;
select c.repository_id,
  c.parent_commit_id,
  c.merged_from_commit_id into v_merge_repo_id,
  v_parent_commit_id,
  v_merged_from_commit_id
from fs.commits c
where c.id = p_commit_id;
if v_merge_repo_id is null then raise exception 'Invalid merge_commit_id: commit does not exist';
end if;
if v_parent_commit_id is null then raise exception 'Merge_commit_id must have parent_commit_id set';
end if;
repository_id := v_merge_repo_id;
target_branch_id := p_target_branch_id;
merge_commit_id := p_commit_id;
-- Resolve branch context if provided
if p_target_branch_id is not null then
select b.repository_id,
  b.head_commit_id into v_target_repo_id,
  v_branch_head_commit_id
from fs.branches b
where b.id = p_target_branch_id;
if v_target_repo_id is null then raise exception 'Invalid target_branch_id: branch does not exist';
end if;
if v_target_repo_id <> v_merge_repo_id then raise exception 'Branch and merge commit must belong to the same repository';
end if;
if v_branch_head_commit_id is null then raise exception 'Branch head is null; cannot finalize merge';
end if;
if v_parent_commit_id <> v_branch_head_commit_id then raise exception 'Merge_commit_id must be based on the current branch head';
end if;
else -- no branch context; treat parent as the target snapshot
v_branch_head_commit_id := v_parent_commit_id;
end if;
previous_target_head_commit_id := v_branch_head_commit_id;
source_commit_id := v_merged_from_commit_id;
-- Fast-forward finalize when merged_from_commit_id is NULL
if v_merged_from_commit_id is null then v_merge_base_commit_id := v_parent_commit_id;
merge_base_commit_id := v_merge_base_commit_id;
applied_file_count := 0;
if p_target_branch_id is not null then
update fs.branches
set head_commit_id = p_commit_id
where id = p_target_branch_id;
previous_target_head_commit_id := v_branch_head_commit_id;
new_target_head_commit_id := p_commit_id;
operation := 'fast_forward';
else previous_target_head_commit_id := v_branch_head_commit_id;
new_target_head_commit_id := null;
operation := 'fast_forward';
end if;
return next;
return;
end if;
v_merge_base_commit_id := fs.get_merge_base(v_branch_head_commit_id, v_merged_from_commit_id);
merge_base_commit_id := v_merge_base_commit_id;
select count(*)::int into v_conflict_count
from fs.get_conflicts(v_branch_head_commit_id, v_merged_from_commit_id);
if v_conflict_count > 0 then
select count(*)::int into v_missing_resolutions
from fs.get_conflicts(v_branch_head_commit_id, v_merged_from_commit_id) c
where not exists (
    select 1
    from fs.files f
    where f.commit_id = p_commit_id
      and f.path = c.path
  );
if v_missing_resolutions > 0 then raise exception 'Merge requires resolutions for % conflict paths; insert rows into fs.files for commit %',
v_missing_resolutions,
p_commit_id;
end if;
end if;
applied_file_count := 0;
-- Apply deletions that the user has not already written into the merge commit.
with base_snapshot as (
  select s.path,
    s.content,
    s.is_symlink
  from fs._get_commit_snapshot_with_content(v_merge_base_commit_id) s
),
target_snapshot as (
  select s.path,
    s.content,
    s.is_symlink
  from fs._get_commit_snapshot_with_content(v_branch_head_commit_id) s
),
source_snapshot as (
  select s.path,
    s.content,
    s.is_symlink
  from fs._get_commit_snapshot_with_content(v_merged_from_commit_id) s
),
user_writes as (
  select f.path
  from fs.files f
  where f.commit_id = p_commit_id
),
paths as (
  select b.path as file_path
  from base_snapshot b
  union
  select t.path as file_path
  from target_snapshot t
  union
  select s.path as file_path
  from source_snapshot s
  union
  select u.path as file_path
  from user_writes u
),
states as (
  select p.file_path,
    (b.path is not null) as base_exists,
    b.is_symlink as base_is_symlink,
    b.content as base_content,
    (t.path is not null) as target_exists,
    t.is_symlink as target_is_symlink,
    t.content as target_content,
    (s.path is not null) as source_exists,
    s.is_symlink as source_is_symlink,
    s.content as source_content
  from paths p
    left join base_snapshot b on b.path = p.file_path
    left join target_snapshot t on t.path = p.file_path
    left join source_snapshot s on s.path = p.file_path
),
diffs as (
  select st.*,
    (
      st.source_exists is distinct
      from st.base_exists
        or st.source_is_symlink is distinct
      from st.base_is_symlink
        or st.source_content is distinct
      from st.base_content
    ) as source_changed
  from states st
),
desired as (
  select d.file_path,
    case
      when d.source_changed then d.source_exists
      else d.target_exists
    end as desired_exists,
    case
      when d.source_changed then d.source_is_symlink
      else d.target_is_symlink
    end as desired_is_symlink,
    case
      when d.source_changed then d.source_content
      else d.target_content
    end as desired_content,
    d.target_exists,
    d.target_is_symlink,
    d.target_content
  from diffs d
),
patch as (
  select de.file_path,
    de.desired_exists,
    de.desired_is_symlink,
    de.desired_content,
    (
      not de.desired_exists
      and de.target_exists
    ) as need_delete,
    (
      de.desired_exists
      and (
        (not de.target_exists)
        or de.target_is_symlink is distinct
        from de.desired_is_symlink
          or de.target_content is distinct
        from de.desired_content
      )
    ) as need_write
  from desired de
)
insert into fs.files (commit_id, path, content, is_deleted, is_symlink)
select p_commit_id,
  p.file_path,
  '',
  true,
  false
from patch p
where p.need_delete
  and not exists (
    select 1
    from fs.files f
    where f.commit_id = p_commit_id
      and f.path = p.file_path
  );
get diagnostics v_rows = row_count;
applied_file_count := applied_file_count + v_rows;
-- Apply writes that the user has not already provided.
with base_snapshot as (
  select s.path,
    s.content,
    s.is_symlink
  from fs._get_commit_snapshot_with_content(v_merge_base_commit_id) s
),
target_snapshot as (
  select s.path,
    s.content,
    s.is_symlink
  from fs._get_commit_snapshot_with_content(v_branch_head_commit_id) s
),
source_snapshot as (
  select s.path,
    s.content,
    s.is_symlink
  from fs._get_commit_snapshot_with_content(v_merged_from_commit_id) s
),
user_writes as (
  select f.path
  from fs.files f
  where f.commit_id = p_commit_id
),
paths as (
  select b.path as file_path
  from base_snapshot b
  union
  select t.path as file_path
  from target_snapshot t
  union
  select s.path as file_path
  from source_snapshot s
  union
  select u.path as file_path
  from user_writes u
),
states as (
  select p.file_path,
    (b.path is not null) as base_exists,
    b.is_symlink as base_is_symlink,
    b.content as base_content,
    (t.path is not null) as target_exists,
    t.is_symlink as target_is_symlink,
    t.content as target_content,
    (s.path is not null) as source_exists,
    s.is_symlink as source_is_symlink,
    s.content as source_content
  from paths p
    left join base_snapshot b on b.path = p.file_path
    left join target_snapshot t on t.path = p.file_path
    left join source_snapshot s on s.path = p.file_path
),
diffs as (
  select st.*,
    (
      st.source_exists is distinct
      from st.base_exists
        or st.source_is_symlink is distinct
      from st.base_is_symlink
        or st.source_content is distinct
      from st.base_content
    ) as source_changed
  from states st
),
desired as (
  select d.file_path,
    case
      when d.source_changed then d.source_exists
      else d.target_exists
    end as desired_exists,
    case
      when d.source_changed then d.source_is_symlink
      else d.target_is_symlink
    end as desired_is_symlink,
    case
      when d.source_changed then d.source_content
      else d.target_content
    end as desired_content,
    d.target_exists,
    d.target_is_symlink,
    d.target_content
  from diffs d
),
patch as (
  select de.file_path,
    de.desired_exists,
    de.desired_is_symlink,
    de.desired_content,
    (
      not de.desired_exists
      and de.target_exists
    ) as need_delete,
    (
      de.desired_exists
      and (
        (not de.target_exists)
        or de.target_is_symlink is distinct
        from de.desired_is_symlink
          or de.target_content is distinct
        from de.desired_content
      )
    ) as need_write
  from desired de
)
insert into fs.files (commit_id, path, content, is_deleted, is_symlink)
select p_commit_id,
  p.file_path,
  p.desired_content,
  false,
  coalesce(p.desired_is_symlink, false)
from patch p
where p.need_write
  and not exists (
    select 1
    from fs.files f
    where f.commit_id = p_commit_id
      and f.path = p.file_path
  );
get diagnostics v_rows = row_count;
applied_file_count := applied_file_count + v_rows;
if p_target_branch_id is not null then
update fs.branches
set head_commit_id = p_commit_id
where id = p_target_branch_id;
end if;
if v_merge_base_commit_id = v_merged_from_commit_id then operation := 'already_up_to_date';
elsif v_conflict_count > 0 then operation := 'merged_with_conflicts_resolved';
else operation := 'merged';
end if;
new_target_head_commit_id := case
  when p_target_branch_id is null then null
  else p_commit_id
end;
return next;
end;
$$ language plpgsql volatile parallel unsafe;
