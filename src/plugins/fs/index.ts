import * as fsp from "node:fs/promises";
import { getSql } from "../../sql2.ts";

/**
 * A repository in the virtual filesystem
 */
export interface Repository {
  id: string;
  name: string;
  defaultBranchId: string | null;
  createdAt: Date;
}

/**
 * A branch pointer in a repository
 */
export interface Branch {
  id: string;
  repositoryId: string;
  name: string;
  headCommitId: string | null;
  createdAt: Date;
}

/**
 * A commit in the repository
 */
export interface Commit {
  id: string;
  repositoryId: string;
  parentCommitId: string | null;
  mergedFromCommitId: string | null;
  message: string;
  createdAt: Date;
}

/**
 * A file entry in a commit
 */
export interface FileEntry {
  id: string;
  commitId: string;
  path: string;
  content: string;
  isDeleted: boolean;
  isSymlink: boolean;
  createdAt: Date;
}

/**
 * File metadata in a commit delta
 */
export interface CommitDeltaEntry {
  repositoryId: string;
  repositoryName: string;
  commitId: string;
  path: string;
  isDeleted: boolean;
  isSymlink: boolean;
  fileCreatedAt: Date;
  commitCreatedAt: Date;
  commitMessage: string;
}

/**
 * File metadata in a commit snapshot
 */
export interface SnapshotEntry {
  repositoryId: string;
  repositoryName: string;
  commitId: string;
  path: string;
  isSymlink: boolean;
  commitCreatedAt: Date;
  commitMessage: string;
}

/**
 * File history entry
 */
export interface FileHistoryEntry {
  commitId: string;
  content: string | null;
  isDeleted: boolean;
  isSymlink: boolean;
  commitMessage: string;
  commitCreatedAt: Date;
}

/**
 * Conflict information for a file
 */
export interface ConflictEntry {
  mergeBaseCommitId: string;
  path: string;
  baseExists: boolean;
  baseIsSymlink: boolean;
  baseContent: string | null;
  leftExists: boolean;
  leftIsSymlink: boolean;
  leftContent: string | null;
  rightExists: boolean;
  rightIsSymlink: boolean;
  rightContent: string | null;
  conflictKind: "delete/modify" | "add/add" | "modify/modify";
}

/**
 * Result from a rebase operation
 */
export interface RebaseResult {
  operation:
    | "noop"
    | "already_up_to_date"
    | "fast_forward"
    | "rebase"
    | "conflict";
  repositoryId: string;
  branchId: string;
  ontoBranchId: string;
  mergeBaseCommitId: string | null;
  previousBranchHeadCommitId: string | null;
  ontoHeadCommitId: string | null;
  rebasedCommitId: string | null;
  newBranchHeadCommitId: string | null;
  appliedFileCount: number;
}

/**
 * Result from a finalize commit operation
 */
export interface FinalizeResult {
  operation: "fast_forward" | "merge" | "conflict";
  repositoryId: string;
  targetBranchId: string | null;
  mergeBaseCommitId: string | null;
  previousTargetHeadCommitId: string | null;
  sourceCommitId: string | null;
  mergeCommitId: string;
  newTargetHeadCommitId: string | null;
  appliedFileCount: number;
}

// ========================================
// Plugin Installation
// ========================================

/**
 * Installs the fs (filesystem) schema and helper functions.
 * Call this once before using any fs functions.
 */
export async function fsPlugin() {
  const sql = getSql({ camelize: false });

  const sqlScript = await fsp.readFile(
    new URL("./fs.sql", import.meta.url),
    "utf-8",
  );

  const strings = Object.assign([sqlScript] as ReadonlyArray<string>, {
    raw: [sqlScript],
  });

  await sql(strings).exec();
}

/**
 * Creates a new repository with a default 'main' branch.
 *
 * @param name - Unique name for the repository
 * @returns The created repository
 */
export async function createRepository(name: string): Promise<Repository> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    insert into
      fs.repositories (name)
    values
      (${name})
    returning
      id,
      name,
      default_branch_id,
      created_at
  `.first<{
    id: string;
    name: string;
    default_branch_id: string | null;
    created_at: Date;
  }>();

  return {
    id: row!.id,
    name: row!.name,
    defaultBranchId: row!.default_branch_id,
    createdAt: row!.created_at,
  };
}

/**
 * Gets a repository by name.
 *
 * @param name - Repository name
 * @returns The repository or null if not found
 */
export async function getRepository(name: string): Promise<Repository | null> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      id,
      name,
      default_branch_id,
      created_at
    from
      fs.repositories
    where
      name = ${name}
  `.first<{
    id: string;
    name: string;
    default_branch_id: string | null;
    created_at: Date;
  }>();

  if (!row) return null;

  return {
    id: row.id,
    name: row.name,
    defaultBranchId: row.default_branch_id,
    createdAt: row.created_at,
  };
}

/**
 * Gets a repository by ID.
 *
 * @param id - Repository ID (UUID)
 * @returns The repository or null if not found
 */
export async function getRepositoryById(
  id: string,
): Promise<Repository | null> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      id,
      name,
      default_branch_id,
      created_at
    from
      fs.repositories
    where
      id = ${id}::UUID
  `.first<{
    id: string;
    name: string;
    default_branch_id: string | null;
    created_at: Date;
  }>();

  if (!row) return null;

  return {
    id: row.id,
    name: row.name,
    defaultBranchId: row.default_branch_id,
    createdAt: row.created_at,
  };
}

/**
 * Lists all repositories.
 *
 * @returns Array of repositories
 */
export async function listRepositories(): Promise<Repository[]> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      id,
      name,
      default_branch_id,
      created_at
    from
      fs.repositories
    order by
      name
  `.all<{
    id: string;
    name: string;
    default_branch_id: string | null;
    created_at: Date;
  }>();

  return rows.map((row) => ({
    id: row.id,
    name: row.name,
    defaultBranchId: row.default_branch_id,
    createdAt: row.created_at,
  }));
}

/**
 * Creates a new branch in a repository.
 *
 * @param repositoryId - Repository ID
 * @param name - Branch name
 * @param headCommitId - Optional starting commit (defaults to default branch head)
 * @returns The created branch
 */
export async function createBranch(
  repositoryId: string,
  name: string,
  headCommitId?: string,
): Promise<Branch> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    insert into
      fs.branches (repository_id, name, head_commit_id)
    values
      (
        ${repositoryId}::UUID,
        ${name},
        ${headCommitId ?? null}::UUID
      )
    returning
      id,
      repository_id,
      name,
      head_commit_id,
      created_at
  `.first<{
    id: string;
    repository_id: string;
    name: string;
    head_commit_id: string | null;
    created_at: Date;
  }>();

  return {
    id: row!.id,
    repositoryId: row!.repository_id,
    name: row!.name,
    headCommitId: row!.head_commit_id,
    createdAt: row!.created_at,
  };
}

/**
 * Gets a branch by repository ID and name.
 *
 * @param repositoryId - Repository ID
 * @param name - Branch name
 * @returns The branch or null if not found
 */
export async function getBranch(
  repositoryId: string,
  name: string,
): Promise<Branch | null> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      id,
      repository_id,
      name,
      head_commit_id,
      created_at
    from
      fs.branches
    where
      repository_id = ${repositoryId}::UUID
      and name = ${name}
  `.first<{
    id: string;
    repository_id: string;
    name: string;
    head_commit_id: string | null;
    created_at: Date;
  }>();

  if (!row) return null;

  return {
    id: row.id,
    repositoryId: row.repository_id,
    name: row.name,
    headCommitId: row.head_commit_id,
    createdAt: row.created_at,
  };
}

/**
 * Gets a branch by ID.
 *
 * @param id - Branch ID (UUID)
 * @returns The branch or null if not found
 */
export async function getBranchById(id: string): Promise<Branch | null> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      id,
      repository_id,
      name,
      head_commit_id,
      created_at
    from
      fs.branches
    where
      id = ${id}::UUID
  `.first<{
    id: string;
    repository_id: string;
    name: string;
    head_commit_id: string | null;
    created_at: Date;
  }>();

  if (!row) return null;

  return {
    id: row.id,
    repositoryId: row.repository_id,
    name: row.name,
    headCommitId: row.head_commit_id,
    createdAt: row.created_at,
  };
}

/**
 * Lists all branches in a repository.
 *
 * @param repositoryId - Repository ID
 * @returns Array of branches
 */
export async function listBranches(repositoryId: string): Promise<Branch[]> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      id,
      repository_id,
      name,
      head_commit_id,
      created_at
    from
      fs.branches
    where
      repository_id = ${repositoryId}::UUID
    order by
      name
  `.all<{
    id: string;
    repository_id: string;
    name: string;
    head_commit_id: string | null;
    created_at: Date;
  }>();

  return rows.map((row) => ({
    id: row.id,
    repositoryId: row.repository_id,
    name: row.name,
    headCommitId: row.head_commit_id,
    createdAt: row.created_at,
  }));
}

/**
 * Updates a branch's head commit.
 *
 * @param branchId - Branch ID
 * @param headCommitId - New head commit ID
 */
export async function updateBranchHead(
  branchId: string,
  headCommitId: string,
): Promise<void> {
  const sql = getSql({ camelize: false });
  await sql`
    update fs.branches
    set
      head_commit_id = ${headCommitId}::UUID
    where
      id = ${branchId}::UUID
  `.query();
}
/**
 * Creates a new commit in a repository.
 *
 * @param repositoryId - Repository ID
 * @param message - Commit message
 * @param parentCommitId - Parent commit ID (null for root commit)
 * @param mergedFromCommitId - Optional merged-from commit for merge commits
 * @returns The created commit
 */
export async function createCommit(
  repositoryId: string,
  message: string,
  parentCommitId?: string | null,
  mergedFromCommitId?: string | null,
): Promise<Commit> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    insert into
      fs.commits (
        repository_id,
        message,
        parent_commit_id,
        merged_from_commit_id
      )
    values
      (
        ${repositoryId}::UUID,
        ${message},
        ${parentCommitId ?? null}::UUID,
        ${mergedFromCommitId ?? null}::UUID
      )
    returning
      id,
      repository_id,
      parent_commit_id,
      merged_from_commit_id,
      message,
      created_at
  `.first<{
    id: string;
    repository_id: string;
    parent_commit_id: string | null;
    merged_from_commit_id: string | null;
    message: string;
    created_at: Date;
  }>();

  return {
    id: row!.id,
    repositoryId: row!.repository_id,
    parentCommitId: row!.parent_commit_id,
    mergedFromCommitId: row!.merged_from_commit_id,
    message: row!.message,
    createdAt: row!.created_at,
  };
}

/**
 * Gets a commit by ID.
 *
 * @param id - Commit ID (UUID)
 * @returns The commit or null if not found
 */
export async function getCommit(id: string): Promise<Commit | null> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      id,
      repository_id,
      parent_commit_id,
      merged_from_commit_id,
      message,
      created_at
    from
      fs.commits
    where
      id = ${id}::UUID
  `.first<{
    id: string;
    repository_id: string;
    parent_commit_id: string | null;
    merged_from_commit_id: string | null;
    message: string;
    created_at: Date;
  }>();

  if (!row) return null;

  return {
    id: row.id,
    repositoryId: row.repository_id,
    parentCommitId: row.parent_commit_id,
    mergedFromCommitId: row.merged_from_commit_id,
    message: row.message,
    createdAt: row.created_at,
  };
}

/**
 * Writes a file to a commit.
 *
 * @param commitId - Commit ID
 * @param path - File path (will be normalized)
 * @param content - File content
 * @param options - Optional flags for symlink/deleted
 * @returns The created file entry
 */
export async function writeFile(
  commitId: string,
  path: string,
  content: string,
  options: { isSymlink?: boolean; isDeleted?: boolean } = {},
): Promise<FileEntry> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    insert into
      fs.files (commit_id, path, content, is_symlink, is_deleted)
    values
      (
        ${commitId}::UUID,
        ${path},
        ${content},
        ${options.isSymlink ?? false},
        ${options.isDeleted ?? false}
      )
    returning
      id,
      commit_id,
      path,
      content,
      is_deleted,
      is_symlink,
      created_at
  `.first<{
    id: string;
    commit_id: string;
    path: string;
    content: string;
    is_deleted: boolean;
    is_symlink: boolean;
    created_at: Date;
  }>();

  return {
    id: row!.id,
    commitId: row!.commit_id,
    path: row!.path,
    content: row!.content,
    isDeleted: row!.is_deleted,
    isSymlink: row!.is_symlink,
    createdAt: row!.created_at,
  };
}

/**
 * Writes multiple files to a commit.
 *
 * @param commitId - Commit ID
 * @param files - Array of files to write
 * @returns Array of created file entries
 */
export async function writeFiles(
  commitId: string,
  files: Array<{
    path: string;
    content: string;
    isSymlink?: boolean;
    isDeleted?: boolean;
  }>,
): Promise<FileEntry[]> {
  const results: FileEntry[] = [];
  for (const file of files) {
    const entry = await writeFile(commitId, file.path, file.content, {
      isSymlink: file.isSymlink,
      isDeleted: file.isDeleted,
    });
    results.push(entry);
  }
  return results;
}

/**
 * Deletes a file by writing a tombstone.
 *
 * @param commitId - Commit ID
 * @param path - File path to delete
 * @returns The tombstone file entry
 */
export async function deleteFile(
  commitId: string,
  path: string,
): Promise<FileEntry> {
  return writeFile(commitId, path, "", { isDeleted: true });
}

/**
 * Reads a file at a specific commit by resolving through ancestry.
 *
 * @param commitId - Commit ID
 * @param path - File path
 * @returns File content or null if not found/deleted
 */
export async function readFile(
  commitId: string,
  path: string,
): Promise<string | null> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      fs.read_file (
        ${commitId}::UUID,
        ${path}
      ) as content
  `.first<{ content: string | null }>();

  return row?.content ?? null;
}

/**
 * Gets the files written in a specific commit (delta).
 *
 * @param commitId - Commit ID
 * @returns Array of files written in this commit
 */
export async function getCommitDelta(
  commitId: string,
): Promise<CommitDeltaEntry[]> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      *
    from
      fs.get_commit_delta (${commitId}::UUID)
  `.all<{
    repository_id: string;
    repository_name: string;
    commit_id: string;
    path: string;
    is_deleted: boolean;
    is_symlink: boolean;
    file_created_at: Date;
    commit_created_at: Date;
    commit_message: string;
  }>();

  return rows.map((row) => ({
    repositoryId: row.repository_id,
    repositoryName: row.repository_name,
    commitId: row.commit_id,
    path: row.path,
    isDeleted: row.is_deleted,
    isSymlink: row.is_symlink,
    fileCreatedAt: row.file_created_at,
    commitCreatedAt: row.commit_created_at,
    commitMessage: row.commit_message,
  }));
}

/**
 * Gets the resolved file tree at a commit.
 *
 * @param commitId - Commit ID
 * @param pathPrefix - Optional prefix to filter paths
 * @returns Array of files in the snapshot
 */
export async function getCommitSnapshot(
  commitId: string,
  pathPrefix?: string,
): Promise<SnapshotEntry[]> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      *
    from
      fs.get_commit_snapshot (
        ${commitId}::UUID,
        ${pathPrefix ?? null}
      )
  `.all<{
    repository_id: string;
    repository_name: string;
    commit_id: string;
    path: string;
    is_symlink: boolean;
    commit_created_at: Date;
    commit_message: string;
  }>();

  return rows.map((row) => ({
    repositoryId: row.repository_id,
    repositoryName: row.repository_name,
    commitId: row.commit_id,
    path: row.path,
    isSymlink: row.is_symlink,
    commitCreatedAt: row.commit_created_at,
    commitMessage: row.commit_message,
  }));
}

/**
 * Gets the history of changes to a file.
 *
 * @param commitId - Commit ID to start from
 * @param path - File path
 * @returns Array of historical file states
 */
export async function getFileHistory(
  commitId: string,
  path: string,
): Promise<FileHistoryEntry[]> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      *
    from
      fs.get_file_history (
        ${commitId}::UUID,
        ${path}
      )
  `.all<{
    commit_id: string;
    content: string | null;
    is_deleted: boolean;
    is_symlink: boolean;
    commit_message: string;
    commit_created_at: Date;
  }>();

  return rows.map((row) => ({
    commitId: row.commit_id,
    content: row.content,
    isDeleted: row.is_deleted,
    isSymlink: row.is_symlink,
    commitMessage: row.commit_message,
    commitCreatedAt: row.commit_created_at,
  }));
}

// ========================================
// Merge & Rebase Operations
// ========================================

/**
 * Finds the merge base (common ancestor) of two commits.
 *
 * @param leftCommitId - First commit ID
 * @param rightCommitId - Second commit ID
 * @returns The merge base commit ID
 */
export async function getMergeBase(
  leftCommitId: string,
  rightCommitId: string,
): Promise<string> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      fs.get_merge_base (
        ${leftCommitId}::UUID,
        ${rightCommitId}::UUID
      ) as merge_base
  `.first<{ merge_base: string }>();

  return row!.merge_base;
}

/**
 * Detects file-level conflicts between two commits.
 *
 * @param leftCommitId - First commit ID
 * @param rightCommitId - Second commit ID
 * @returns Array of conflicts (empty if no conflicts)
 */
export async function getConflicts(
  leftCommitId: string,
  rightCommitId: string,
): Promise<ConflictEntry[]> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      *
    from
      fs.get_conflicts (
        ${leftCommitId}::UUID,
        ${rightCommitId}::UUID
      )
  `.all<{
    merge_base_commit_id: string;
    path: string;
    base_exists: boolean;
    base_is_symlink: boolean;
    base_content: string | null;
    left_exists: boolean;
    left_is_symlink: boolean;
    left_content: string | null;
    right_exists: boolean;
    right_is_symlink: boolean;
    right_content: string | null;
    conflict_kind: string;
  }>();

  return rows.map((row) => ({
    mergeBaseCommitId: row.merge_base_commit_id,
    path: row.path,
    baseExists: row.base_exists,
    baseIsSymlink: row.base_is_symlink,
    baseContent: row.base_content,
    leftExists: row.left_exists,
    leftIsSymlink: row.left_is_symlink,
    leftContent: row.left_content,
    rightExists: row.right_exists,
    rightIsSymlink: row.right_is_symlink,
    rightContent: row.right_content,
    conflictKind: row.conflict_kind as ConflictEntry["conflictKind"],
  }));
}

/**
 * Rebases a branch onto another branch.
 *
 * @param branchId - Branch to rebase
 * @param ontoBranchId - Target branch to rebase onto
 * @param message - Optional message for the rebased commit
 * @returns Result of the rebase operation
 */
export async function rebaseBranch(
  branchId: string,
  ontoBranchId: string,
  message?: string,
): Promise<RebaseResult> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      *
    from
      fs.rebase_branch (
        ${branchId}::UUID,
        ${ontoBranchId}::UUID,
        ${message ?? null}
      )
  `.first<{
    operation: string;
    repository_id: string;
    branch_id: string;
    onto_branch_id: string;
    merge_base_commit_id: string | null;
    previous_branch_head_commit_id: string | null;
    onto_head_commit_id: string | null;
    rebased_commit_id: string | null;
    new_branch_head_commit_id: string | null;
    applied_file_count: number;
  }>();

  return {
    operation: row!.operation as RebaseResult["operation"],
    repositoryId: row!.repository_id,
    branchId: row!.branch_id,
    ontoBranchId: row!.onto_branch_id,
    mergeBaseCommitId: row!.merge_base_commit_id,
    previousBranchHeadCommitId: row!.previous_branch_head_commit_id,
    ontoHeadCommitId: row!.onto_head_commit_id,
    rebasedCommitId: row!.rebased_commit_id,
    newBranchHeadCommitId: row!.new_branch_head_commit_id,
    appliedFileCount: row!.applied_file_count,
  };
}

/**
 * Finalizes a merge commit and optionally advances a branch.
 *
 * @param commitId - The merge commit ID
 * @param targetBranchId - Optional branch to advance
 * @returns Result of the finalize operation
 */
export async function finalizeCommit(
  commitId: string,
  targetBranchId?: string,
): Promise<FinalizeResult> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      *
    from
      fs.finalize_commit (
        ${commitId}::UUID,
        ${targetBranchId ?? null}::UUID
      )
  `.first<{
    operation: string;
    repository_id: string;
    target_branch_id: string | null;
    merge_base_commit_id: string | null;
    previous_target_head_commit_id: string | null;
    source_commit_id: string | null;
    merge_commit_id: string;
    new_target_head_commit_id: string | null;
    applied_file_count: number;
  }>();

  return {
    operation: row!.operation as FinalizeResult["operation"],
    repositoryId: row!.repository_id,
    targetBranchId: row!.target_branch_id,
    mergeBaseCommitId: row!.merge_base_commit_id,
    previousTargetHeadCommitId: row!.previous_target_head_commit_id,
    sourceCommitId: row!.source_commit_id,
    mergeCommitId: row!.merge_commit_id,
    newTargetHeadCommitId: row!.new_target_head_commit_id,
    appliedFileCount: row!.applied_file_count,
  };
}

/**
 * Creates a commit with files and advances the branch in one operation.
 * This is a convenience wrapper for the common workflow.
 *
 * @param branchId - Branch to commit to
 * @param message - Commit message
 * @param files - Files to write in this commit
 * @returns The created commit and updated branch
 */
export async function commitToBranch(
  branchId: string,
  message: string,
  files: Array<{
    path: string;
    content: string;
    isSymlink?: boolean;
    isDeleted?: boolean;
  }>,
): Promise<{ commit: Commit; branch: Branch }> {
  // Get current branch state
  const branch = await getBranchById(branchId);
  if (!branch) {
    throw new Error(`Branch ${branchId} not found`);
  }

  // Create commit
  const commit = await createCommit(
    branch.repositoryId,
    message,
    branch.headCommitId,
  );

  // Write files
  await writeFiles(commit.id, files);

  // Advance branch head
  await updateBranchHead(branchId, commit.id);

  // Return updated state
  const updatedBranch = await getBranchById(branchId);

  return { commit, branch: updatedBranch! };
}

/**
 * Initializes a repository with an initial commit containing files.
 *
 * @param repoName - Repository name
 * @param files - Initial files
 * @param commitMessage - Initial commit message
 * @returns The created repository, branch, and commit
 */
export async function initRepository(
  repoName: string,
  files: Array<{ path: string; content: string }>,
  commitMessage: string = "Initial commit",
): Promise<{ repository: Repository; branch: Branch; commit: Commit }> {
  // Create repository
  const repository = await createRepository(repoName);

  // Get the default branch (created automatically)
  const branches = await listBranches(repository.id);
  let branch = branches.find((b) => b.name === "main") || branches[0];

  // If no branch exists, create main branch
  if (!branch) {
    branch = await createBranch(repository.id, "main");
  }

  // Create initial commit
  const commit = await createCommit(
    repository.id,
    commitMessage,
    null, // root commit
  );

  // Write files
  await writeFiles(
    commit.id,
    files.map((f) => ({ ...f, isSymlink: false, isDeleted: false })),
  );

  // Advance branch head
  await updateBranchHead(branch.id, commit.id);

  // Get updated state
  const updatedRepo = await getRepositoryById(repository.id);
  const updatedBranch = await getBranchById(branch.id);

  return {
    repository: updatedRepo!,
    branch: updatedBranch!,
    commit,
  };
}
