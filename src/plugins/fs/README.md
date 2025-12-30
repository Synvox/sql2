# FS Plugin (Virtual Filesystem with Version Control)

A complete virtual filesystem with Git-like version control implemented in PostgreSQL. Built for AI pair-coding tools where repositories act as virtual drives with full version control capabilities.

## Features

- **Git-like Version Control**: Commits, branches, and file history
- **Automatic Branch Management**: Branches automatically track their head commits
- **File Versioning**: Immutable file versions with cascading reads
- **Path Normalization**: Automatic path cleaning and validation
- **Cross-platform Compatibility**: Rejects invalid paths for Windows/Unix filesystems
- **TypeScript API**: Clean helper functions for all operations
- **ACID Transactions**: Full PostgreSQL transactional guarantees

## Installation

```typescript
import { fsPlugin } from "sql2/fs";

// Install filesystem schema (run once)
await fsPlugin();
```

## Quick Start

```typescript
import {
  initRepository,
  commitToBranch,
  readFile,
  getCommitSnapshot,
} from "sql2/fs";

// Initialize a repository with files
const { repository, branch, commit } = await initRepository(
  "my-project",
  [
    { path: "/README.md", content: "# My Project\n" },
    { path: "/src/index.ts", content: "console.log('Hello');" },
  ],
  "Initial commit",
);

// Make more commits
const { commit: newCommit } = await commitToBranch(
  branch.id,
  "Add utilities",
  [{ path: "/src/utils.ts", content: "export const ok = true;" }],
);

// Read a file
const content = await readFile(newCommit.id, "/src/utils.ts");
console.log(content); // "export const ok = true;"

// Get all files at a commit
const files = await getCommitSnapshot(newCommit.id);
// [{ path: "/README.md", ... }, { path: "/src/index.ts", ... }, { path: "/src/utils.ts", ... }]
```

## Core Concepts

### Repositories

Repositories are containers for branches and commits. Creating a repository automatically creates a 'main' branch.

```typescript
import {
  createRepository,
  getRepository,
  listRepositories,
} from "sql2/fs";

// Create a repository
const repo = await createRepository("my-app");

// Get by name
const repo = await getRepository("my-app");

// List all
const repos = await listRepositories();
```

### Branches

Branches point to head commits and track the latest state of a line of development.

```typescript
import {
  createBranch,
  getBranch,
  listBranches,
  updateBranchHead,
} from "sql2/fs";

// Create a feature branch (starts at default branch head)
const branch = await createBranch(repo.id, "feature/dark-mode");

// Create from a specific commit
const branch = await createBranch(repo.id, "hotfix", commitId);

// Get a branch
const main = await getBranch(repo.id, "main");

// List all branches in a repo
const branches = await listBranches(repo.id);

// Update branch head
await updateBranchHead(branch.id, newCommitId);
```

### Commits

Commits are immutable snapshots of repository state. Each commit (except the root) has a parent.

```typescript
import { createCommit, getCommit } from "sql2/fs";

// Create a commit
const commit = await createCommit(
  repo.id,
  "Add feature X",
  parentCommitId, // null for root commit
);

// Get a commit by ID
const commit = await getCommit(commitId);
```

### Files

Files are versioned content attached to commits. Use path normalization and validation automatically.

```typescript
import {
  writeFile,
  writeFiles,
  readFile,
  deleteFile,
} from "sql2/fs";

// Write a single file
await writeFile(commitId, "/src/app.ts", "const app = {};");

// Write multiple files
await writeFiles(commitId, [
  { path: "/src/a.ts", content: "export const a = 1;" },
  { path: "/src/b.ts", content: "export const b = 2;" },
]);

// Read a file (resolves through ancestry)
const content = await readFile(commitId, "/src/app.ts");

// Delete a file (creates a tombstone)
await deleteFile(commitId, "/src/old.ts");

// Create a symlink
await writeFile(commitId, "/link.ts", "/src/app.ts", { isSymlink: true });
```

## Convenience Functions

### `initRepository(name, files, message?)`

Creates a repository with an initial commit containing files.

```typescript
const { repository, branch, commit } = await initRepository(
  "my-project",
  [
    { path: "/README.md", content: "# Project" },
    { path: "/package.json", content: '{"name": "project"}' },
  ],
  "Initial commit",
);
```

### `commitToBranch(branchId, message, files)`

Creates a commit with files and advances the branch in one operation.

```typescript
const { commit, branch } = await commitToBranch(branch.id, "Update config", [
  { path: "/config.json", content: '{"debug": false}' },
]);
```

## Reading Repository State

### Get Commit Delta

Get files written in a specific commit (not inherited files):

```typescript
import { getCommitDelta } from "sql2/fs";

const delta = await getCommitDelta(commitId);
// [{ path, isDeleted, isSymlink, commitMessage, ... }]
```

### Get Commit Snapshot

Get the complete file tree at a commit (includes inherited files):

```typescript
import { getCommitSnapshot } from "sql2/fs";

const snapshot = await getCommitSnapshot(commitId);
// [{ path, isSymlink, commitMessage, ... }]

// Filter by path prefix
const srcFiles = await getCommitSnapshot(commitId, "/src");
```

### Get File History

Get the change history of a specific file:

```typescript
import { getFileHistory } from "sql2/fs";

const history = await getFileHistory(commitId, "/src/app.ts");
// [{ commitId, content, isDeleted, commitMessage, commitCreatedAt }]
```

## Merge & Rebase Operations

### Find Merge Base

Find the common ancestor of two commits:

```typescript
import { getMergeBase } from "sql2/fs";

const baseCommitId = await getMergeBase(commit1Id, commit2Id);
```

### Detect Conflicts

Check for file-level conflicts between two commits:

```typescript
import { getConflicts } from "sql2/fs";

const conflicts = await getConflicts(targetHead, sourceHead);
// [{ path, conflictKind, baseContent, leftContent, rightContent, ... }]

if (conflicts.length > 0) {
  // Handle conflicts before merging
}
```

### Rebase Branch

Replay a branch's changes onto another branch:

```typescript
import { rebaseBranch } from "sql2/fs";

const result = await rebaseBranch(featureBranchId, mainBranchId, "Rebase message");
// { operation, appliedFileCount, newBranchHeadCommitId, ... }
```

### Finalize Commit

Apply a merge commit and advance a branch:

```typescript
import { createCommit, writeFile, finalizeCommit } from "sql2/fs";

// Create merge commit
const mergeCommit = await createCommit(
  repoId,
  "Merge feature into main",
  targetHead, // parent
  sourceHead, // merged from
);

// Resolve any conflicts by writing files
await writeFile(mergeCommit.id, "/conflicted.txt", "resolved content");

// Apply changes and advance branch
const result = await finalizeCommit(mergeCommit.id, targetBranchId);
// { operation, appliedFileCount, newTargetHeadCommitId, ... }
```

## API Reference

### Repository Functions

| Function                   | Description                 |
| -------------------------- | --------------------------- |
| `fsPlugin()`               | Install the filesystem schema |
| `createRepository(name)`   | Create a new repository     |
| `getRepository(name)`      | Get repository by name      |
| `getRepositoryById(id)`    | Get repository by ID        |
| `listRepositories()`       | List all repositories       |

### Branch Functions

| Function                            | Description                    |
| ----------------------------------- | ------------------------------ |
| `createBranch(repoId, name, head?)` | Create a branch                |
| `getBranch(repoId, name)`           | Get branch by repo and name    |
| `getBranchById(id)`                 | Get branch by ID               |
| `listBranches(repoId)`              | List branches in a repository  |
| `updateBranchHead(id, commitId)`    | Update branch head commit      |

### Commit Functions

| Function                                          | Description                     |
| ------------------------------------------------- | ------------------------------- |
| `createCommit(repoId, message, parent?, merged?)` | Create a commit                 |
| `getCommit(id)`                                   | Get commit by ID                |
| `getCommitDelta(id)`                              | Get files in this commit only   |
| `getCommitSnapshot(id, prefix?)`                  | Get full file tree at commit    |

### File Functions

| Function                              | Description                         |
| ------------------------------------- | ----------------------------------- |
| `writeFile(commitId, path, content)`  | Write a file to a commit            |
| `writeFiles(commitId, files)`         | Write multiple files                |
| `readFile(commitId, path)`            | Read file content                   |
| `deleteFile(commitId, path)`          | Delete a file (tombstone)           |
| `getFileHistory(commitId, path)`      | Get file change history             |

### Merge Functions

| Function                                  | Description                        |
| ----------------------------------------- | ---------------------------------- |
| `getMergeBase(left, right)`               | Find common ancestor               |
| `getConflicts(left, right)`               | Detect file conflicts              |
| `rebaseBranch(branchId, ontoId, msg?)`    | Rebase a branch                    |
| `finalizeCommit(commitId, branchId?)`     | Apply merge and advance branch     |

### Convenience Functions

| Function                                   | Description                              |
| ------------------------------------------ | ---------------------------------------- |
| `initRepository(name, files, message?)`    | Create repo with initial commit          |
| `commitToBranch(branchId, message, files)` | Commit files and advance branch          |

## Path Validation

All file paths are automatically validated and normalized:

- **Invalid on Windows**: `< > : " | ? *`
- **Invalid on Unix**: null bytes, control characters
- **Normalization**: absolute paths, remove duplicate slashes, trim trailing slashes
- **Length limit**: 4096 characters max

## Architecture

- **Pure PostgreSQL**: No external dependencies
- **ACID Compliant**: Full transactional guarantees
- **Immutable Files**: File versions never change
- **Cascading Reads**: Parent commit lookup for missing files
- **Branch Head Tracking**: Automatic branch pointer updates
