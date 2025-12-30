import {
  PGlite,
  type PGliteInterface,
  type Transaction,
} from "@electric-sql/pglite";
import * as assert from "node:assert";
import { describe, it } from "node:test";
import { getSql, provideClient, type Client } from "../../sql2.ts";
import { fsPlugin } from "./index.ts";

const dbRoot = new PGlite();

function makeClient(db: PGlite | Transaction | PGliteInterface): Client {
  return {
    exec: async (query) => {
      await db.exec(query);
    },
    query: async (query, values) => {
      return await db.query(query, values);
    },
    transaction: async (fn) => {
      if ("transaction" in db) {
        return await db.transaction(async (trx) => {
          return await fn(makeClient(trx));
        });
      } else {
        return await fn(makeClient(db));
      }
    },
  };
}

await provideClient(makeClient(dbRoot), async () => {
  await fsPlugin();
});

async function withDb(fn: () => Promise<void>) {
  const db = await dbRoot.clone();
  return provideClient(makeClient(db), async () => {
    return await fn();
  });
}

function itWithDb(name: string, fn: () => Promise<void>) {
  it(name, async () => {
    await withDb(async () => {
      await fn();
    });
  });
}

describe("SQL Filesystem with Version Control", () => {
  describe("Basic Operations", () => {
    itWithDb("should create a repository", async () => {
      const sql = getSql({ camelize: false });
      const repoResult = await sql`
        insert into
          fs.repositories (name)
        values
          ('test-repo')
        returning
          *
      `.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>();
      const repoId = repoResult?.rows[0].id;

      const result = await sql`
        select
          name
        from
          fs.repositories
        where
          id = ${repoId}
      `.query<{
        name: string;
      }>();

      assert.strictEqual(result?.rows.length, 1);
      assert.strictEqual(result?.rows[0].name, "test-repo");
    });

    itWithDb("should create fs.commits", async () => {
      const sql = getSql({ camelize: false });
      // Create repository
      const repoResult = await sql`
        insert into
          fs.repositories (name)
        values
          ('test-repo')
        returning
          *
      `.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>();
      const repoId = repoResult?.rows[0].id;

      // Create additional commit
      await sql`
        insert into
          fs.commits (repository_id, parent_commit_id, message)
        values
          (
            ${repoId},
            (
              select
                head_commit_id
              from
                fs.branches
              where
                repository_id = ${repoId}
                and name = 'main'
            ),
            'Additional commit'
          )
      `.query();

      const commitResult = await sql`
        select
          message
        from
          fs.commits
        where
          message = 'Additional commit'
      `.query<{
        message: string;
      }>();

      assert.strictEqual(commitResult?.rows.length, 1);
      assert.strictEqual(commitResult?.rows[0].message, "Additional commit");
    });

    itWithDb("should write and read files", async () => {
      const sql = getSql({ camelize: false });
      // Create repository
      const repoResult = await sql`
        insert into
          fs.repositories (name)
        values
          ('test-repo')
        returning
          *
      `.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>();
      const repoId = repoResult?.rows[0].id;

      await sql`
        insert into
          fs.commits (repository_id, parent_commit_id, message)
        values
          (
            ${repoId},
            null,
            'Initial commit'
          )
      `.query();

      const commitResult = await sql`
        select
          id
        from
          fs.commits
        where
          message = 'Initial commit'
      `.query<{
        id: string;
      }>();
      const commitId = commitResult?.rows[0].id;

      // Write a file
      await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${commitId},
            '/test.txt',
            'Hello World'
          )
      `.query();

      // Read the file
      const fileResult = await sql`
        select
          fs.read_file (${commitId}, '/test.txt') as content
      `.query<{
        content: string;
      }>();

      assert.strictEqual(fileResult?.rows[0].content, "Hello World");
    });
  });

  describe("Version Control", () => {
    itWithDb("should cascade file reads through commit history", async () => {
      const sql = getSql({ camelize: false });
      // Create repository
      const repoResult = await sql`
        insert into
          fs.repositories (name)
        values
          ('version-test')
        returning
          *
      `.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>();
      const repoId = repoResult!.rows[0].id;

      // Create first commit
      await sql`
        insert into
          fs.commits (repository_id, parent_commit_id, message)
        values
          (
            ${repoId},
            null,
            'Commit 1'
          )
      `.query();

      const commit1Result = await sql`
        select
          id
        from
          fs.commits
        where
          message = 'Commit 1'
      `.query<{
        id: string;
      }>();
      const commit1Id = commit1Result?.rows[0].id!;

      // Create second commit
      await sql`
        insert into
          fs.commits (repository_id, parent_commit_id, message)
        values
          (
            ${repoId},
            ${commit1Id},
            'Commit 2'
          )
      `.query();

      const commit2Result = await sql`
        select
          id
        from
          fs.commits
        where
          message = 'Commit 2'
      `.query<{
        id: string;
      }>();
      const commit2Id = commit2Result?.rows[0].id!;

      // Write file in commit 1
      await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${commit1Id},
            '/persistent.txt',
            'Version 1'
          )
      `.query();

      // File should be readable from both fs.commits
      const result1 = await sql`
        select
          fs.read_file (
            ${commit1Id},
            '/persistent.txt'
          ) as content
      `.query<{
        content: string;
      }>();

      const result2 = await sql`
        select
          fs.read_file (
            ${commit2Id},
            '/persistent.txt'
          ) as content
      `.query<{
        content: string;
      }>();

      assert.strictEqual(result1?.rows[0].content, "Version 1");
      assert.strictEqual(result2?.rows[0].content, "Version 1");
    });

    itWithDb("should override files in newer fs.commits", async () => {
      const sql = getSql({ camelize: false });
      // Create repository
      const repoResult = await sql`
        insert into
          fs.repositories (name)
        values
          ('version-test')
        returning
          *
      `.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>();
      const repoId = repoResult!.rows[0].id;

      // Create first commit
      await sql`
        insert into
          fs.commits (repository_id, parent_commit_id, message)
        values
          (
            ${repoId},
            null,
            'Commit 1'
          )
      `.query();

      const commit1Result = await sql`
        select
          id
        from
          fs.commits
        where
          message = 'Commit 1'
      `.query<{
        id: string;
      }>();
      const commit1Id = commit1Result?.rows[0].id!;

      // Create second commit
      await sql`
        insert into
          fs.commits (repository_id, parent_commit_id, message)
        values
          (
            ${repoId},
            ${commit1Id},
            'Commit 2'
          )
      `.query();

      const commit2Result = await sql`
        select
          id
        from
          fs.commits
        where
          message = 'Commit 2'
      `.query<{
        id: string;
      }>();
      const commit2Id = commit2Result?.rows[0].id!;

      // Write file in commit 1
      await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${commit1Id},
            '/changing.txt',
            'Version 1'
          )
      `.query();

      // Override in commit 2
      await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${commit2Id},
            '/changing.txt',
            'Version 2'
          )
      `.query();

      // Check versions
      const result1 = await sql`
        select
          fs.read_file (
            ${commit1Id},
            '/changing.txt'
          ) as content
      `.query<{
        content: string;
      }>();

      const result2 = await sql`
        select
          fs.read_file (
            ${commit2Id},
            '/changing.txt'
          ) as content
      `.query<{
        content: string;
      }>();

      assert.strictEqual(result1?.rows[0].content, "Version 1");
      assert.strictEqual(result2?.rows[0].content, "Version 2");
    });

    itWithDb("should list files from commit and ancestors", async () => {
      const sql = getSql({ camelize: false });
      // Create repository
      const repoResult = await sql`
        insert into
          fs.repositories (name)
        values
          ('version-test')
        returning
          *
      `.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>();
      const repoId = repoResult!.rows[0].id;

      // Create first commit
      await sql`
        insert into
          fs.commits (repository_id, parent_commit_id, message)
        values
          (
            ${repoId},
            null,
            'Commit 1'
          )
      `.query();

      const commit1Result = await sql`
        select
          id
        from
          fs.commits
        where
          message = 'Commit 1'
      `.query<{
        id: string;
      }>();
      const commit1Id = commit1Result?.rows[0].id!;

      // Create second commit
      await sql`
        insert into
          fs.commits (repository_id, parent_commit_id, message)
        values
          (
            ${repoId},
            ${commit1Id},
            'Commit 2'
          )
      `.query();

      const commit2Result = await sql`
        select
          id
        from
          fs.commits
        where
          message = 'Commit 2'
      `.query<{
        id: string;
      }>();
      const commit2Id = commit2Result?.rows[0].id!;

      // Files in commit 1
      await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${commit1Id},
            '/file1.txt',
            'Content 1'
          ),
          (
            ${commit1Id},
            '/file2.txt',
            'Content 2'
          )
      `.query();

      // File in commit 2
      await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${commit2Id},
            '/file3.txt',
            'Content 3'
          )
      `.query();

      const result = await sql`
        select
          path
        from
          fs.get_commit_snapshot (${commit2Id})
        order by
          path
      `.query<{
        path: string;
      }>();

      assert.strictEqual(result?.rows.length, 3);
      assert.strictEqual(result?.rows[0].path, "/file1.txt");
      assert.strictEqual(result?.rows[1].path, "/file2.txt");
      assert.strictEqual(result?.rows[2].path, "/file3.txt");
    });

    itWithDb("should get file history", async () => {
      const sql = getSql({ camelize: false });
      // Create repository
      const repoResult = await sql`
        insert into
          fs.repositories (name)
        values
          ('version-test')
        returning
          *
      `.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>();
      const repoId = repoResult!.rows[0].id;

      // Create first commit
      await sql`
        insert into
          fs.commits (repository_id, parent_commit_id, message)
        values
          (
            ${repoId},
            null,
            'Commit 1'
          )
      `.query();

      const commit1Result = await sql`
        select
          id
        from
          fs.commits
        where
          message = 'Commit 1'
      `.query<{
        id: string;
      }>();
      const commit1Id = commit1Result?.rows[0].id!;

      // Create second commit
      await sql`
        insert into
          fs.commits (repository_id, parent_commit_id, message)
        values
          (
            ${repoId},
            ${commit1Id},
            'Commit 2'
          )
      `.query();

      const commit2Result = await sql`
        select
          id
        from
          fs.commits
        where
          message = 'Commit 2'
      `.query<{
        id: string;
      }>();
      const commit2Id = commit2Result?.rows[0].id!;

      // Version 1 in commit 1
      await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${commit1Id},
            '/history.txt',
            'Version 1'
          )
      `.query();

      // Version 2 in commit 2
      await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${commit2Id},
            '/history.txt',
            'Version 2'
          )
      `.query();

      const result = await sql`
        select
          commit_id,
          content,
          is_deleted,
          is_symlink
        from
          fs.get_file_history (
            ${commit2Id},
            '/history.txt'
          )
        order by
          content
      `.query<{
        commit_id: string;
        content: string;
        is_deleted: boolean;
        is_symlink: boolean;
      }>();

      assert.strictEqual(result?.rows.length, 2);
      assert.strictEqual(result?.rows[0].content, "Version 1");
      assert.strictEqual(result?.rows[1].content, "Version 2");
      assert.strictEqual(result?.rows[0].is_deleted, false);
      assert.strictEqual(result?.rows[0].is_symlink, false);
      assert.strictEqual(result?.rows[1].is_deleted, false);
      assert.strictEqual(result?.rows[1].is_symlink, false);
    });
  });

  describe("Repository and Branch Management", () => {
    itWithDb(
      "should create fs.repositories with default fs.branches",
      async () => {
        const sql = getSql({ camelize: false });
        const repoResult = await sql`
          insert into
            fs.repositories (name)
          values
            ('test-repo')
          returning
            *
        `.query<{
          id: string;
          name: string;
          default_branch_id: string;
          created_at: string;
        }>();
        const repoId = repoResult?.rows[0].id;

        // Verify repository was created
        const repoCheck = await sql`
          select
            name,
            default_branch_id
          from
            fs.repositories
          where
            id = ${repoId}
        `.query<{
          name: string;
          default_branch_id: string;
        }>();
        assert.strictEqual(repoCheck?.rows[0].name, "test-repo");
        assert.ok(repoCheck?.rows[0].default_branch_id);

        // Verify default branch was created
        const branchCheck = await sql`
          select
            name,
            repository_id,
            head_commit_id
          from
            fs.branches
          where
            repository_id = ${repoId}
        `.query<{
          name: string;
          repository_id: string;
          head_commit_id: string | null;
        }>();
        assert.strictEqual(branchCheck?.rows.length, 1);
        assert.strictEqual(branchCheck?.rows[0].name, "main");
        assert.strictEqual(branchCheck?.rows[0].repository_id, repoId);
        assert.strictEqual(branchCheck?.rows[0].head_commit_id, null); // No initial commit by default

        // Verify no initial commit was created
        const commitCheck = await sql`
          select
            repository_id,
            message,
            parent_commit_id
          from
            fs.commits
          where
            repository_id = ${repoId}
        `.query<{
          repository_id: string;
          message: string;
          parent_commit_id: string | null;
        }>();
        assert.strictEqual(commitCheck?.rows.length, 0);
      },
    );

    itWithDb("should create additional fs.branches", async () => {
      const sql = getSql({ camelize: false });
      // Create repository first
      const repoResult = await sql`
        insert into
          fs.repositories (name)
        values
          ('branch-test')
        returning
          *
      `.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>();
      const repoId = repoResult!.rows[0].id;

      // Get initial commit and branch count
      const initialBranchCount = await sql`
        select
          COUNT(*) as count
        from
          fs.branches
        where
          repository_id = ${repoId}
      `.query<{
        count: number;
      }>();
      const initialCommitCount = await sql`
        select
          COUNT(*) as count
        from
          fs.commits
        where
          repository_id = ${repoId}
      `.query<{
        count: number;
      }>();

      // Create additional branch
      const branchResult = await sql`
        insert into
          fs.branches (repository_id, name)
        values
          (
            ${repoId},
            'feature-branch'
          )
        returning
          *
      `.query<{
        id: string;
        name: string;
        head_commit_id: string;
        created_at: string;
      }>();
      const branchId = branchResult!.rows[0].id;

      // Verify branch was created
      const branchCheck = await sql`
        select
          name,
          repository_id,
          head_commit_id
        from
          fs.branches
        where
          id = ${branchId}
      `.query<{
        name: string;
        repository_id: string;
        head_commit_id: string | null;
      }>();
      assert.strictEqual(branchCheck?.rows[0].name, "feature-branch");
      assert.strictEqual(branchCheck?.rows[0].repository_id, repoId);
      // By default, new branches start from the repository default branch head
      const mainHead = await sql`
        select
          head_commit_id
        from
          fs.branches
        where
          repository_id = ${repoId}
          and name = 'main'
      `.query<{
        head_commit_id: string | null;
      }>();
      assert.strictEqual(
        branchCheck?.rows[0].head_commit_id,
        mainHead?.rows[0].head_commit_id,
      );

      // Verify repository now has 2 fs.branches
      const allBranches = await sql`
        select
          COUNT(*) as count
        from
          fs.branches
        where
          repository_id = ${repoId}
      `.query<{
        count: number;
      }>();
      assert.strictEqual(
        allBranches?.rows[0].count,
        initialBranchCount!.rows[0].count + 1,
      );

      // Creating a branch should not create a new commit (branches point at existing commits)
      const allCommits = await sql`
        select
          COUNT(*) as count
        from
          fs.commits
        where
          repository_id = ${repoId}
      `.query<{
        count: number;
      }>();
      assert.strictEqual(
        allCommits?.rows[0].count,
        initialCommitCount!.rows[0].count,
      );
    });
  });

  describe("Path Normalization", () => {
    itWithDb("should normalize absolute paths", async () => {
      const sql = getSql({ camelize: false });
      // Create repository for each test
      const repoResult = await sql`
        insert into
          fs.repositories (name)
        values
          ('path-test')
        returning
          *
      `.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>();
      const repoId = repoResult!.rows[0].id;

      // Create a commit for file operations
      const commitResult = await sql`
        insert into
          fs.commits (repository_id, message)
        values
          (${repoId}, 'Test commit')
        returning
          id
      `.query<{
        id: string;
      }>();
      const commitId = commitResult!.rows[0].id;

      // Manually update branch head
      await sql`
        update fs.branches
        set
          head_commit_id = ${commitId}
        where
          repository_id = ${repoId}
          and name = 'main'
      `.query();

      const insertResult = await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${commitId},
            '/src/main.ts',
            'content'
          )
        returning
          *
      `.query<{
        id: string;
        commit_id: string;
        path: string;
        content: string;
        created_at: string;
      }>();

      assert.strictEqual(insertResult?.rows[0].path, "/src/main.ts");
    });

    itWithDb("should normalize relative paths to absolute", async () => {
      const sql = getSql({ camelize: false });
      // Create repository for each test
      const repoResult = await sql`
        insert into
          fs.repositories (name)
        values
          ('path-test')
        returning
          *
      `.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>();
      const repoId = repoResult!.rows[0].id;

      // Create a commit for file operations
      const commitResult = await sql`
        insert into
          fs.commits (repository_id, message)
        values
          (${repoId}, 'Test commit')
        returning
          id
      `.query<{
        id: string;
      }>();
      const commitId = commitResult!.rows[0].id;

      // Manually update branch head
      await sql`
        update fs.branches
        set
          head_commit_id = ${commitId}
        where
          repository_id = ${repoId}
          and name = 'main'
      `.query();

      const insertResult = await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${commitId},
            'src/main.ts',
            'content'
          )
        returning
          *
      `.query<{
        id: string;
        commit_id: string;
        path: string;
        content: string;
        created_at: string;
      }>();

      assert.strictEqual(insertResult?.rows[0].path, "/src/main.ts");
    });

    itWithDb("should remove duplicate slashes", async () => {
      const sql = getSql({ camelize: false });
      // Create repository for each test
      const repoResult = await sql`
        insert into
          fs.repositories (name)
        values
          ('path-test')
        returning
          *
      `.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>();
      const repoId = repoResult!.rows[0].id;

      // Create a commit for file operations
      const commitResult = await sql`
        insert into
          fs.commits (repository_id, message)
        values
          (${repoId}, 'Test commit')
        returning
          id
      `.query<{
        id: string;
      }>();
      const commitId = commitResult!.rows[0].id;

      // Manually update branch head
      await sql`
        update fs.branches
        set
          head_commit_id = ${commitId}
        where
          repository_id = ${repoId}
          and name = 'main'
      `.query();

      const insertResult = await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${commitId},
            '//src//main.ts',
            'content'
          )
        returning
          *
      `.query<{
        id: string;
        commit_id: string;
        path: string;
        content: string;
        created_at: string;
      }>();

      assert.strictEqual(insertResult?.rows[0].path, "/src/main.ts");
    });

    itWithDb("should remove trailing slashes", async () => {
      const sql = getSql({ camelize: false });
      // Create repository for each test
      const repoResult = await sql`
        insert into
          fs.repositories (name)
        values
          ('path-test')
        returning
          *
      `.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>();
      const repoId = repoResult!.rows[0].id;

      // Create a commit for file operations
      const commitResult = await sql`
        insert into
          fs.commits (repository_id, message)
        values
          (${repoId}, 'Test commit')
        returning
          id
      `.query<{
        id: string;
      }>();
      const commitId = commitResult!.rows[0].id;

      // Manually update branch head
      await sql`
        update fs.branches
        set
          head_commit_id = ${commitId}
        where
          repository_id = ${repoId}
          and name = 'main'
      `.query();

      const insertResult = await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${commitId},
            '/src/main.ts/',
            'content'
          )
        returning
          *
      `.query<{
        id: string;
        commit_id: string;
        path: string;
        content: string;
        created_at: string;
      }>();

      assert.strictEqual(insertResult?.rows[0].path, "/src/main.ts");
    });

    itWithDb("should handle root path", async () => {
      const sql = getSql({ camelize: false });
      // Create repository for each test
      const repoResult = await sql`
        insert into
          fs.repositories (name)
        values
          ('path-test')
        returning
          *
      `.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>();
      const repoId = repoResult!.rows[0].id;

      // Create a commit for file operations
      const commitResult = await sql`
        insert into
          fs.commits (repository_id, message)
        values
          (${repoId}, 'Test commit')
        returning
          id
      `.query<{
        id: string;
      }>();
      const commitId = commitResult!.rows[0].id;

      // Manually update branch head
      await sql`
        update fs.branches
        set
          head_commit_id = ${commitId}
        where
          repository_id = ${repoId}
          and name = 'main'
      `.query();

      const insertResult = await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${commitId},
            '/',
            'content'
          )
        returning
          *
      `.query<{
        id: string;
        commit_id: string;
        path: string;
        content: string;
        created_at: string;
      }>();

      assert.strictEqual(insertResult?.rows[0].path, "/");
    });
  });

  describe("Path Validation", () => {
    itWithDb("should reject null paths", async () => {
      const sql = getSql({ camelize: false });
      try {
        await sql`
          select
            fs._validate_path (null) as _validate_path
        `.query();
        assert.fail("Expected validation to fail for null path");
      } catch (err: any) {
        assert.match(err.message, /Path cannot be null or empty/);
      }
    });

    itWithDb("should reject empty paths", async () => {
      const sql = getSql({ camelize: false });
      try {
        await sql`
          select
            fs._validate_path ('') as _validate_path
        `.query();
        assert.fail("Expected validation to fail for empty path");
      } catch (err: any) {
        assert.match(err.message, /Path cannot be null or empty/);
      }
    });

    itWithDb("should reject paths with control characters", async () => {
      const sql = getSql({ camelize: false });
      try {
        await sql`
          select
            fs._validate_path ('/testfile.txt') as _validate_path
        `.query();
        assert.fail(
          "Expected validation to fail for path with control characters",
        );
      } catch (err: any) {
        assert.match(err.message, /Path contains control characters/);
      }
    });

    itWithDb(
      "should reject paths with Windows-invalid characters",
      async () => {
        const sql = getSql({ camelize: false });
        // Test characters invalid on Windows: < > : " | ? *
        const invalidChars = ["<", ">", ":", '"', "|", "?", "*"];

        for (const char of invalidChars) {
          try {
            const path = `/test${char}file.txt`;
            await sql`
              select
                fs._validate_path (${path}) as _validate_path
            `.query();
            assert.fail(`Expected validation to fail for path with ${char}`);
          } catch (err: any) {
            assert.match(
              err.message,
              /Path contains characters invalid on Windows/,
            );
          }
        }
      },
    );

    itWithDb("should reject paths with control characters", async () => {
      const sql = getSql({ camelize: false });
      // Test control characters (0x00-0x1F except tab, newline, carriage return)
      try {
        await sql`
          select
            fs._validate_path ('/test' || chr(1) || 'file.txt') as _validate_path
        `.query();
        assert.fail(
          "Expected validation to fail for path with control character",
        );
      } catch (err: any) {
        assert.match(err.message, /Path contains control characters/);
      }

      try {
        await sql`
          select
            fs._validate_path ('/test' || chr(2) || 'file.txt') as _validate_path
        `.query();
        assert.fail(
          "Expected validation to fail for path with control character",
        );
      } catch (err: any) {
        assert.match(err.message, /Path contains control characters/);
      }
    });

    itWithDb("should reject paths with null bytes", async () => {
      const sql = getSql({ camelize: false });
      try {
        await sql`
          select
            fs._validate_path ('/test' || chr(0) || 'file.txt') as _validate_path
        `.query();
        assert.fail("Expected validation to fail for path with null byte");
      } catch (err: any) {
        // PostgreSQL itself rejects null characters, so we accept either our validation error or PostgreSQL's
        assert.ok(
          err.message.includes("Path contains null bytes") ||
            err.message.includes("null character not permitted"),
          `Unexpected error: ${err.message}`,
        );
      }
    });

    itWithDb("should reject very long paths", async () => {
      const sql = getSql({ camelize: false });
      const longPath = "/" + "a".repeat(4100);
      try {
        await sql`
          select
            fs._validate_path (${longPath}) as _validate_path
        `.query();
        assert.fail("Expected validation to fail for very long path");
      } catch (err: any) {
        assert.match(err.message, /Path is too long/);
      }
    });

    itWithDb("should accept valid paths", async () => {
      const sql = getSql({ camelize: false });
      const result = await sql`
        select
          fs._validate_path ('/valid/path/file.txt') as _validate_path
      `.query<{
        _validate_path: any;
      }>();
      // Should not throw an error
      assert.ok(result);
    });
  });

  describe("Real World Usage Scenario", () => {
    itWithDb(
      "should demonstrate basic repository and file operations",
      async () => {
        const sql = getSql({ camelize: false });
        // Create a repository
        const repoResult = await sql`
          insert into
            fs.repositories (name)
          values
            ('demo-repo')
          returning
            *
        `.query<{
          id: string;
          name: string;
          default_branch_id: string;
          created_at: string;
        }>();
        const repoId = repoResult!.rows[0].id;

        // Create a commit for adding files
        const commitResult = await sql`
          insert into
            fs.commits (repository_id, message)
          values
            (
              ${repoId},
              'Add initial files'
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const commitId = commitResult!.rows[0].id;

        // Manually update branch head
        await sql`
          update fs.branches
          set
            head_commit_id = ${commitId}
          where
            repository_id = ${repoId}
            and name = 'main'
        `.query();

        // Add some files to the commit
        await sql`
          insert into
            fs.files (commit_id, path, content)
          values
            (
              ${commitId},
              'index.html',
              ${"<h1>Hello World</h1>"}
            )
        `.query();

        await sql`
          insert into
            fs.files (commit_id, path, content)
          values
            (
              ${commitId},
              'styles.css',
              ${"body { background: #f0f0f0; }"}
            )
        `.query();

        // Verify files in initial commit
        const files = await sql`
          select
            path
          from
            fs.get_commit_snapshot (${commitId})
        `.query<{
          path: string;
        }>();
        assert.strictEqual(files?.rows.length, 2);

        const filePaths = files?.rows.map((f) => f.path).sort();
        assert.deepStrictEqual(filePaths, ["/index.html", "/styles.css"]);

        // Verify exact file contents
        const htmlFile = await sql`
          select
            fs.read_file (${commitId}, '/index.html') as content
        `.query<{
          content: string | null;
        }>();
        assert.strictEqual(htmlFile?.rows[0].content, "<h1>Hello World</h1>");

        const cssFile = await sql`
          select
            fs.read_file (${commitId}, '/styles.css') as content
        `.query<{
          content: string | null;
        }>();
        assert.strictEqual(
          cssFile?.rows[0].content,
          "body { background: #f0f0f0; }",
        );

        // Create another commit for file updates
        const updateCommitResult = await sql`
          insert into
            fs.commits (repository_id, message, parent_commit_id)
          values
            (
              ${repoId},
              'Update HTML file',
              ${commitId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const updateCommitId = updateCommitResult!.rows[0].id;

        // Manually update branch head
        await sql`
          update fs.branches
          set
            head_commit_id = ${updateCommitId}
          where
            repository_id = ${repoId}
            and name = 'main'
        `.query();

        // Modify a file in the new commit
        await sql`
          insert into
            fs.files (commit_id, path, content)
          values
            (
              ${updateCommitId},
              '/index.html',
              ${"<h1>Hello Updated World</h1>"}
            )
        `.query();

        // Verify the updated files
        const updatedFiles = await sql`
          select
            path
          from
            fs.get_commit_snapshot (${updateCommitId})
        `.query<{
          path: string;
        }>();
        assert.strictEqual(updatedFiles?.rows.length, 2);

        const updatedFilePaths = updatedFiles?.rows.map((f) => f.path).sort();
        assert.deepStrictEqual(updatedFilePaths, [
          "/index.html",
          "/styles.css",
        ]);

        // Verify exact updated contents
        const updatedHtmlFile = await sql`
          select
            fs.read_file (${updateCommitId}, '/index.html') as content
        `.query<{
          content: string | null;
        }>();
        assert.strictEqual(
          updatedHtmlFile?.rows[0].content,
          "<h1>Hello Updated World</h1>",
        );

        const updatedCssFile = await sql`
          select
            fs.read_file (${updateCommitId}, '/styles.css') as content
        `.query<{
          content: string | null;
        }>();
        assert.strictEqual(
          updatedCssFile?.rows[0].content,
          "body { background: #f0f0f0; }",
        ); // Should remain unchanged

        // Verify we have multiple fs.commits
        const commitCount = await sql`
          select
            COUNT(*) as count
          from
            fs.commits
          where
            repository_id = ${repoId}
        `.query<{
          count: number;
        }>();
        assert.ok(
          commitCount && commitCount.rows[0] && commitCount.rows[0].count >= 2,
        ); // 2 manual fs.commits
      },
    );
  });

  describe("Edge Cases", () => {
    itWithDb("should return null for non-existent files", async () => {
      const sql = getSql({ camelize: false });
      // Create repository
      const repoResult = await sql`
        insert into
          fs.repositories (name)
        values
          ('edge-test')
        returning
          *
      `.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>();
      const repoId = repoResult?.rows[0].id;

      // Create a commit manually so we have one to read from
      await sql`
        insert into
          fs.commits (repository_id, parent_commit_id, message)
        values
          (
            ${repoId},
            null,
            'Empty commit'
          )
      `.query();

      const commitResult = await sql`
        select
          id
        from
          fs.commits
        where
          message = 'Empty commit'
      `.query<{
        id: string;
      }>();
      const commitId = commitResult?.rows[0].id;

      const result = await sql`
        select
          fs.read_file (
            ${commitId},
            '/nonexistent.txt'
          ) as content
      `.query<{
        content: string | null;
      }>();

      assert.strictEqual(result?.rows[0].content, null);
    });

    itWithDb("should handle empty file content", async () => {
      const sql = getSql({ camelize: false });
      // Create repository
      const repoResult = await sql`
        insert into
          fs.repositories (name)
        values
          ('empty-test')
        returning
          *
      `.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>();
      const repoId = repoResult?.rows[0].id;

      // Create a commit
      await sql`
        insert into
          fs.commits (repository_id, parent_commit_id, message)
        values
          (
            ${repoId},
            null,
            'Empty file commit'
          )
      `.query();

      const commitResult = await sql`
        select
          id
        from
          fs.commits
        where
          message = 'Empty file commit'
      `.query<{
        id: string;
      }>();
      const commitId = commitResult?.rows[0].id;

      // Write empty file
      await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${commitId},
            '/empty.txt',
            ''
          )
      `.query();

      const result = await sql`
        select
          fs.read_file (${commitId}, '/empty.txt') as content
      `.query<{
        content: string;
      }>();

      assert.strictEqual(result?.rows[0].content, "");
    });
  });

  describe("Deletions", () => {
    itWithDb(
      "should support tombstone deletions via files.is_deleted",
      async () => {
        const sql = getSql({ camelize: false });
        // Create repository
        const repoResult = await sql`
          insert into
            fs.repositories (name)
          values
            ('delete-test')
          returning
            *
        `.query<{
          id: string;
          name: string;
          default_branch_id: string;
          created_at: string;
        }>();
        const repoId = repoResult!.rows[0].id;

        // Create commit 1
        const commit1Result = await sql`
          insert into
            fs.commits (repository_id, message)
          values
            (${repoId}, 'Commit 1')
          returning
            id
        `.query<{
          id: string;
        }>();
        const commit1Id = commit1Result!.rows[0].id;

        // Update branch head
        await sql`
          update fs.branches
          set
            head_commit_id = ${commit1Id}
          where
            repository_id = ${repoId}
            and name = 'main'
        `.query();

        // Write a file in commit 1
        await sql`
          insert into
            fs.files (commit_id, path, content)
          values
            (
              ${commit1Id},
              ${"/delete-me.txt"},
              ${"hello"}
            )
        `.query();

        // Create commit 2
        const commit2Result = await sql`
          insert into
            fs.commits (repository_id, message, parent_commit_id)
          values
            (
              ${repoId},
              'Commit 2',
              ${commit1Id}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const commit2Id = commit2Result!.rows[0].id;

        // Update branch head
        await sql`
          update fs.branches
          set
            head_commit_id = ${commit2Id}
          where
            repository_id = ${repoId}
            and name = 'main'
        `.query();

        // Tombstone delete in commit 2 (no content required)
        await sql`
          insert into
            fs.files (commit_id, path, is_deleted)
          values
            (
              ${commit2Id},
              ${"/delete-me.txt"},
              true
            )
        `.query();

        // read_file should normalize and respect tombstones
        const before = await sql`
          select
            fs.read_file (
              ${commit1Id},
              'delete-me.txt'
            ) as content
        `.query<{
          content: string | null;
        }>();
        assert.strictEqual(before?.rows[0].content, "hello");

        const after = await sql`
          select
            fs.read_file (
              ${commit2Id},
              'delete-me.txt'
            ) as content
        `.query<{
          content: string | null;
        }>();
        assert.strictEqual(after?.rows[0].content, null);

        const files = await sql`
          select
            path
          from
            fs.get_commit_snapshot (${commit2Id})
          order by
            path
        `.query<{
          path: string;
        }>();
        assert.deepStrictEqual(
          files?.rows.map((r) => r.path),
          [],
        );

        const history = await sql`
          select
            *
          from
            fs.get_file_history (
              ${commit2Id},
              ${"/delete-me.txt"}
            )
        `.query<{
          commit_id: string;
          content: string | null;
          is_deleted: boolean;
          is_symlink: boolean;
        }>();
        assert.strictEqual(history?.rows.length, 2);
        assert.ok(
          history?.rows.some((r) => r.is_deleted && r.content === null),
        );
        assert.ok(
          history?.rows.some((r) => !r.is_deleted && r.content === "hello"),
        );
        assert.ok(history?.rows.every((r) => r.is_symlink === false));
      },
    );
  });

  describe("Symlinks", () => {
    itWithDb(
      "should store symlink targets as normalized absolute paths",
      async () => {
        const sql = getSql({ camelize: false });
        // Create repository
        const repoResult = await sql`
          insert into
            fs.repositories (name)
          values
            ('symlink-test')
          returning
            *
        `.query<{
          id: string;
          name: string;
          default_branch_id: string;
          created_at: string;
        }>();
        const repoId = repoResult!.rows[0].id;

        // Create commit
        const commitResult = await sql`
          insert into
            fs.commits (repository_id, message)
          values
            (${repoId}, 'Add symlink')
          returning
            id
        `.query<{
          id: string;
        }>();
        const commitId = commitResult!.rows[0].id;

        // Update branch head
        await sql`
          update fs.branches
          set
            head_commit_id = ${commitId}
          where
            repository_id = ${repoId}
            and name = 'main'
        `.query();

        // Target file
        await sql`
          insert into
            fs.files (commit_id, path, content)
          values
            (
              ${commitId},
              ${"/target.txt"},
              ${"hello"}
            )
        `.query();

        // Symlink file (target path is stored in content)
        await sql`
          insert into
            fs.files (commit_id, path, content, is_symlink)
          values
            (
              ${commitId},
              ${"/link.txt"},
              ${"target.txt"},
              true
            )
        `.query();

        const stored = await sql`
          select
            path,
            content,
            is_symlink
          from
            fs.files
          where
            commit_id = ${commitId}
            and path = ${"/link.txt"}
        `.query<{
          path: string;
          content: string;
          is_symlink: boolean;
        }>();

        assert.strictEqual(stored?.rows.length, 1);
        assert.strictEqual(stored?.rows[0].is_symlink, true);
        assert.strictEqual(stored?.rows[0].content, "/target.txt"); // normalized to absolute

        const snapshot = await sql`
          select
            path,
            is_symlink
          from
            fs.get_commit_snapshot (${commitId})
          order by
            path
        `.query<{
          path: string;
          is_symlink: boolean;
        }>();
        assert.deepStrictEqual(
          snapshot?.rows.map((r) => r.path),
          ["/link.txt", "/target.txt"],
        );
        const link = snapshot?.rows.find((r) => r.path === "/link.txt");
        assert.strictEqual(link?.is_symlink, true);

        // read_file returns the stored content (the link target path) for now
        const read = await sql`
          select
            fs.read_file (${commitId}, '/link.txt') as content
        `.query<{
          content: string | null;
        }>();
        assert.strictEqual(read?.rows[0].content, "/target.txt");

        const history = await sql`
          select
            *
          from
            fs.get_file_history (
              ${commitId},
              ${"/link.txt"}
            )
        `.query<{
          commit_id: string;
          content: string | null;
          is_deleted: boolean;
          is_symlink: boolean;
        }>();
        assert.strictEqual(history?.rows.length, 1);
        assert.strictEqual(history?.rows[0].is_deleted, false);
        assert.strictEqual(history?.rows[0].is_symlink, true);
        assert.strictEqual(history?.rows[0].content, "/target.txt");
      },
    );
  });

  describe("Merge / Rebase Helpers", () => {
    itWithDb(
      "should compute merge base for ancestor relationships",
      async () => {
        const sql = getSql({ camelize: false });
        const repoResult = await sql`
          insert into
            fs.repositories (name)
          values
            ('merge-base-ancestor')
          returning
            id
        `.query<{
          id: string;
        }>();
        const repoId = repoResult!.rows[0].id;

        const rootHead = await sql`
          select
            head_commit_id
          from
            fs.branches
          where
            repository_id = ${repoId}
            and name = 'main'
        `.query<{
          head_commit_id: string;
        }>();
        const rootCommitId = rootHead!.rows[0].head_commit_id;

        const commitAResult = await sql`
          insert into
            fs.commits (repository_id, message, parent_commit_id)
          values
            (
              ${repoId},
              'A',
              ${rootCommitId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const commitAId = commitAResult!.rows[0].id;

        const commitBResult = await sql`
          insert into
            fs.commits (repository_id, message, parent_commit_id)
          values
            (
              ${repoId},
              'B',
              ${commitAId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const commitBId = commitBResult!.rows[0].id;

        const base1 = await sql`
          select
            fs.get_merge_base (
              ${commitAId},
              ${commitBId}
            ) as base
        `.query<{
          base: string;
        }>();
        assert.strictEqual(base1?.rows[0].base, commitAId);

        const base2 = await sql`
          select
            fs.get_merge_base (
              ${commitBId},
              ${commitAId}
            ) as base
        `.query<{
          base: string;
        }>();
        assert.strictEqual(base2?.rows[0].base, commitAId);

        const base3 = await sql`
          select
            fs.get_merge_base (
              ${commitBId},
              ${commitBId}
            ) as base
        `.query<{
          base: string;
        }>();
        assert.strictEqual(base3?.rows[0].base, commitBId);
      },
    );

    itWithDb("should compute merge base for diverged branches", async () => {
      const sql = getSql({ camelize: false });
      const repoResult = await sql`
        insert into
          fs.repositories (name)
        values
          ('merge-base-diverged')
        returning
          id
      `.query<{
        id: string;
      }>();
      const repoId = repoResult!.rows[0].id;

      const rootHead = await sql`
        select
          head_commit_id
        from
          fs.branches
        where
          repository_id = ${repoId}
          and name = 'main'
      `.query<{
        head_commit_id: string;
      }>();
      const rootCommitId = rootHead!.rows[0].head_commit_id;

      const baseResult = await sql`
        insert into
          fs.commits (repository_id, message, parent_commit_id)
        values
          (
            ${repoId},
            'Base',
            ${rootCommitId}
          )
        returning
          id
      `.query<{
        id: string;
      }>();
      const baseCommitId = baseResult!.rows[0].id;

      // Move main forward so the new branch defaults to this base
      await sql`
        update fs.branches
        set
          head_commit_id = ${baseCommitId}
        where
          repository_id = ${repoId}
          and name = 'main'
      `.query();

      const featureBranchResult = await sql`
        insert into
          fs.branches (repository_id, name)
        values
          (${repoId}, 'feature')
        returning
          id
      `.query<{
        id: string;
      }>();
      const featureBranchId = featureBranchResult!.rows[0].id;

      const main1Result = await sql`
        insert into
          fs.commits (repository_id, message, parent_commit_id)
        values
          (
            ${repoId},
            'main-1',
            ${baseCommitId}
          )
        returning
          id
      `.query<{
        id: string;
      }>();
      const main1Id = main1Result!.rows[0].id;

      const feat1Result = await sql`
        insert into
          fs.commits (repository_id, message, parent_commit_id)
        values
          (
            ${repoId},
            'feature-1',
            ${baseCommitId}
          )
        returning
          id
      `.query<{
        id: string;
      }>();
      const feat1Id = feat1Result!.rows[0].id;

      // (Not required for merge base, but keep branch heads realistic)
      await sql`
        update fs.branches
        set
          head_commit_id = ${main1Id}
        where
          repository_id = ${repoId}
          and name = 'main'
      `.query();
      await sql`
        update fs.branches
        set
          head_commit_id = ${feat1Id}
        where
          id = ${featureBranchId}
      `.query();

      const base = await sql`
        select
          fs.get_merge_base (
            ${main1Id},
            ${feat1Id}
          ) as base
      `.query<{
        base: string;
      }>();
      assert.strictEqual(base?.rows[0].base, baseCommitId);
      assert.notStrictEqual(base?.rows[0].base, rootCommitId);
    });

    itWithDb("should reject merge base across repositories", async () => {
      const sql = getSql({ camelize: false });
      const repo1 = await sql`
        insert into
          fs.repositories (name)
        values
          ('merge-base-repo-1')
        returning
          id
      `.query<{
        id: string;
      }>();
      const repo2 = await sql`
        insert into
          fs.repositories (name)
        values
          ('merge-base-repo-2')
        returning
          id
      `.query<{
        id: string;
      }>();

      const repo1Id = repo1!.rows[0].id;
      const repo2Id = repo2!.rows[0].id;

      // Create one commit in each repo so we have valid commit ids
      const c1 = await sql`
        insert into
          fs.commits (repository_id, message)
        values
          (${repo1Id}, 'repo-1-root')
        returning
          id
      `.query<{
        id: string;
      }>();
      const c2 = await sql`
        insert into
          fs.commits (repository_id, message)
        values
          (${repo2Id}, 'repo-2-root')
        returning
          id
      `.query<{
        id: string;
      }>();

      try {
        await sql`
          select
            fs.get_merge_base (
              ${c1!.rows[0].id},
              ${c2!.rows[0].id}
            )
        `.query();
        assert.fail("Expected merge base to fail across repositories");
      } catch (err: any) {
        assert.match(err.message, /Commits must belong to the same repository/);
      }
    });

    itWithDb(
      "should return no conflicts when changes do not overlap",
      async () => {
        const sql = getSql({ camelize: false });
        const repoResult = await sql`
          insert into
            fs.repositories (name)
          values
            ('conflicts-non-overlap')
          returning
            id
        `.query<{
          id: string;
        }>();
        const repoId = repoResult!.rows[0].id;

        const rootHead = await sql`
          select
            head_commit_id
          from
            fs.branches
          where
            repository_id = ${repoId}
            and name = 'main'
        `.query<{
          head_commit_id: string;
        }>();
        const rootCommitId = rootHead!.rows[0].head_commit_id;

        const baseResult = await sql`
          insert into
            fs.commits (repository_id, message, parent_commit_id)
          values
            (
              ${repoId},
              'Base',
              ${rootCommitId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const baseCommitId = baseResult!.rows[0].id;

        const leftResult = await sql`
          insert into
            fs.commits (repository_id, message, parent_commit_id)
          values
            (
              ${repoId},
              'Left',
              ${baseCommitId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const leftCommitId = leftResult!.rows[0].id;
        await sql`
          insert into
            fs.files (commit_id, path, content)
          values
            (
              ${leftCommitId},
              ${"/main-only.txt"},
              ${"main"}
            )
        `.query();

        const rightResult = await sql`
          insert into
            fs.commits (repository_id, message, parent_commit_id)
          values
            (
              ${repoId},
              'Right',
              ${baseCommitId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const rightCommitId = rightResult!.rows[0].id;
        await sql`
          insert into
            fs.files (commit_id, path, content)
          values
            (
              ${rightCommitId},
              ${"/feature-only.txt"},
              ${"feature"}
            )
        `.query();

        const conflicts = await sql`
          select
            path
          from
            fs.get_conflicts (
              ${leftCommitId},
              ${rightCommitId}
            )
        `.query<{
          path: string;
        }>();
        assert.strictEqual(conflicts?.rows.length, 0);
      },
    );

    itWithDb("should detect modify/modify conflicts", async () => {
      const sql = getSql({ camelize: false });
      const repoResult = await sql`
        insert into
          fs.repositories (name)
        values
          ('conflicts-modify-modify')
        returning
          id
      `.query<{
        id: string;
      }>();
      const repoId = repoResult!.rows[0].id;

      const rootHead = await sql`
        select
          head_commit_id
        from
          fs.branches
        where
          repository_id = ${repoId}
          and name = 'main'
      `.query<{
        head_commit_id: string;
      }>();
      const rootCommitId = rootHead!.rows[0].head_commit_id;

      const baseResult = await sql`
        insert into
          fs.commits (repository_id, message, parent_commit_id)
        values
          (
            ${repoId},
            'Base',
            ${rootCommitId}
          )
        returning
          id
      `.query<{
        id: string;
      }>();
      const baseCommitId = baseResult!.rows[0].id;
      await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${baseCommitId},
            ${"/same.txt"},
            ${"base"}
          )
      `.query();

      const leftResult = await sql`
        insert into
          fs.commits (repository_id, message, parent_commit_id)
        values
          (
            ${repoId},
            'Left',
            ${baseCommitId}
          )
        returning
          id
      `.query<{
        id: string;
      }>();
      const leftCommitId = leftResult!.rows[0].id;
      await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${leftCommitId},
            ${"/same.txt"},
            ${"left"}
          )
      `.query();

      const rightResult = await sql`
        insert into
          fs.commits (repository_id, message, parent_commit_id)
        values
          (
            ${repoId},
            'Right',
            ${baseCommitId}
          )
        returning
          id
      `.query<{
        id: string;
      }>();
      const rightCommitId = rightResult!.rows[0].id;
      await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${rightCommitId},
            ${"/same.txt"},
            ${"right"}
          )
      `.query();

      const conflicts = await sql`
        select
          *
        from
          fs.get_conflicts (
            ${leftCommitId},
            ${rightCommitId}
          )
      `.query<{
        merge_base_commit_id: string;
        path: string;
        base_exists: boolean;
        left_exists: boolean;
        right_exists: boolean;
        base_content: string | null;
        left_content: string | null;
        right_content: string | null;
        conflict_kind: string;
      }>();

      assert.strictEqual(conflicts?.rows.length, 1);
      const c = conflicts!.rows[0];
      assert.strictEqual(c.merge_base_commit_id, baseCommitId);
      assert.strictEqual(c.path, "/same.txt");
      assert.strictEqual(c.base_exists, true);
      assert.strictEqual(c.left_exists, true);
      assert.strictEqual(c.right_exists, true);
      assert.strictEqual(c.base_content, "base");
      assert.strictEqual(c.left_content, "left");
      assert.strictEqual(c.right_content, "right");
      assert.strictEqual(c.conflict_kind, "modify/modify");
    });

    itWithDb("should detect delete/modify conflicts", async () => {
      const sql = getSql({ camelize: false });
      const repoResult = await sql`
        insert into
          fs.repositories (name)
        values
          ('conflicts-delete-modify')
        returning
          id
      `.query<{
        id: string;
      }>();
      const repoId = repoResult!.rows[0].id;

      const rootHead = await sql`
        select
          head_commit_id
        from
          fs.branches
        where
          repository_id = ${repoId}
          and name = 'main'
      `.query<{
        head_commit_id: string;
      }>();
      const rootCommitId = rootHead!.rows[0].head_commit_id;

      const baseResult = await sql`
        insert into
          fs.commits (repository_id, message, parent_commit_id)
        values
          (
            ${repoId},
            'Base',
            ${rootCommitId}
          )
        returning
          id
      `.query<{
        id: string;
      }>();
      const baseCommitId = baseResult!.rows[0].id;
      await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${baseCommitId},
            ${"/del.txt"},
            ${"base"}
          )
      `.query();

      const leftResult = await sql`
        insert into
          fs.commits (repository_id, message, parent_commit_id)
        values
          (
            ${repoId},
            'Left',
            ${baseCommitId}
          )
        returning
          id
      `.query<{
        id: string;
      }>();
      const leftCommitId = leftResult!.rows[0].id;
      await sql`
        insert into
          fs.files (commit_id, path, is_deleted)
        values
          (
            ${leftCommitId},
            ${"/del.txt"},
            true
          )
      `.query();

      const rightResult = await sql`
        insert into
          fs.commits (repository_id, message, parent_commit_id)
        values
          (
            ${repoId},
            'Right',
            ${baseCommitId}
          )
        returning
          id
      `.query<{
        id: string;
      }>();
      const rightCommitId = rightResult!.rows[0].id;
      await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${rightCommitId},
            ${"/del.txt"},
            ${"right"}
          )
      `.query();

      const conflicts = await sql`
        select
          path,
          base_exists,
          left_exists,
          right_exists,
          base_content,
          left_content,
          right_content,
          conflict_kind
        from
          fs.get_conflicts (
            ${leftCommitId},
            ${rightCommitId}
          )
      `.query<{
        path: string;
        base_exists: boolean;
        left_exists: boolean;
        right_exists: boolean;
        base_content: string | null;
        left_content: string | null;
        right_content: string | null;
        conflict_kind: string;
      }>();

      assert.strictEqual(conflicts?.rows.length, 1);
      const c = conflicts!.rows[0];
      assert.strictEqual(c.path, "/del.txt");
      assert.strictEqual(c.base_exists, true);
      assert.strictEqual(c.left_exists, false);
      assert.strictEqual(c.right_exists, true);
      assert.strictEqual(c.base_content, "base");
      assert.strictEqual(c.left_content, null);
      assert.strictEqual(c.right_content, "right");
      assert.strictEqual(c.conflict_kind, "delete/modify");
    });

    itWithDb("should detect add/add conflicts", async () => {
      const sql = getSql({ camelize: false });
      const repoResult = await sql`
        insert into
          fs.repositories (name)
        values
          ('conflicts-add-add')
        returning
          id
      `.query<{
        id: string;
      }>();
      const repoId = repoResult!.rows[0].id;

      const rootHead = await sql`
        select
          head_commit_id
        from
          fs.branches
        where
          repository_id = ${repoId}
          and name = 'main'
      `.query<{
        head_commit_id: string;
      }>();
      const rootCommitId = rootHead!.rows[0].head_commit_id;

      const baseResult = await sql`
        insert into
          fs.commits (repository_id, message, parent_commit_id)
        values
          (
            ${repoId},
            'Base',
            ${rootCommitId}
          )
        returning
          id
      `.query<{
        id: string;
      }>();
      const baseCommitId = baseResult!.rows[0].id;

      const leftResult = await sql`
        insert into
          fs.commits (repository_id, message, parent_commit_id)
        values
          (
            ${repoId},
            'Left',
            ${baseCommitId}
          )
        returning
          id
      `.query<{
        id: string;
      }>();
      const leftCommitId = leftResult!.rows[0].id;
      await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${leftCommitId},
            ${"/new.txt"},
            ${"left"}
          )
      `.query();

      const rightResult = await sql`
        insert into
          fs.commits (repository_id, message, parent_commit_id)
        values
          (
            ${repoId},
            'Right',
            ${baseCommitId}
          )
        returning
          id
      `.query<{
        id: string;
      }>();
      const rightCommitId = rightResult!.rows[0].id;
      await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${rightCommitId},
            ${"/new.txt"},
            ${"right"}
          )
      `.query();

      const conflicts = await sql`
        select
          path,
          base_exists,
          left_exists,
          right_exists,
          base_content,
          left_content,
          right_content,
          conflict_kind
        from
          fs.get_conflicts (
            ${leftCommitId},
            ${rightCommitId}
          )
      `.query<{
        path: string;
        base_exists: boolean;
        left_exists: boolean;
        right_exists: boolean;
        base_content: string | null;
        left_content: string | null;
        right_content: string | null;
        conflict_kind: string;
      }>();

      assert.strictEqual(conflicts?.rows.length, 1);
      const c = conflicts!.rows[0];
      assert.strictEqual(c.path, "/new.txt");
      assert.strictEqual(c.base_exists, false);
      assert.strictEqual(c.left_exists, true);
      assert.strictEqual(c.right_exists, true);
      assert.strictEqual(c.base_content, null);
      assert.strictEqual(c.left_content, "left");
      assert.strictEqual(c.right_content, "right");
      assert.strictEqual(c.conflict_kind, "add/add");
    });

    itWithDb("should treat symlink/file differences as conflicts", async () => {
      const sql = getSql({ camelize: false });
      const repoResult = await sql`
        insert into
          fs.repositories (name)
        values
          ('conflicts-symlink-file')
        returning
          id
      `.query<{
        id: string;
      }>();
      const repoId = repoResult!.rows[0].id;

      const rootHead = await sql`
        select
          head_commit_id
        from
          fs.branches
        where
          repository_id = ${repoId}
          and name = 'main'
      `.query<{
        head_commit_id: string;
      }>();
      const rootCommitId = rootHead!.rows[0].head_commit_id;

      const baseResult = await sql`
        insert into
          fs.commits (repository_id, message, parent_commit_id)
        values
          (
            ${repoId},
            'Base',
            ${rootCommitId}
          )
        returning
          id
      `.query<{
        id: string;
      }>();
      const baseCommitId = baseResult!.rows[0].id;
      await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${baseCommitId},
            ${"/thing.txt"},
            ${"base"}
          )
      `.query();

      const leftResult = await sql`
        insert into
          fs.commits (repository_id, message, parent_commit_id)
        values
          (
            ${repoId},
            'Left',
            ${baseCommitId}
          )
        returning
          id
      `.query<{
        id: string;
      }>();
      const leftCommitId = leftResult!.rows[0].id;
      await sql`
        insert into
          fs.files (commit_id, path, content, is_symlink)
        values
          (
            ${leftCommitId},
            ${"/thing.txt"},
            ${"target.txt"},
            true
          )
      `.query();

      const rightResult = await sql`
        insert into
          fs.commits (repository_id, message, parent_commit_id)
        values
          (
            ${repoId},
            'Right',
            ${baseCommitId}
          )
        returning
          id
      `.query<{
        id: string;
      }>();
      const rightCommitId = rightResult!.rows[0].id;
      await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${rightCommitId},
            ${"/thing.txt"},
            ${"right"}
          )
      `.query();

      const conflicts = await sql`
        select
          path,
          base_is_symlink,
          left_is_symlink,
          right_is_symlink,
          base_content,
          left_content,
          right_content,
          conflict_kind
        from
          fs.get_conflicts (
            ${leftCommitId},
            ${rightCommitId}
          )
      `.query<{
        path: string;
        base_is_symlink: boolean;
        left_is_symlink: boolean;
        right_is_symlink: boolean;
        base_content: string | null;
        left_content: string | null;
        right_content: string | null;
        conflict_kind: string;
      }>();

      assert.strictEqual(conflicts?.rows.length, 1);
      const c = conflicts!.rows[0];
      assert.strictEqual(c.path, "/thing.txt");
      assert.strictEqual(c.base_is_symlink, false);
      assert.strictEqual(c.left_is_symlink, true);
      assert.strictEqual(c.right_is_symlink, false);
      assert.strictEqual(c.base_content, "base");
      assert.strictEqual(c.left_content, "/target.txt"); // normalized absolute target
      assert.strictEqual(c.right_content, "right");
      assert.strictEqual(c.conflict_kind, "modify/modify");
    });

    itWithDb(
      "should reject conflict checks for invalid commit ids",
      async () => {
        const sql = getSql({ camelize: false });
        const repoResult = await sql`
          insert into
            fs.repositories (name)
          values
            ('conflicts-invalid-ids')
          returning
            id
        `.query<{
          id: string;
        }>();
        const repoId = repoResult!.rows[0].id;

        const goodCommit = await sql`
          insert into
            fs.commits (repository_id, message)
          values
            (${repoId}, 'good')
          returning
            id
        `.query<{
          id: string;
        }>();
        const goodCommitId = goodCommit!.rows[0].id;

        try {
          await sql`
            select
              *
            from
              fs.get_conflicts (
                ${goodCommitId},
                ${"00000000-0000-0000-0000-000000000000"}
              )
          `.query();
          assert.fail("Expected conflict check to fail for invalid commit id");
        } catch (err: any) {
          assert.match(
            err.message,
            /Invalid commit_id \(right\): commit does not exist/,
          );
        }
      },
    );
  });

  describe("Merge / Rebase Operations", () => {
    itWithDb(
      "should finalize merge by applying non-conflicting changes",
      async () => {
        const sql = getSql({ camelize: false });
        const repoResult = await sql`
          insert into
            fs.repositories (name)
          values
            ('merge-non-conflict')
          returning
            id
        `.query<{
          id: string;
        }>();
        const repoId = repoResult!.rows[0].id;

        const mainBranch = await sql`
          select
            id,
            head_commit_id
          from
            fs.branches
          where
            repository_id = ${repoId}
            and name = 'main'
        `.query<{
          id: string;
          head_commit_id: string | null;
        }>();
        const mainBranchId = mainBranch!.rows[0].id;
        const rootHeadId = mainBranch!.rows[0].head_commit_id;

        const baseCommit = await sql`
          insert into
            fs.commits (repository_id, message, parent_commit_id)
          values
            (
              ${repoId},
              'base',
              ${rootHeadId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const baseCommitId = baseCommit!.rows[0].id;
        await sql`
          update fs.branches
          set
            head_commit_id = ${baseCommitId}
          where
            id = ${mainBranchId}
        `.query();

        const featureBranch = await sql`
          insert into
            fs.branches (repository_id, name)
          values
            (${repoId}, 'feature')
          returning
            id,
            head_commit_id
        `.query<{
          id: string;
          head_commit_id: string;
        }>();
        const featureBranchId = featureBranch!.rows[0].id;
        assert.strictEqual(featureBranch!.rows[0].head_commit_id, baseCommitId);

        const mainCommit = await sql`
          insert into
            fs.commits (repository_id, message, parent_commit_id)
          values
            (
              ${repoId},
              'main-1',
              ${baseCommitId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const mainCommitId = mainCommit!.rows[0].id;
        await sql`
          insert into
            fs.files (commit_id, path, content)
          values
            (
              ${mainCommitId},
              ${"/main.txt"},
              ${"main"}
            )
        `.query();
        await sql`
          update fs.branches
          set
            head_commit_id = ${mainCommitId}
          where
            id = ${mainBranchId}
        `.query();

        const featCommit = await sql`
          insert into
            fs.commits (repository_id, message, parent_commit_id)
          values
            (
              ${repoId},
              'feature-1',
              ${baseCommitId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const featCommitId = featCommit!.rows[0].id;
        await sql`
          insert into
            fs.files (commit_id, path, content)
          values
            (
              ${featCommitId},
              ${"/feature.txt"},
              ${"feature"}
            )
        `.query();
        await sql`
          update fs.branches
          set
            head_commit_id = ${featCommitId}
          where
            id = ${featureBranchId}
        `.query();

        const mergeCommit = await sql`
          insert into
            fs.commits (
              repository_id,
              message,
              parent_commit_id,
              merged_from_commit_id
            )
          values
            (
              ${repoId},
              'Merge feature into main',
              ${mainCommitId},
              ${featCommitId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const mergeCommitId = mergeCommit!.rows[0].id;

        const mergeResult = await sql`
          select
            operation,
            merge_commit_id,
            new_target_head_commit_id,
            applied_file_count
          from
            fs.finalize_commit (
              ${mergeCommitId},
              ${mainBranchId}
            )
        `.query<{
          operation: string;
          merge_commit_id: string | null;
          new_target_head_commit_id: string;
          applied_file_count: number;
        }>();

        assert.strictEqual(mergeResult?.rows.length, 1);
        assert.strictEqual(mergeResult?.rows[0].operation, "merged");
        assert.strictEqual(mergeResult?.rows[0].merge_commit_id, mergeCommitId);
        assert.strictEqual(
          mergeResult?.rows[0].new_target_head_commit_id,
          mergeCommitId,
        );
        assert.strictEqual(mergeResult?.rows[0].applied_file_count, 1);

        const snapshot = await sql`
          select
            path
          from
            fs.get_commit_snapshot (${mergeCommitId})
          order by
            path
        `.query<{
          path: string;
        }>();
        assert.deepStrictEqual(
          snapshot?.rows.map((r) => r.path),
          ["/feature.txt", "/main.txt"],
        );
      },
    );

    itWithDb(
      "should require conflict resolutions before finalizing",
      async () => {
        const sql = getSql({ camelize: false });
        const repoResult = await sql`
          insert into
            fs.repositories (name)
          values
            ('merge-conflict-required')
          returning
            id
        `.query<{
          id: string;
        }>();
        const repoId = repoResult!.rows[0].id;

        const mainBranch = await sql`
          select
            id,
            head_commit_id
          from
            fs.branches
          where
            repository_id = ${repoId}
            and name = 'main'
        `.query<{
          id: string;
          head_commit_id: string;
        }>();
        const mainBranchId = mainBranch!.rows[0].id;
        const rootHeadId = mainBranch!.rows[0].head_commit_id;

        const baseCommit = await sql`
          insert into
            fs.commits (repository_id, message, parent_commit_id)
          values
            (
              ${repoId},
              'base',
              ${rootHeadId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const baseCommitId = baseCommit!.rows[0].id;
        await sql`
          update fs.branches
          set
            head_commit_id = ${baseCommitId}
          where
            id = ${mainBranchId}
        `.query();
        await sql`
          insert into
            fs.files (commit_id, path, content)
          values
            (
              ${baseCommitId},
              ${"/same.txt"},
              ${"base"}
            )
        `.query();

        const featureBranch = await sql`
          insert into
            fs.branches (repository_id, name)
          values
            (${repoId}, 'feature')
          returning
            id
        `.query<{
          id: string;
        }>();
        const featureBranchId = featureBranch!.rows[0].id;

        const mainCommit = await sql`
          insert into
            fs.commits (repository_id, message, parent_commit_id)
          values
            (
              ${repoId},
              'main-1',
              ${baseCommitId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const mainCommitId = mainCommit!.rows[0].id;
        await sql`
          insert into
            fs.files (commit_id, path, content)
          values
            (
              ${mainCommitId},
              ${"/same.txt"},
              ${"main"}
            )
        `.query();
        await sql`
          update fs.branches
          set
            head_commit_id = ${mainCommitId}
          where
            id = ${mainBranchId}
        `.query();

        const featCommit = await sql`
          insert into
            fs.commits (repository_id, message, parent_commit_id)
          values
            (
              ${repoId},
              'feature-1',
              ${baseCommitId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const featCommitId = featCommit!.rows[0].id;
        await sql`
          insert into
            fs.files (commit_id, path, content)
          values
            (
              ${featCommitId},
              ${"/same.txt"},
              ${"feature"}
            )
        `.query();
        await sql`
          update fs.branches
          set
            head_commit_id = ${featCommitId}
          where
            id = ${featureBranchId}
        `.query();

        const mergeCommit = await sql`
          insert into
            fs.commits (
              repository_id,
              message,
              parent_commit_id,
              merged_from_commit_id
            )
          values
            (
              ${repoId},
              'Merge with conflict',
              ${mainCommitId},
              ${featCommitId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const mergeCommitId = mergeCommit!.rows[0].id;

        try {
          await sql`
            select
              *
            from
              fs.finalize_commit (
                ${mergeCommitId},
                ${mainBranchId}
              )
          `.query();
          assert.fail("Expected merge to fail without conflict resolutions");
        } catch (err: any) {
          assert.match(err.message, /Merge requires resolutions/);
        }

        const mainHead = await sql`
          select
            head_commit_id
          from
            fs.branches
          where
            id = ${mainBranchId}
        `.query<{
          head_commit_id: string;
        }>();
        assert.strictEqual(mainHead?.rows[0].head_commit_id, mainCommitId);
      },
    );

    itWithDb("should honor user-provided conflict resolutions", async () => {
      const sql = getSql({ camelize: false });
      const repoResult = await sql`
        insert into
          fs.repositories (name)
        values
          ('merge-conflict-resolution')
        returning
          id
      `.query<{
        id: string;
      }>();
      const repoId = repoResult!.rows[0].id;

      const mainBranch = await sql`
        select
          id,
          head_commit_id
        from
          fs.branches
        where
          repository_id = ${repoId}
          and name = 'main'
      `.query<{
        id: string;
        head_commit_id: string;
      }>();
      const mainBranchId = mainBranch!.rows[0].id;
      const rootHeadId = mainBranch!.rows[0].head_commit_id;

      const baseCommit = await sql`
        insert into
          fs.commits (repository_id, message, parent_commit_id)
        values
          (
            ${repoId},
            'base',
            ${rootHeadId}
          )
        returning
          id
      `.query<{
        id: string;
      }>();
      const baseCommitId = baseCommit!.rows[0].id;
      await sql`
        update fs.branches
        set
          head_commit_id = ${baseCommitId}
        where
          id = ${mainBranchId}
      `.query();
      await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${baseCommitId},
            ${"/same.txt"},
            ${"base"}
          )
      `.query();

      const featureBranch = await sql`
        insert into
          fs.branches (repository_id, name)
        values
          (${repoId}, 'feature')
        returning
          id
      `.query<{
        id: string;
      }>();
      const featureBranchId = featureBranch!.rows[0].id;

      const mainCommit = await sql`
        insert into
          fs.commits (repository_id, message, parent_commit_id)
        values
          (
            ${repoId},
            'main-1',
            ${baseCommitId}
          )
        returning
          id
      `.query<{
        id: string;
      }>();
      const mainCommitId = mainCommit!.rows[0].id;
      await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${mainCommitId},
            ${"/same.txt"},
            ${"main"}
          )
      `.query();
      await sql`
        update fs.branches
        set
          head_commit_id = ${mainCommitId}
        where
          id = ${mainBranchId}
      `.query();

      const featCommit = await sql`
        insert into
          fs.commits (repository_id, message, parent_commit_id)
        values
          (
            ${repoId},
            'feature-1',
            ${baseCommitId}
          )
        returning
          id
      `.query<{
        id: string;
      }>();
      const featCommitId = featCommit!.rows[0].id;
      await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${featCommitId},
            ${"/same.txt"},
            ${"feature"}
          )
      `.query();
      await sql`
        update fs.branches
        set
          head_commit_id = ${featCommitId}
        where
          id = ${featureBranchId}
      `.query();

      const mergeCommit = await sql`
        insert into
          fs.commits (
            repository_id,
            message,
            parent_commit_id,
            merged_from_commit_id
          )
        values
          (
            ${repoId},
            'Merge with resolution',
            ${mainCommitId},
            ${featCommitId}
          )
        returning
          id
      `.query<{
        id: string;
      }>();
      const mergeCommitId = mergeCommit!.rows[0].id;

      await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${mergeCommitId},
            ${"/same.txt"},
            ${"resolved"}
          )
      `.query();

      const mergeResult = await sql`
        select
          operation,
          merge_commit_id,
          new_target_head_commit_id
        from
          fs.finalize_commit (
            ${mergeCommitId},
            ${mainBranchId}
          )
      `.query<{
        operation: string;
        merge_commit_id: string | null;
        new_target_head_commit_id: string;
      }>();

      assert.strictEqual(mergeResult?.rows.length, 1);
      assert.strictEqual(
        mergeResult?.rows[0].operation,
        "merged_with_conflicts_resolved",
      );
      assert.strictEqual(mergeResult?.rows[0].merge_commit_id, mergeCommitId);
      assert.strictEqual(
        mergeResult?.rows[0].new_target_head_commit_id,
        mergeCommitId,
      );

      const resolved = await sql`
        select
          fs.read_file (${mergeCommitId}, '/same.txt') as content
      `.query<{
        content: string | null;
      }>();
      assert.strictEqual(resolved?.rows[0].content, "resolved");
    });

    itWithDb(
      "should report already_up_to_date when source is ancestor of target",
      async () => {
        const sql = getSql({ camelize: false });
        const repoResult = await sql`
          insert into
            fs.repositories (name)
          values
            ('merge-already-up-to-date')
          returning
            id
        `.query<{
          id: string;
        }>();
        const repoId = repoResult!.rows[0].id;

        const mainBranch = await sql`
          select
            id,
            head_commit_id
          from
            fs.branches
          where
            repository_id = ${repoId}
            and name = 'main'
        `.query<{
          id: string;
          head_commit_id: string;
        }>();
        const mainBranchId = mainBranch!.rows[0].id;
        const rootHeadId = mainBranch!.rows[0].head_commit_id;

        const baseCommit = await sql`
          insert into
            fs.commits (repository_id, message, parent_commit_id)
          values
            (
              ${repoId},
              'base',
              ${rootHeadId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const baseCommitId = baseCommit!.rows[0].id;
        await sql`
          update fs.branches
          set
            head_commit_id = ${baseCommitId}
          where
            id = ${mainBranchId}
        `.query();
        await sql`
          insert into
            fs.files (commit_id, path, content)
          values
            (
              ${baseCommitId},
              ${"/same.txt"},
              ${"base"}
            )
        `.query();

        const mainCommit = await sql`
          insert into
            fs.commits (repository_id, message, parent_commit_id)
          values
            (
              ${repoId},
              'main-1',
              ${baseCommitId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const mainCommitId = mainCommit!.rows[0].id;
        await sql`
          insert into
            fs.files (commit_id, path, content)
          values
            (
              ${mainCommitId},
              ${"/same.txt"},
              ${"same"}
            )
        `.query();
        await sql`
          update fs.branches
          set
            head_commit_id = ${mainCommitId}
          where
            id = ${mainBranchId}
        `.query();

        const mergeCommit = await sql`
          insert into
            fs.commits (
              repository_id,
              message,
              parent_commit_id,
              merged_from_commit_id
            )
          values
            (
              ${repoId},
              'Merge noop',
              ${mainCommitId},
              ${baseCommitId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const mergeCommitId = mergeCommit!.rows[0].id;

        const mergeResult = await sql`
          select
            operation,
            merge_commit_id,
            new_target_head_commit_id,
            applied_file_count
          from
            fs.finalize_commit (
              ${mergeCommitId},
              ${mainBranchId}
            )
        `.query<{
          operation: string;
          merge_commit_id: string | null;
          new_target_head_commit_id: string;
          applied_file_count: number;
        }>();

        assert.strictEqual(mergeResult?.rows.length, 1);
        assert.strictEqual(
          mergeResult?.rows[0].operation,
          "already_up_to_date",
        );
        assert.strictEqual(mergeResult?.rows[0].merge_commit_id, mergeCommitId);
        assert.strictEqual(
          mergeResult?.rows[0].new_target_head_commit_id,
          mergeCommitId,
        );
        assert.strictEqual(mergeResult?.rows[0].applied_file_count, 0);
      },
    );

    itWithDb(
      "should fast-forward rebase when branch is behind onto",
      async () => {
        const sql = getSql({ camelize: false });
        const repoResult = await sql`
          insert into
            fs.repositories (name)
          values
            ('rebase-ff')
          returning
            id
        `.query<{
          id: string;
        }>();
        const repoId = repoResult!.rows[0].id;

        const mainBranch = await sql`
          select
            id,
            head_commit_id
          from
            fs.branches
          where
            repository_id = ${repoId}
            and name = 'main'
        `.query<{
          id: string;
          head_commit_id: string;
        }>();
        const mainBranchId = mainBranch!.rows[0].id;
        const rootHeadId = mainBranch!.rows[0].head_commit_id;

        const baseCommit = await sql`
          insert into
            fs.commits (repository_id, message, parent_commit_id)
          values
            (
              ${repoId},
              'base',
              ${rootHeadId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const baseCommitId = baseCommit!.rows[0].id;
        await sql`
          update fs.branches
          set
            head_commit_id = ${baseCommitId}
          where
            id = ${mainBranchId}
        `.query();

        const featureBranch = await sql`
          insert into
            fs.branches (repository_id, name)
          values
            (${repoId}, 'feature')
          returning
            id,
            head_commit_id
        `.query<{
          id: string;
          head_commit_id: string;
        }>();
        const featureBranchId = featureBranch!.rows[0].id;
        assert.strictEqual(featureBranch!.rows[0].head_commit_id, baseCommitId);

        const mainCommit = await sql`
          insert into
            fs.commits (repository_id, message, parent_commit_id)
          values
            (
              ${repoId},
              'main-1',
              ${baseCommitId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const mainCommitId = mainCommit!.rows[0].id;
        await sql`
          insert into
            fs.files (commit_id, path, content)
          values
            (
              ${mainCommitId},
              ${"/main.txt"},
              ${"main"}
            )
        `.query();
        await sql`
          update fs.branches
          set
            head_commit_id = ${mainCommitId}
          where
            id = ${mainBranchId}
        `.query();

        const rebaseResult = await sql`
          select
            operation,
            rebased_commit_id,
            new_branch_head_commit_id,
            applied_file_count
          from
            fs.rebase_branch (
              ${featureBranchId},
              ${mainBranchId},
              ${"Rebase feature onto main"}
            )
        `.query<{
          operation: string;
          rebased_commit_id: string | null;
          new_branch_head_commit_id: string;
          applied_file_count: number;
        }>();
        assert.strictEqual(rebaseResult?.rows[0].operation, "fast_forward");
        assert.strictEqual(rebaseResult?.rows[0].rebased_commit_id, null);
        assert.strictEqual(
          rebaseResult?.rows[0].new_branch_head_commit_id,
          mainCommitId,
        );
        assert.strictEqual(rebaseResult?.rows[0].applied_file_count, 0);

        const featureHead = await sql`
          select
            head_commit_id
          from
            fs.branches
          where
            id = ${featureBranchId}
        `.query<{
          head_commit_id: string;
        }>();
        assert.strictEqual(featureHead?.rows[0].head_commit_id, mainCommitId);
      },
    );

    itWithDb(
      "should rebase diverged branch by creating a new linear commit (no conflicts)",
      async () => {
        const sql = getSql({ camelize: false });
        const repoResult = await sql`
          insert into
            fs.repositories (name)
          values
            ('rebase-diverged')
          returning
            id
        `.query<{
          id: string;
        }>();
        const repoId = repoResult!.rows[0].id;

        const mainBranch = await sql`
          select
            id,
            head_commit_id
          from
            fs.branches
          where
            repository_id = ${repoId}
            and name = 'main'
        `.query<{
          id: string;
          head_commit_id: string;
        }>();
        const mainBranchId = mainBranch!.rows[0].id;
        const rootHeadId = mainBranch!.rows[0].head_commit_id;

        const baseCommit = await sql`
          insert into
            fs.commits (repository_id, message, parent_commit_id)
          values
            (
              ${repoId},
              'base',
              ${rootHeadId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const baseCommitId = baseCommit!.rows[0].id;
        await sql`
          update fs.branches
          set
            head_commit_id = ${baseCommitId}
          where
            id = ${mainBranchId}
        `.query();

        const featureBranch = await sql`
          insert into
            fs.branches (repository_id, name)
          values
            (${repoId}, 'feature')
          returning
            id,
            head_commit_id
        `.query<{
          id: string;
          head_commit_id: string;
        }>();
        const featureBranchId = featureBranch!.rows[0].id;
        assert.strictEqual(featureBranch!.rows[0].head_commit_id, baseCommitId);

        const featCommit = await sql`
          insert into
            fs.commits (repository_id, message, parent_commit_id)
          values
            (
              ${repoId},
              'feature-1',
              ${baseCommitId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const featCommitId = featCommit!.rows[0].id;
        await sql`
          insert into
            fs.files (commit_id, path, content)
          values
            (
              ${featCommitId},
              ${"/feature.txt"},
              ${"feature"}
            )
        `.query();
        await sql`
          update fs.branches
          set
            head_commit_id = ${featCommitId}
          where
            id = ${featureBranchId}
        `.query();

        const mainCommit = await sql`
          insert into
            fs.commits (repository_id, message, parent_commit_id)
          values
            (
              ${repoId},
              'main-1',
              ${baseCommitId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const mainCommitId = mainCommit!.rows[0].id;
        await sql`
          insert into
            fs.files (commit_id, path, content)
          values
            (
              ${mainCommitId},
              ${"/main.txt"},
              ${"main"}
            )
        `.query();
        await sql`
          update fs.branches
          set
            head_commit_id = ${mainCommitId}
          where
            id = ${mainBranchId}
        `.query();

        const rebaseResult = await sql`
          select
            operation,
            rebased_commit_id,
            new_branch_head_commit_id,
            applied_file_count
          from
            fs.rebase_branch (
              ${featureBranchId},
              ${mainBranchId},
              ${"Rebase feature onto main"}
            )
        `.query<{
          operation: string;
          rebased_commit_id: string | null;
          new_branch_head_commit_id: string;
          applied_file_count: number;
        }>();

        assert.strictEqual(rebaseResult?.rows.length, 1);
        assert.strictEqual(rebaseResult?.rows[0].operation, "rebased");
        assert.ok(rebaseResult?.rows[0].rebased_commit_id);
        assert.strictEqual(
          rebaseResult?.rows[0].rebased_commit_id,
          rebaseResult?.rows[0].new_branch_head_commit_id,
        );
        assert.strictEqual(rebaseResult?.rows[0].applied_file_count, 1);

        const rebasedCommitId = rebaseResult!.rows[0].rebased_commit_id!;

        const parent = await sql`
          select
            parent_commit_id
          from
            fs.commits
          where
            id = ${rebasedCommitId}
        `.query<{
          parent_commit_id: string;
        }>();
        assert.strictEqual(parent?.rows[0].parent_commit_id, mainCommitId);

        const snapshot = await sql`
          select
            path
          from
            fs.get_commit_snapshot (${rebasedCommitId})
          order by
            path
        `.query<{
          path: string;
        }>();
        assert.deepStrictEqual(
          snapshot?.rows.map((r) => r.path),
          ["/feature.txt", "/main.txt"],
        );

        const feature = await sql`
          select
            fs.read_file (
              ${rebasedCommitId},
              '/feature.txt'
            ) as content
        `.query<{
          content: string | null;
        }>();
        assert.strictEqual(feature?.rows[0].content, "feature");

        const main = await sql`
          select
            fs.read_file (${rebasedCommitId}, '/main.txt') as content
        `.query<{
          content: string | null;
        }>();
        assert.strictEqual(main?.rows[0].content, "main");
      },
    );

    itWithDb(
      "should fail rebase on conflict and leave branch head unchanged",
      async () => {
        const sql = getSql({ camelize: false });
        const repoResult = await sql`
          insert into
            fs.repositories (name)
          values
            ('rebase-conflict')
          returning
            id
        `.query<{
          id: string;
        }>();
        const repoId = repoResult!.rows[0].id;

        const mainBranch = await sql`
          select
            id,
            head_commit_id
          from
            fs.branches
          where
            repository_id = ${repoId}
            and name = 'main'
        `.query<{
          id: string;
          head_commit_id: string;
        }>();
        const mainBranchId = mainBranch!.rows[0].id;
        const rootHeadId = mainBranch!.rows[0].head_commit_id;

        const baseCommit = await sql`
          insert into
            fs.commits (repository_id, message, parent_commit_id)
          values
            (
              ${repoId},
              'base',
              ${rootHeadId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const baseCommitId = baseCommit!.rows[0].id;
        await sql`
          update fs.branches
          set
            head_commit_id = ${baseCommitId}
          where
            id = ${mainBranchId}
        `.query();
        await sql`
          insert into
            fs.files (commit_id, path, content)
          values
            (
              ${baseCommitId},
              ${"/same.txt"},
              ${"base"}
            )
        `.query();

        const featureBranch = await sql`
          insert into
            fs.branches (repository_id, name)
          values
            (${repoId}, 'feature')
          returning
            id
        `.query<{
          id: string;
        }>();
        const featureBranchId = featureBranch!.rows[0].id;

        const featCommit = await sql`
          insert into
            fs.commits (repository_id, message, parent_commit_id)
          values
            (
              ${repoId},
              'feature-1',
              ${baseCommitId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const featCommitId = featCommit!.rows[0].id;
        await sql`
          insert into
            fs.files (commit_id, path, content)
          values
            (
              ${featCommitId},
              ${"/same.txt"},
              ${"feature"}
            )
        `.query();
        await sql`
          update fs.branches
          set
            head_commit_id = ${featCommitId}
          where
            id = ${featureBranchId}
        `.query();

        const mainCommit = await sql`
          insert into
            fs.commits (repository_id, message, parent_commit_id)
          values
            (
              ${repoId},
              'main-1',
              ${baseCommitId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const mainCommitId = mainCommit!.rows[0].id;
        await sql`
          insert into
            fs.files (commit_id, path, content)
          values
            (
              ${mainCommitId},
              ${"/same.txt"},
              ${"main"}
            )
        `.query();
        await sql`
          update fs.branches
          set
            head_commit_id = ${mainCommitId}
          where
            id = ${mainBranchId}
        `.query();

        try {
          await sql`
            select
              *
            from
              fs.rebase_branch (
                ${featureBranchId},
                ${mainBranchId},
                ${"Rebase with conflict"}
              )
          `.query();
          assert.fail("Expected rebase to fail on conflict");
        } catch (err: any) {
          assert.match(err.message, /Rebase blocked by/);
        }

        const featureHead = await sql`
          select
            head_commit_id
          from
            fs.branches
          where
            id = ${featureBranchId}
        `.query<{
          head_commit_id: string;
        }>();
        assert.strictEqual(featureHead?.rows[0].head_commit_id, featCommitId);
      },
    );

    itWithDb(
      "should noop rebase when onto head is already an ancestor of the branch head",
      async () => {
        const sql = getSql({ camelize: false });
        const repoResult = await sql`
          insert into
            fs.repositories (name)
          values
            ('rebase-noop')
          returning
            id
        `.query<{
          id: string;
        }>();
        const repoId = repoResult!.rows[0].id;

        const mainBranch = await sql`
          select
            id,
            head_commit_id
          from
            fs.branches
          where
            repository_id = ${repoId}
            and name = 'main'
        `.query<{
          id: string;
          head_commit_id: string;
        }>();
        const mainBranchId = mainBranch!.rows[0].id;
        const rootHeadId = mainBranch!.rows[0].head_commit_id;

        const baseCommit = await sql`
          insert into
            fs.commits (repository_id, message, parent_commit_id)
          values
            (
              ${repoId},
              'base',
              ${rootHeadId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const baseCommitId = baseCommit!.rows[0].id;
        await sql`
          update fs.branches
          set
            head_commit_id = ${baseCommitId}
          where
            id = ${mainBranchId}
        `.query();

        const featureBranch = await sql`
          insert into
            fs.branches (repository_id, name)
          values
            (${repoId}, 'feature')
          returning
            id,
            head_commit_id
        `.query<{
          id: string;
          head_commit_id: string;
        }>();
        const featureBranchId = featureBranch!.rows[0].id;
        assert.strictEqual(featureBranch!.rows[0].head_commit_id, baseCommitId);

        const featCommit = await sql`
          insert into
            fs.commits (repository_id, message, parent_commit_id)
          values
            (
              ${repoId},
              'feature-1',
              ${baseCommitId}
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const featCommitId = featCommit!.rows[0].id;
        await sql`
          update fs.branches
          set
            head_commit_id = ${featCommitId}
          where
            id = ${featureBranchId}
        `.query();

        const rebaseResult = await sql`
          select
            operation,
            rebased_commit_id,
            new_branch_head_commit_id
          from
            fs.rebase_branch (
              ${featureBranchId},
              ${mainBranchId},
              ${"Rebase noop"}
            )
        `.query<{
          operation: string;
          rebased_commit_id: string | null;
          new_branch_head_commit_id: string;
        }>();

        assert.strictEqual(
          rebaseResult?.rows[0].operation,
          "already_up_to_date",
        );
        assert.strictEqual(rebaseResult?.rows[0].rebased_commit_id, null);
        assert.strictEqual(
          rebaseResult?.rows[0].new_branch_head_commit_id,
          featCommitId,
        );
      },
    );
  });

  describe("Content Browsing", () => {
    itWithDb(
      "should browse commit delta with fs.get_commit_delta",
      async () => {
        const sql = getSql({ camelize: false });
        // Create repository
        const repoResult = await sql`
          insert into
            fs.repositories (name)
          values
            ('browse-test')
          returning
            *
        `.query<{
          id: string;
          name: string;
          created_at: string;
        }>();
        const repoId = repoResult!.rows[0].id;

        // Get the default branch ID (created by the AFTER INSERT trigger)
        const branchResult = await sql`
          select
            default_branch_id
          from
            fs.repositories
          where
            id = ${repoId}
        `.query<{
          default_branch_id: string;
        }>();
        const branchId = branchResult!.rows[0].default_branch_id;

        // Create a commit with some files
        const commitResult = await sql`
          insert into
            fs.commits (repository_id, message)
          values
            (${repoId}, 'Test commit')
          returning
            id
        `.query<{
          id: string;
        }>();
        const commitId = commitResult!.rows[0].id;

        // Manually update branch head
        await sql`
          update fs.branches
          set
            head_commit_id = ${commitId}
          where
            id = ${branchId}
        `.query();

        // Add some files
        await sql`
          insert into
            fs.files (commit_id, path, content)
          values
            (
              ${commitId},
              '/index.html',
              '<h1>Hello</h1>'
            ),
            (
              ${commitId},
              '/styles.css',
              'body { color: red; }'
            ),
            (
              ${commitId},
              '/script.js',
              'console.log("hi");'
            )
        `.query();

        const contents = await sql`
          select
            repository_id,
            repository_name,
            commit_id,
            path,
            is_deleted,
            is_symlink
          from
            fs.get_commit_delta (${commitId})
          order by
            path
        `.query<{
          repository_id: string;
          repository_name: string;
          commit_id: string;
          path: string;
          is_deleted: boolean;
          is_symlink: boolean;
        }>();

        assert.strictEqual(contents?.rows.length, 3);
        assert.strictEqual(contents?.rows[0].repository_name, "browse-test");
        assert.strictEqual(contents?.rows[0].commit_id, commitId);
        assert.strictEqual(contents?.rows[0].path, "/index.html");
        assert.strictEqual(contents?.rows[0].is_deleted, false);
        assert.strictEqual(contents?.rows[0].is_symlink, false);
        assert.strictEqual(contents?.rows[1].path, "/script.js");
        assert.strictEqual(contents?.rows[2].path, "/styles.css");

        const html = await sql`
          select
            fs.read_file (${commitId}, '/index.html') as content
        `.query<{
          content: string | null;
        }>();
        assert.strictEqual(html?.rows[0].content, "<h1>Hello</h1>");
      },
    );

    itWithDb(
      "should browse commit snapshot with fs.get_commit_snapshot",
      async () => {
        const sql = getSql({ camelize: false });
        // Create repository
        const repoResult = await sql`
          insert into
            fs.repositories (name)
          values
            ('browse-test')
          returning
            *
        `.query<{
          id: string;
          name: string;
          created_at: string;
        }>();
        const repoId = repoResult!.rows[0].id;

        // Get the default branch ID (created by the AFTER INSERT trigger)
        const branchResult = await sql`
          select
            default_branch_id
          from
            fs.repositories
          where
            id = ${repoId}
        `.query<{
          default_branch_id: string;
        }>();
        const branchId = branchResult!.rows[0].default_branch_id;

        // Create a commit with some files
        const commitResult = await sql`
          insert into
            fs.commits (repository_id, message)
          values
            (${repoId}, 'Test commit')
          returning
            id
        `.query<{
          id: string;
        }>();
        const commitId = commitResult!.rows[0].id;

        // Manually update branch head
        await sql`
          update fs.branches
          set
            head_commit_id = ${commitId}
          where
            id = ${branchId}
        `.query();

        // Add some files
        await sql`
          insert into
            fs.files (commit_id, path, content)
          values
            (
              ${commitId},
              '/index.html',
              '<h1>Hello</h1>'
            ),
            (
              ${commitId},
              '/styles.css',
              'body { color: red; }'
            ),
            (
              ${commitId},
              '/script.js',
              'console.log("hi");'
            )
        `.query();

        const snapshot = await sql`
          select
            repository_id,
            repository_name,
            commit_id,
            path,
            is_symlink,
            commit_created_at,
            commit_message
          from
            fs.get_commit_snapshot (${commitId})
          order by
            path
        `.query<{
          repository_id: string;
          repository_name: string;
          commit_id: string;
          path: string;
          is_symlink: boolean;
          commit_created_at: string;
          commit_message: string;
        }>();

        assert.strictEqual(snapshot?.rows.length, 3);
        assert.strictEqual(snapshot?.rows[0].repository_name, "browse-test");
        assert.strictEqual(snapshot?.rows[0].commit_id, commitId);
        assert.strictEqual(snapshot?.rows[0].commit_message, "Test commit");
        assert.ok(snapshot?.rows[0].commit_created_at);
        assert.strictEqual(snapshot?.rows[0].path, "/index.html");
        assert.strictEqual(snapshot?.rows[1].path, "/script.js");
        assert.strictEqual(snapshot?.rows[2].path, "/styles.css");

        const html = await sql`
          select
            fs.read_file (${commitId}, '/index.html') as content
        `.query<{
          content: string | null;
        }>();
        assert.strictEqual(html?.rows[0].content, "<h1>Hello</h1>");
      },
    );

    itWithDb(
      "should browse branch delta using fs.get_commit_delta with branch resolution",
      async () => {
        const sql = getSql({ camelize: false });
        // Create repository
        const repoResult = await sql`
          insert into
            fs.repositories (name)
          values
            ('browse-test')
          returning
            *
        `.query<{
          id: string;
          name: string;
          created_at: string;
        }>();
        const repoId = repoResult!.rows[0].id;

        // Get the default branch ID (created by the AFTER INSERT trigger)
        const branchResult = await sql`
          select
            default_branch_id
          from
            fs.repositories
          where
            id = ${repoId}
        `.query<{
          default_branch_id: string;
        }>();
        const branchId = branchResult!.rows[0].default_branch_id;

        // Create a commit with some files
        const commitResult = await sql`
          insert into
            fs.commits (repository_id, message)
          values
            (${repoId}, 'Test commit')
          returning
            id
        `.query<{
          id: string;
        }>();
        const commitId = commitResult!.rows[0].id;

        // Manually update branch head
        await sql`
          update fs.branches
          set
            head_commit_id = ${commitId}
          where
            id = ${branchId}
        `.query();

        // Add some files
        await sql`
          insert into
            fs.files (commit_id, path, content)
          values
            (
              ${commitId},
              '/index.html',
              '<h1>Hello</h1>'
            ),
            (
              ${commitId},
              '/styles.css',
              'body { color: red; }'
            ),
            (
              ${commitId},
              '/script.js',
              'console.log("hi");'
            )
        `.query();

        const contents = await sql`
          select
            gcd.repository_id,
            gcd.repository_name,
            gcd.commit_id,
            gcd.path,
            gcd.is_deleted,
            gcd.is_symlink,
            b.name as branch_name
          from
            fs.get_commit_delta (
              (
                select
                  head_commit_id
                from
                  fs.branches
                where
                  id = ${branchId}
              )
            ) gcd
            cross join fs.branches b
          where
            b.id = ${branchId}
          order by
            gcd.path
        `.query<{
          repository_id: string;
          repository_name: string;
          commit_id: string;
          path: string;
          is_deleted: boolean;
          is_symlink: boolean;
          branch_name: string;
        }>();

        assert.strictEqual(contents?.rows.length, 3);
        assert.strictEqual(contents?.rows[0].repository_name, "browse-test");
        assert.strictEqual(contents?.rows[0].branch_name, "main");
        assert.strictEqual(contents?.rows[0].commit_id, commitId);
        assert.strictEqual(contents?.rows[0].path, "/index.html");
        assert.strictEqual(contents?.rows[1].path, "/script.js");
        assert.strictEqual(contents?.rows[2].path, "/styles.css");
      },
    );

    itWithDb(
      "should return empty result for commit with no files",
      async () => {
        const sql = getSql({ camelize: false });
        // Create repository
        const repoResult = await sql`
          insert into
            fs.repositories (name)
          values
            ('browse-test')
          returning
            *
        `.query<{
          id: string;
          name: string;
          created_at: string;
        }>();
        const repoId = repoResult!.rows[0].id;

        // Get the default branch ID (created by the AFTER INSERT trigger)
        const branchResult = await sql`
          select
            default_branch_id
          from
            fs.repositories
          where
            id = ${repoId}
        `.query<{
          default_branch_id: string;
        }>();
        const branchId = branchResult!.rows[0].default_branch_id;

        // Create a commit with no files
        const emptyCommitResult = await sql`
          insert into
            fs.commits (repository_id, message)
          values
            (
              ${repoId},
              'Empty commit'
            )
          returning
            id
        `.query<{
          id: string;
        }>();
        const emptyCommitId = emptyCommitResult!.rows[0].id;

        // Manually update branch head
        await sql`
          update fs.branches
          set
            head_commit_id = ${emptyCommitId}
          where
            id = ${branchId}
        `.query();

        const contents = await sql`
          select
            *
          from
            fs.get_commit_delta (${emptyCommitId})
        `.query();

        assert.strictEqual(contents?.rows.length, 0);
      },
    );

    itWithDb("should include commit metadata", async () => {
      const sql = getSql({ camelize: false });
      // Create repository
      const repoResult = await sql`
        insert into
          fs.repositories (name)
        values
          ('browse-test')
        returning
          *
      `.query<{
        id: string;
        name: string;
        created_at: string;
      }>();
      const repoId = repoResult!.rows[0].id;

      // Get the default branch ID (created by the AFTER INSERT trigger)
      const branchResult = await sql`
        select
          default_branch_id
        from
          fs.repositories
        where
          id = ${repoId}
      `.query<{
        default_branch_id: string;
      }>();
      const branchId = branchResult!.rows[0].default_branch_id;

      // Create a commit with some files
      const commitResult = await sql`
        insert into
          fs.commits (repository_id, message)
        values
          (${repoId}, 'Test commit')
        returning
          id
      `.query<{
        id: string;
      }>();
      const commitId = commitResult!.rows[0].id;

      // Manually update branch head
      await sql`
        update fs.branches
        set
          head_commit_id = ${commitId}
        where
          id = ${branchId}
      `.query();

      // Add some files
      await sql`
        insert into
          fs.files (commit_id, path, content)
        values
          (
            ${commitId},
            '/index.html',
            '<h1>Hello</h1>'
          ),
          (
            ${commitId},
            '/styles.css',
            'body { color: red; }'
          ),
          (
            ${commitId},
            '/script.js',
            'console.log("hi");'
          )
      `.query();

      const contents = await sql`
        select
          commit_created_at,
          commit_message
        from
          fs.get_commit_delta (${commitId})
        limit
          1
      `.query<{
        commit_created_at: string;
        commit_message: string;
      }>();

      assert.ok(contents?.rows[0].commit_created_at);
      assert.strictEqual(contents?.rows[0].commit_message, "Test commit");
    });
  });
});
