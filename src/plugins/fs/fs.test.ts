import { PGlite, type PGliteInterface } from "@electric-sql/pglite";
import * as assert from "node:assert";
import * as fs from "node:fs/promises";
import { beforeEach, describe, it } from "node:test";
import { QueryableStatement, type Interpolable } from "../../sql2.ts";
import { fsPlugin } from "./index.ts";

const dbRoot = new PGlite();
let db: PGliteInterface | null = null;

class RootStatement extends QueryableStatement {
  async exec() {
    if (this.values.length) throw new Error("No parameters are provided.");
    await dbRoot.exec(this.compile());
  }
  async query<T>(): Promise<{ rows: T[] }> {
    return dbRoot.query(this.compile(), this.values);
  }
}

await fsPlugin(
  (strings: TemplateStringsArray, ...values: Interpolable[]) =>
    new RootStatement(strings, values)
);

class TestStatement extends QueryableStatement {
  async exec() {
    if (this.values.length) throw new Error("No parameters are provided.");
    await db!.exec(this.compile());
  }
  async query<T>(): Promise<{ rows: T[] }> {
    return db!.query(this.compile(), this.values);
  }
}

let sql = (strings: TemplateStringsArray, ...values: Interpolable[]) =>
  new TestStatement(strings, values);

describe("SQL Filesystem with Version Control", () => {
  beforeEach(async () => {
    db = await dbRoot.clone();
  });

  describe("Basic Operations", () => {
    it("should create a repository", async () => {
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('test-repo') RETURNING *`.query<{
          id: string;
          name: string;
          default_branch_id: string;
          created_at: string;
        }>();
      const repoId = repoResult?.rows[0].id;

      const result =
        await sql`SELECT name FROM fs.repositories WHERE id = ${repoId}`.query<{
          name: string;
        }>();

      assert.strictEqual(result?.rows.length, 1);
      assert.strictEqual(result?.rows[0].name, "test-repo");
    });

    it("should create fs.commits", async () => {
      // Create repository
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('test-repo') RETURNING *`.query<{
          id: string;
          name: string;
          default_branch_id: string;
          created_at: string;
        }>();
      const repoId = repoResult?.rows[0].id;

      // Create additional commit
      await sql`INSERT INTO fs.commits (repository_id, parent_commit_id, message) VALUES (${repoId}, (SELECT head_commit_id FROM fs.branches WHERE repository_id = ${repoId} AND name = 'main'), 'Additional commit')`.query();

      const commitResult =
        await sql`SELECT message FROM fs.commits WHERE message = 'Additional commit'`.query<{
          message: string;
        }>();

      assert.strictEqual(commitResult?.rows.length, 1);
      assert.strictEqual(commitResult?.rows[0].message, "Additional commit");
    });

    it("should write and read files", async () => {
      // Create repository
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('test-repo') RETURNING *`.query<{
          id: string;
          name: string;
          default_branch_id: string;
          created_at: string;
        }>();
      const repoId = repoResult?.rows[0].id;

      await sql`INSERT INTO fs.commits (repository_id, parent_commit_id, message) VALUES (${repoId}, NULL, 'Initial commit')`.query();

      const commitResult =
        await sql`SELECT id FROM fs.commits WHERE message = 'Initial commit'`.query<{
          id: string;
        }>();
      const commitId = commitResult?.rows[0].id;

      // Write a file
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${commitId}, '/test.txt', 'Hello World')`.query();

      // Read the file
      const fileResult =
        await sql`SELECT fs.read_file(${commitId}, '/test.txt') as content`.query<{
          content: string;
        }>();

      assert.strictEqual(fileResult?.rows[0].content, "Hello World");
    });
  });

  describe("Version Control", () => {
    let repoId: string;
    let commit1Id: string;
    let commit2Id: string;

    beforeEach(async () => {
      // Create repository
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('version-test') RETURNING *`.query<{
          id: string;
          name: string;
          default_branch_id: string;
          created_at: string;
        }>();
      repoId = repoResult!.rows[0].id;

      // Create first commit
      await sql`INSERT INTO fs.commits (repository_id, parent_commit_id, message) VALUES (${repoId}, NULL, 'Commit 1')`.query();

      const commit1Result =
        await sql`SELECT id FROM fs.commits WHERE message = 'Commit 1'`.query<{
          id: string;
        }>();
      commit1Id = commit1Result?.rows[0].id!;

      // Create second commit
      await sql`INSERT INTO fs.commits (repository_id, parent_commit_id, message) VALUES (${repoId}, ${commit1Id}, 'Commit 2')`.query();

      const commit2Result =
        await sql`SELECT id FROM fs.commits WHERE message = 'Commit 2'`.query<{
          id: string;
        }>();
      commit2Id = commit2Result?.rows[0].id!;
    });

    it("should cascade file reads through commit history", async () => {
      // Write file in commit 1
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${commit1Id}, '/persistent.txt', 'Version 1')`.query();

      // File should be readable from both fs.commits
      const result1 =
        await sql`SELECT fs.read_file(${commit1Id}, '/persistent.txt') as content`.query<{
          content: string;
        }>();

      const result2 =
        await sql`SELECT fs.read_file(${commit2Id}, '/persistent.txt') as content`.query<{
          content: string;
        }>();

      assert.strictEqual(result1?.rows[0].content, "Version 1");
      assert.strictEqual(result2?.rows[0].content, "Version 1");
    });

    it("should override files in newer fs.commits", async () => {
      // Write file in commit 1
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${commit1Id}, '/changing.txt', 'Version 1')`.query();

      // Override in commit 2
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${commit2Id}, '/changing.txt', 'Version 2')`.query();

      // Check versions
      const result1 =
        await sql`SELECT fs.read_file(${commit1Id}, '/changing.txt') as content`.query<{
          content: string;
        }>();

      const result2 =
        await sql`SELECT fs.read_file(${commit2Id}, '/changing.txt') as content`.query<{
          content: string;
        }>();

      assert.strictEqual(result1?.rows[0].content, "Version 1");
      assert.strictEqual(result2?.rows[0].content, "Version 2");
    });

    it("should list files from commit and ancestors", async () => {
      // Files in commit 1
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${commit1Id}, '/file1.txt', 'Content 1'), (${commit1Id}, '/file2.txt', 'Content 2')`.query();

      // File in commit 2
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${commit2Id}, '/file3.txt', 'Content 3')`.query();

      const result =
        await sql`SELECT path FROM fs.get_commit_snapshot(${commit2Id}) ORDER BY path`.query<{
          path: string;
        }>();

      assert.strictEqual(result?.rows.length, 3);
      assert.strictEqual(result?.rows[0].path, "/file1.txt");
      assert.strictEqual(result?.rows[1].path, "/file2.txt");
      assert.strictEqual(result?.rows[2].path, "/file3.txt");
    });

    it("should get file history", async () => {
      // Version 1 in commit 1
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${commit1Id}, '/history.txt', 'Version 1')`.query();

      // Version 2 in commit 2
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${commit2Id}, '/history.txt', 'Version 2')`.query();

      const result =
        await sql`SELECT commit_id, content, is_deleted, is_symlink FROM fs.get_file_history(${commit2Id}, '/history.txt') ORDER BY content`.query<{
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
    it("should create fs.repositories with default fs.branches", async () => {
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('test-repo') RETURNING *`.query<{
          id: string;
          name: string;
          default_branch_id: string;
          created_at: string;
        }>();
      const repoId = repoResult?.rows[0].id;

      // Verify repository was created
      const repoCheck =
        await sql`SELECT name, default_branch_id FROM fs.repositories WHERE id = ${repoId}`.query<{
          name: string;
          default_branch_id: string;
        }>();
      assert.strictEqual(repoCheck?.rows[0].name, "test-repo");
      assert.ok(repoCheck?.rows[0].default_branch_id);

      // Verify default branch was created
      const branchCheck =
        await sql`SELECT name, repository_id, head_commit_id FROM fs.branches WHERE repository_id = ${repoId}`.query<{
          name: string;
          repository_id: string;
          head_commit_id: string | null;
        }>();
      assert.strictEqual(branchCheck?.rows.length, 1);
      assert.strictEqual(branchCheck?.rows[0].name, "main");
      assert.strictEqual(branchCheck?.rows[0].repository_id, repoId);
      assert.strictEqual(branchCheck?.rows[0].head_commit_id, null); // No initial commit by default

      // Verify no initial commit was created
      const commitCheck =
        await sql`SELECT repository_id, message, parent_commit_id FROM fs.commits WHERE repository_id = ${repoId}`.query<{
          repository_id: string;
          message: string;
          parent_commit_id: string | null;
        }>();
      assert.strictEqual(commitCheck?.rows.length, 0);
    });

    it("should create additional fs.branches", async () => {
      // Create repository first
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('branch-test') RETURNING *`.query<{
          id: string;
          name: string;
          default_branch_id: string;
          created_at: string;
        }>();
      const repoId = repoResult!.rows[0].id;

      // Get initial commit and branch count
      const initialBranchCount =
        await sql`SELECT COUNT(*) as count FROM fs.branches WHERE repository_id = ${repoId}`.query<{
          count: number;
        }>();
      const initialCommitCount =
        await sql`SELECT COUNT(*) as count FROM fs.commits WHERE repository_id = ${repoId}`.query<{
          count: number;
        }>();

      // Create additional branch
      const branchResult =
        await sql`INSERT INTO fs.branches (repository_id, name) VALUES (${repoId}, 'feature-branch') RETURNING *`.query<{
          id: string;
          name: string;
          head_commit_id: string;
          created_at: string;
        }>();
      const branchId = branchResult!.rows[0].id;

      // Verify branch was created
      const branchCheck =
        await sql`SELECT name, repository_id, head_commit_id FROM fs.branches WHERE id = ${branchId}`.query<{
          name: string;
          repository_id: string;
          head_commit_id: string | null;
        }>();
      assert.strictEqual(branchCheck?.rows[0].name, "feature-branch");
      assert.strictEqual(branchCheck?.rows[0].repository_id, repoId);
      // By default, new branches start from the repository default branch head
      const mainHead =
        await sql`SELECT head_commit_id FROM fs.branches WHERE repository_id = ${repoId} AND name = 'main'`.query<{
          head_commit_id: string | null;
        }>();
      assert.strictEqual(
        branchCheck?.rows[0].head_commit_id,
        mainHead?.rows[0].head_commit_id
      );

      // Verify repository now has 2 fs.branches
      const allBranches =
        await sql`SELECT COUNT(*) as count FROM fs.branches WHERE repository_id = ${repoId}`.query<{
          count: number;
        }>();
      assert.strictEqual(
        allBranches?.rows[0].count,
        initialBranchCount!.rows[0].count + 1
      );

      // Creating a branch should not create a new commit (branches point at existing commits)
      const allCommits =
        await sql`SELECT COUNT(*) as count FROM fs.commits WHERE repository_id = ${repoId}`.query<{
          count: number;
        }>();
      assert.strictEqual(
        allCommits?.rows[0].count,
        initialCommitCount!.rows[0].count
      );
    });
  });

  describe("Path Normalization", () => {
    let repoId: string;

    let commitId: string;

    beforeEach(async () => {
      // Create repository for each test
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('path-test') RETURNING *`.query<{
          id: string;
          name: string;
          default_branch_id: string;
          created_at: string;
        }>();
      repoId = repoResult!.rows[0].id;

      // Create a commit for file operations
      const commitResult =
        await sql`INSERT INTO fs.commits (repository_id, message) VALUES (${repoId}, 'Test commit') RETURNING id`.query<{
          id: string;
        }>();
      commitId = commitResult!.rows[0].id;

      // Manually update branch head
      await sql`UPDATE fs.branches SET head_commit_id = ${commitId} WHERE repository_id = ${repoId} AND name = 'main'`.query();
    });

    it("should normalize absolute paths", async () => {
      const insertResult =
        await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${commitId}, '/src/main.ts', 'content') RETURNING *`.query<{
          id: string;
          commit_id: string;
          path: string;
          content: string;
          created_at: string;
        }>();

      assert.strictEqual(insertResult?.rows[0].path, "/src/main.ts");
    });

    it("should normalize relative paths to absolute", async () => {
      const insertResult =
        await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${commitId}, 'src/main.ts', 'content') RETURNING *`.query<{
          id: string;
          commit_id: string;
          path: string;
          content: string;
          created_at: string;
        }>();

      assert.strictEqual(insertResult?.rows[0].path, "/src/main.ts");
    });

    it("should remove duplicate slashes", async () => {
      const insertResult =
        await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${commitId}, '//src//main.ts', 'content') RETURNING *`.query<{
          id: string;
          commit_id: string;
          path: string;
          content: string;
          created_at: string;
        }>();

      assert.strictEqual(insertResult?.rows[0].path, "/src/main.ts");
    });

    it("should remove trailing slashes", async () => {
      const insertResult =
        await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${commitId}, '/src/main.ts/', 'content') RETURNING *`.query<{
          id: string;
          commit_id: string;
          path: string;
          content: string;
          created_at: string;
        }>();

      assert.strictEqual(insertResult?.rows[0].path, "/src/main.ts");
    });

    it("should handle root path", async () => {
      const insertResult =
        await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${commitId}, '/', 'content') RETURNING *`.query<{
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
    it("should reject null paths", async () => {
      try {
        await sql`SELECT fs._validate_path(NULL) as _validate_path`.query();
        assert.fail("Expected validation to fail for null path");
      } catch (err: any) {
        assert.match(err.message, /Path cannot be null or empty/);
      }
    });

    it("should reject empty paths", async () => {
      try {
        await sql`SELECT fs._validate_path('') as _validate_path`.query();
        assert.fail("Expected validation to fail for empty path");
      } catch (err: any) {
        assert.match(err.message, /Path cannot be null or empty/);
      }
    });

    it("should reject paths with control characters", async () => {
      try {
        await sql`SELECT fs._validate_path('/test\x01file.txt') as _validate_path`.query();
        assert.fail(
          "Expected validation to fail for path with control characters"
        );
      } catch (err: any) {
        assert.match(err.message, /Path contains control characters/);
      }
    });

    it("should reject paths with Windows-invalid characters", async () => {
      // Test characters invalid on Windows: < > : " | ? *
      const invalidChars = ["<", ">", ":", '"', "|", "?", "*"];

      for (const char of invalidChars) {
        try {
          const path = `/test${char}file.txt`;
          await sql`SELECT fs._validate_path(${path}) as _validate_path`.query();
          assert.fail(`Expected validation to fail for path with ${char}`);
        } catch (err: any) {
          assert.match(
            err.message,
            /Path contains characters invalid on Windows/
          );
        }
      }
    });

    it("should reject paths with control characters", async () => {
      // Test control characters (0x00-0x1F except tab, newline, carriage return)
      try {
        await sql`SELECT fs._validate_path('/test' || chr(1) || 'file.txt') as _validate_path`.query();
        assert.fail(
          "Expected validation to fail for path with control character"
        );
      } catch (err: any) {
        assert.match(err.message, /Path contains control characters/);
      }

      try {
        await sql`SELECT fs._validate_path('/test' || chr(2) || 'file.txt') as _validate_path`.query();
        assert.fail(
          "Expected validation to fail for path with control character"
        );
      } catch (err: any) {
        assert.match(err.message, /Path contains control characters/);
      }
    });

    it("should reject paths with null bytes", async () => {
      try {
        await sql`SELECT fs._validate_path('/test' || chr(0) || 'file.txt') as _validate_path`.query();
        assert.fail("Expected validation to fail for path with null byte");
      } catch (err: any) {
        // PostgreSQL itself rejects null characters, so we accept either our validation error or PostgreSQL's
        assert.ok(
          err.message.includes("Path contains null bytes") ||
            err.message.includes("null character not permitted"),
          `Unexpected error: ${err.message}`
        );
      }
    });

    it("should reject very long paths", async () => {
      const longPath = "/" + "a".repeat(4100);
      try {
        await sql`SELECT fs._validate_path(${longPath}) as _validate_path`.query();
        assert.fail("Expected validation to fail for very long path");
      } catch (err: any) {
        assert.match(err.message, /Path is too long/);
      }
    });

    it("should accept valid paths", async () => {
      const result =
        await sql`SELECT fs._validate_path('/valid/path/file.txt') as _validate_path`.query<{
          _validate_path: any;
        }>();
      // Should not throw an error
      assert.ok(result);
    });
  });

  describe("Real World Usage Scenario", () => {
    it("should demonstrate basic repository and file operations", async () => {
      // Create a repository
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('demo-repo') RETURNING *`.query<{
          id: string;
          name: string;
          default_branch_id: string;
          created_at: string;
        }>();
      const repoId = repoResult!.rows[0].id;

      // Create a commit for adding files
      const commitResult =
        await sql`INSERT INTO fs.commits (repository_id, message) VALUES (${repoId}, 'Add initial files') RETURNING id`.query<{
          id: string;
        }>();
      const commitId = commitResult!.rows[0].id;

      // Manually update branch head
      await sql`UPDATE fs.branches SET head_commit_id = ${commitId} WHERE repository_id = ${repoId} AND name = 'main'`.query();

      // Add some files to the commit
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${commitId}, 'index.html', ${"<h1>Hello World</h1>"})`.query();

      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${commitId}, 'styles.css', ${"body { background: #f0f0f0; }"})`.query();

      // Verify files in initial commit
      const files =
        await sql`SELECT path FROM fs.get_commit_snapshot(${commitId})`.query<{
          path: string;
        }>();
      assert.strictEqual(files?.rows.length, 2);

      const filePaths = files?.rows.map((f) => f.path).sort();
      assert.deepStrictEqual(filePaths, ["/index.html", "/styles.css"]);

      // Verify exact file contents
      const htmlFile =
        await sql`SELECT fs.read_file(${commitId}, '/index.html') as content`.query<{
          content: string | null;
        }>();
      assert.strictEqual(htmlFile?.rows[0].content, "<h1>Hello World</h1>");

      const cssFile =
        await sql`SELECT fs.read_file(${commitId}, '/styles.css') as content`.query<{
          content: string | null;
        }>();
      assert.strictEqual(
        cssFile?.rows[0].content,
        "body { background: #f0f0f0; }"
      );

      // Create another commit for file updates
      const updateCommitResult =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'Update HTML file', ${commitId}) RETURNING id`.query<{
          id: string;
        }>();
      const updateCommitId = updateCommitResult!.rows[0].id;

      // Manually update branch head
      await sql`UPDATE fs.branches SET head_commit_id = ${updateCommitId} WHERE repository_id = ${repoId} AND name = 'main'`.query();

      // Modify a file in the new commit
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${updateCommitId}, '/index.html', ${"<h1>Hello Updated World</h1>"})`.query();

      // Verify the updated files
      const updatedFiles =
        await sql`SELECT path FROM fs.get_commit_snapshot(${updateCommitId})`.query<{
          path: string;
        }>();
      assert.strictEqual(updatedFiles?.rows.length, 2);

      const updatedFilePaths = updatedFiles?.rows.map((f) => f.path).sort();
      assert.deepStrictEqual(updatedFilePaths, ["/index.html", "/styles.css"]);

      // Verify exact updated contents
      const updatedHtmlFile =
        await sql`SELECT fs.read_file(${updateCommitId}, '/index.html') as content`.query<{
          content: string | null;
        }>();
      assert.strictEqual(
        updatedHtmlFile?.rows[0].content,
        "<h1>Hello Updated World</h1>"
      );

      const updatedCssFile =
        await sql`SELECT fs.read_file(${updateCommitId}, '/styles.css') as content`.query<{
          content: string | null;
        }>();
      assert.strictEqual(
        updatedCssFile?.rows[0].content,
        "body { background: #f0f0f0; }"
      ); // Should remain unchanged

      // Verify we have multiple fs.commits
      const commitCount =
        await sql`SELECT COUNT(*) as count FROM fs.commits WHERE repository_id = ${repoId}`.query<{
          count: number;
        }>();
      assert.ok(
        commitCount && commitCount.rows[0] && commitCount.rows[0].count >= 2
      ); // 2 manual fs.commits
    });
  });

  describe("Edge Cases", () => {
    it("should return null for non-existent files", async () => {
      // Create repository
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('edge-test') RETURNING *`.query<{
          id: string;
          name: string;
          default_branch_id: string;
          created_at: string;
        }>();
      const repoId = repoResult?.rows[0].id;

      // Create a commit manually so we have one to read from
      await sql`INSERT INTO fs.commits (repository_id, parent_commit_id, message) VALUES (${repoId}, NULL, 'Empty commit')`.query();

      const commitResult =
        await sql`SELECT id FROM fs.commits WHERE message = 'Empty commit'`.query<{
          id: string;
        }>();
      const commitId = commitResult?.rows[0].id;

      const result =
        await sql`SELECT fs.read_file(${commitId}, '/nonexistent.txt') as content`.query<{
          content: string | null;
        }>();

      assert.strictEqual(result?.rows[0].content, null);
    });

    it("should handle empty file content", async () => {
      // Create repository
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('empty-test') RETURNING *`.query<{
          id: string;
          name: string;
          default_branch_id: string;
          created_at: string;
        }>();
      const repoId = repoResult?.rows[0].id;

      // Create a commit
      await sql`INSERT INTO fs.commits (repository_id, parent_commit_id, message) VALUES (${repoId}, NULL, 'Empty file commit')`.query();

      const commitResult =
        await sql`SELECT id FROM fs.commits WHERE message = 'Empty file commit'`.query<{
          id: string;
        }>();
      const commitId = commitResult?.rows[0].id;

      // Write empty file
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${commitId}, '/empty.txt', '')`.query();

      const result =
        await sql`SELECT fs.read_file(${commitId}, '/empty.txt') as content`.query<{
          content: string;
        }>();

      assert.strictEqual(result?.rows[0].content, "");
    });
  });

  describe("Deletions", () => {
    it("should support tombstone deletions via files.is_deleted", async () => {
      // Create repository
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('delete-test') RETURNING *`.query<{
          id: string;
          name: string;
          default_branch_id: string;
          created_at: string;
        }>();
      const repoId = repoResult!.rows[0].id;

      // Create commit 1
      const commit1Result =
        await sql`INSERT INTO fs.commits (repository_id, message) VALUES (${repoId}, 'Commit 1') RETURNING id`.query<{
          id: string;
        }>();
      const commit1Id = commit1Result!.rows[0].id;

      // Update branch head
      await sql`UPDATE fs.branches SET head_commit_id = ${commit1Id} WHERE repository_id = ${repoId} AND name = 'main'`.query();

      // Write a file in commit 1
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${commit1Id}, ${"/delete-me.txt"}, ${"hello"})`.query();

      // Create commit 2
      const commit2Result =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'Commit 2', ${commit1Id}) RETURNING id`.query<{
          id: string;
        }>();
      const commit2Id = commit2Result!.rows[0].id;

      // Update branch head
      await sql`UPDATE fs.branches SET head_commit_id = ${commit2Id} WHERE repository_id = ${repoId} AND name = 'main'`.query();

      // Tombstone delete in commit 2 (no content required)
      await sql`INSERT INTO fs.files (commit_id, path, is_deleted) VALUES (${commit2Id}, ${"/delete-me.txt"}, TRUE)`.query();

      // read_file should normalize and respect tombstones
      const before =
        await sql`SELECT fs.read_file(${commit1Id}, 'delete-me.txt') as content`.query<{
          content: string | null;
        }>();
      assert.strictEqual(before?.rows[0].content, "hello");

      const after =
        await sql`SELECT fs.read_file(${commit2Id}, 'delete-me.txt') as content`.query<{
          content: string | null;
        }>();
      assert.strictEqual(after?.rows[0].content, null);

      const files =
        await sql`SELECT path FROM fs.get_commit_snapshot(${commit2Id}) ORDER BY path`.query<{
          path: string;
        }>();
      assert.deepStrictEqual(
        files?.rows.map((r) => r.path),
        []
      );

      const history =
        await sql`SELECT * FROM fs.get_file_history(${commit2Id}, ${"/delete-me.txt"})`.query<{
          commit_id: string;
          content: string | null;
          is_deleted: boolean;
          is_symlink: boolean;
        }>();
      assert.strictEqual(history?.rows.length, 2);
      assert.ok(history?.rows.some((r) => r.is_deleted && r.content === null));
      assert.ok(
        history?.rows.some((r) => !r.is_deleted && r.content === "hello")
      );
      assert.ok(history?.rows.every((r) => r.is_symlink === false));
    });
  });

  describe("Symlinks", () => {
    it("should store symlink targets as normalized absolute paths", async () => {
      // Create repository
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('symlink-test') RETURNING *`.query<{
          id: string;
          name: string;
          default_branch_id: string;
          created_at: string;
        }>();
      const repoId = repoResult!.rows[0].id;

      // Create commit
      const commitResult =
        await sql`INSERT INTO fs.commits (repository_id, message) VALUES (${repoId}, 'Add symlink') RETURNING id`.query<{
          id: string;
        }>();
      const commitId = commitResult!.rows[0].id;

      // Update branch head
      await sql`UPDATE fs.branches SET head_commit_id = ${commitId} WHERE repository_id = ${repoId} AND name = 'main'`.query();

      // Target file
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${commitId}, ${"/target.txt"}, ${"hello"})`.query();

      // Symlink file (target path is stored in content)
      await sql`INSERT INTO fs.files (commit_id, path, content, is_symlink) VALUES (${commitId}, ${"/link.txt"}, ${"target.txt"}, TRUE)`.query();

      const stored =
        await sql`SELECT path, content, is_symlink FROM fs.files WHERE commit_id = ${commitId} AND path = ${"/link.txt"}`.query<{
          path: string;
          content: string;
          is_symlink: boolean;
        }>();

      assert.strictEqual(stored?.rows.length, 1);
      assert.strictEqual(stored?.rows[0].is_symlink, true);
      assert.strictEqual(stored?.rows[0].content, "/target.txt"); // normalized to absolute

      const snapshot =
        await sql`SELECT path, is_symlink FROM fs.get_commit_snapshot(${commitId}) ORDER BY path`.query<{
          path: string;
          is_symlink: boolean;
        }>();
      assert.deepStrictEqual(
        snapshot?.rows.map((r) => r.path),
        ["/link.txt", "/target.txt"]
      );
      const link = snapshot?.rows.find((r) => r.path === "/link.txt");
      assert.strictEqual(link?.is_symlink, true);

      // read_file returns the stored content (the link target path) for now
      const read =
        await sql`SELECT fs.read_file(${commitId}, '/link.txt') as content`.query<{
          content: string | null;
        }>();
      assert.strictEqual(read?.rows[0].content, "/target.txt");

      const history =
        await sql`SELECT * FROM fs.get_file_history(${commitId}, ${"/link.txt"})`.query<{
          commit_id: string;
          content: string | null;
          is_deleted: boolean;
          is_symlink: boolean;
        }>();
      assert.strictEqual(history?.rows.length, 1);
      assert.strictEqual(history?.rows[0].is_deleted, false);
      assert.strictEqual(history?.rows[0].is_symlink, true);
      assert.strictEqual(history?.rows[0].content, "/target.txt");
    });
  });

  describe("Merge / Rebase Helpers", () => {
    it("should compute merge base for ancestor relationships", async () => {
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('merge-base-ancestor') RETURNING id`.query<{
          id: string;
        }>();
      const repoId = repoResult!.rows[0].id;

      const rootHead =
        await sql`SELECT head_commit_id FROM fs.branches WHERE repository_id = ${repoId} AND name = 'main'`.query<{
          head_commit_id: string;
        }>();
      const rootCommitId = rootHead!.rows[0].head_commit_id;

      const commitAResult =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'A', ${rootCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const commitAId = commitAResult!.rows[0].id;

      const commitBResult =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'B', ${commitAId}) RETURNING id`.query<{
          id: string;
        }>();
      const commitBId = commitBResult!.rows[0].id;

      const base1 =
        await sql`SELECT fs.get_merge_base(${commitAId}, ${commitBId}) as base`.query<{
          base: string;
        }>();
      assert.strictEqual(base1?.rows[0].base, commitAId);

      const base2 =
        await sql`SELECT fs.get_merge_base(${commitBId}, ${commitAId}) as base`.query<{
          base: string;
        }>();
      assert.strictEqual(base2?.rows[0].base, commitAId);

      const base3 =
        await sql`SELECT fs.get_merge_base(${commitBId}, ${commitBId}) as base`.query<{
          base: string;
        }>();
      assert.strictEqual(base3?.rows[0].base, commitBId);
    });

    it("should compute merge base for diverged branches", async () => {
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('merge-base-diverged') RETURNING id`.query<{
          id: string;
        }>();
      const repoId = repoResult!.rows[0].id;

      const rootHead =
        await sql`SELECT head_commit_id FROM fs.branches WHERE repository_id = ${repoId} AND name = 'main'`.query<{
          head_commit_id: string;
        }>();
      const rootCommitId = rootHead!.rows[0].head_commit_id;

      const baseResult =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'Base', ${rootCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const baseCommitId = baseResult!.rows[0].id;

      // Move main forward so the new branch defaults to this base
      await sql`UPDATE fs.branches SET head_commit_id = ${baseCommitId} WHERE repository_id = ${repoId} AND name = 'main'`.query();

      const featureBranchResult =
        await sql`INSERT INTO fs.branches (repository_id, name) VALUES (${repoId}, 'feature') RETURNING id`.query<{
          id: string;
        }>();
      const featureBranchId = featureBranchResult!.rows[0].id;

      const main1Result =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'main-1', ${baseCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const main1Id = main1Result!.rows[0].id;

      const feat1Result =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'feature-1', ${baseCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const feat1Id = feat1Result!.rows[0].id;

      // (Not required for merge base, but keep branch heads realistic)
      await sql`UPDATE fs.branches SET head_commit_id = ${main1Id} WHERE repository_id = ${repoId} AND name = 'main'`.query();
      await sql`UPDATE fs.branches SET head_commit_id = ${feat1Id} WHERE id = ${featureBranchId}`.query();

      const base =
        await sql`SELECT fs.get_merge_base(${main1Id}, ${feat1Id}) as base`.query<{
          base: string;
        }>();
      assert.strictEqual(base?.rows[0].base, baseCommitId);
      assert.notStrictEqual(base?.rows[0].base, rootCommitId);
    });

    it("should reject merge base across repositories", async () => {
      const repo1 =
        await sql`INSERT INTO fs.repositories (name) VALUES ('merge-base-repo-1') RETURNING id`.query<{
          id: string;
        }>();
      const repo2 =
        await sql`INSERT INTO fs.repositories (name) VALUES ('merge-base-repo-2') RETURNING id`.query<{
          id: string;
        }>();

      const repo1Id = repo1!.rows[0].id;
      const repo2Id = repo2!.rows[0].id;

      // Create one commit in each repo so we have valid commit ids
      const c1 =
        await sql`INSERT INTO fs.commits (repository_id, message) VALUES (${repo1Id}, 'repo-1-root') RETURNING id`.query<{
          id: string;
        }>();
      const c2 =
        await sql`INSERT INTO fs.commits (repository_id, message) VALUES (${repo2Id}, 'repo-2-root') RETURNING id`.query<{
          id: string;
        }>();

      try {
        await sql`SELECT fs.get_merge_base(${c1!.rows[0].id}, ${c2!.rows[0].id})`.query();
        assert.fail("Expected merge base to fail across repositories");
      } catch (err: any) {
        assert.match(err.message, /Commits must belong to the same repository/);
      }
    });

    it("should return no conflicts when changes do not overlap", async () => {
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('conflicts-non-overlap') RETURNING id`.query<{
          id: string;
        }>();
      const repoId = repoResult!.rows[0].id;

      const rootHead =
        await sql`SELECT head_commit_id FROM fs.branches WHERE repository_id = ${repoId} AND name = 'main'`.query<{
          head_commit_id: string;
        }>();
      const rootCommitId = rootHead!.rows[0].head_commit_id;

      const baseResult =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'Base', ${rootCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const baseCommitId = baseResult!.rows[0].id;

      const leftResult =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'Left', ${baseCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const leftCommitId = leftResult!.rows[0].id;
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${leftCommitId}, ${"/main-only.txt"}, ${"main"})`.query();

      const rightResult =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'Right', ${baseCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const rightCommitId = rightResult!.rows[0].id;
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${rightCommitId}, ${"/feature-only.txt"}, ${"feature"})`.query();

      const conflicts =
        await sql`SELECT path FROM fs.get_conflicts(${leftCommitId}, ${rightCommitId})`.query<{
          path: string;
        }>();
      assert.strictEqual(conflicts?.rows.length, 0);
    });

    it("should detect modify/modify conflicts", async () => {
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('conflicts-modify-modify') RETURNING id`.query<{
          id: string;
        }>();
      const repoId = repoResult!.rows[0].id;

      const rootHead =
        await sql`SELECT head_commit_id FROM fs.branches WHERE repository_id = ${repoId} AND name = 'main'`.query<{
          head_commit_id: string;
        }>();
      const rootCommitId = rootHead!.rows[0].head_commit_id;

      const baseResult =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'Base', ${rootCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const baseCommitId = baseResult!.rows[0].id;
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${baseCommitId}, ${"/same.txt"}, ${"base"})`.query();

      const leftResult =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'Left', ${baseCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const leftCommitId = leftResult!.rows[0].id;
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${leftCommitId}, ${"/same.txt"}, ${"left"})`.query();

      const rightResult =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'Right', ${baseCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const rightCommitId = rightResult!.rows[0].id;
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${rightCommitId}, ${"/same.txt"}, ${"right"})`.query();

      const conflicts =
        await sql`SELECT * FROM fs.get_conflicts(${leftCommitId}, ${rightCommitId})`.query<{
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

    it("should detect delete/modify conflicts", async () => {
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('conflicts-delete-modify') RETURNING id`.query<{
          id: string;
        }>();
      const repoId = repoResult!.rows[0].id;

      const rootHead =
        await sql`SELECT head_commit_id FROM fs.branches WHERE repository_id = ${repoId} AND name = 'main'`.query<{
          head_commit_id: string;
        }>();
      const rootCommitId = rootHead!.rows[0].head_commit_id;

      const baseResult =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'Base', ${rootCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const baseCommitId = baseResult!.rows[0].id;
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${baseCommitId}, ${"/del.txt"}, ${"base"})`.query();

      const leftResult =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'Left', ${baseCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const leftCommitId = leftResult!.rows[0].id;
      await sql`INSERT INTO fs.files (commit_id, path, is_deleted) VALUES (${leftCommitId}, ${"/del.txt"}, TRUE)`.query();

      const rightResult =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'Right', ${baseCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const rightCommitId = rightResult!.rows[0].id;
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${rightCommitId}, ${"/del.txt"}, ${"right"})`.query();

      const conflicts =
        await sql`SELECT path, base_exists, left_exists, right_exists, base_content, left_content, right_content, conflict_kind FROM fs.get_conflicts(${leftCommitId}, ${rightCommitId})`.query<{
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

    it("should detect add/add conflicts", async () => {
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('conflicts-add-add') RETURNING id`.query<{
          id: string;
        }>();
      const repoId = repoResult!.rows[0].id;

      const rootHead =
        await sql`SELECT head_commit_id FROM fs.branches WHERE repository_id = ${repoId} AND name = 'main'`.query<{
          head_commit_id: string;
        }>();
      const rootCommitId = rootHead!.rows[0].head_commit_id;

      const baseResult =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'Base', ${rootCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const baseCommitId = baseResult!.rows[0].id;

      const leftResult =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'Left', ${baseCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const leftCommitId = leftResult!.rows[0].id;
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${leftCommitId}, ${"/new.txt"}, ${"left"})`.query();

      const rightResult =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'Right', ${baseCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const rightCommitId = rightResult!.rows[0].id;
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${rightCommitId}, ${"/new.txt"}, ${"right"})`.query();

      const conflicts =
        await sql`SELECT path, base_exists, left_exists, right_exists, base_content, left_content, right_content, conflict_kind FROM fs.get_conflicts(${leftCommitId}, ${rightCommitId})`.query<{
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

    it("should treat symlink/file differences as conflicts", async () => {
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('conflicts-symlink-file') RETURNING id`.query<{
          id: string;
        }>();
      const repoId = repoResult!.rows[0].id;

      const rootHead =
        await sql`SELECT head_commit_id FROM fs.branches WHERE repository_id = ${repoId} AND name = 'main'`.query<{
          head_commit_id: string;
        }>();
      const rootCommitId = rootHead!.rows[0].head_commit_id;

      const baseResult =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'Base', ${rootCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const baseCommitId = baseResult!.rows[0].id;
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${baseCommitId}, ${"/thing.txt"}, ${"base"})`.query();

      const leftResult =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'Left', ${baseCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const leftCommitId = leftResult!.rows[0].id;
      await sql`INSERT INTO fs.files (commit_id, path, content, is_symlink) VALUES (${leftCommitId}, ${"/thing.txt"}, ${"target.txt"}, TRUE)`.query();

      const rightResult =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'Right', ${baseCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const rightCommitId = rightResult!.rows[0].id;
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${rightCommitId}, ${"/thing.txt"}, ${"right"})`.query();

      const conflicts =
        await sql`SELECT path, base_is_symlink, left_is_symlink, right_is_symlink, base_content, left_content, right_content, conflict_kind FROM fs.get_conflicts(${leftCommitId}, ${rightCommitId})`.query<{
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

    it("should reject conflict checks for invalid commit ids", async () => {
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('conflicts-invalid-ids') RETURNING id`.query<{
          id: string;
        }>();
      const repoId = repoResult!.rows[0].id;

      const goodCommit =
        await sql`INSERT INTO fs.commits (repository_id, message) VALUES (${repoId}, 'good') RETURNING id`.query<{
          id: string;
        }>();
      const goodCommitId = goodCommit!.rows[0].id;

      try {
        await sql`SELECT * FROM fs.get_conflicts(${goodCommitId}, ${"00000000-0000-0000-0000-000000000000"})`.query();
        assert.fail("Expected conflict check to fail for invalid commit id");
      } catch (err: any) {
        assert.match(
          err.message,
          /Invalid commit_id \(right\): commit does not exist/
        );
      }
    });
  });

  describe("Merge / Rebase Operations", () => {
    it("should finalize merge by applying non-conflicting changes", async () => {
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('merge-non-conflict') RETURNING id`.query<{
          id: string;
        }>();
      const repoId = repoResult!.rows[0].id;

      const mainBranch =
        await sql`SELECT id, head_commit_id FROM fs.branches WHERE repository_id = ${repoId} AND name = 'main'`.query<{
          id: string;
          head_commit_id: string | null;
        }>();
      const mainBranchId = mainBranch!.rows[0].id;
      const rootHeadId = mainBranch!.rows[0].head_commit_id;

      const baseCommit =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'base', ${rootHeadId}) RETURNING id`.query<{
          id: string;
        }>();
      const baseCommitId = baseCommit!.rows[0].id;
      await sql`UPDATE fs.branches SET head_commit_id = ${baseCommitId} WHERE id = ${mainBranchId}`.query();

      const featureBranch =
        await sql`INSERT INTO fs.branches (repository_id, name) VALUES (${repoId}, 'feature') RETURNING id, head_commit_id`.query<{
          id: string;
          head_commit_id: string;
        }>();
      const featureBranchId = featureBranch!.rows[0].id;
      assert.strictEqual(featureBranch!.rows[0].head_commit_id, baseCommitId);

      const mainCommit =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'main-1', ${baseCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const mainCommitId = mainCommit!.rows[0].id;
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${mainCommitId}, ${"/main.txt"}, ${"main"})`.query();
      await sql`UPDATE fs.branches SET head_commit_id = ${mainCommitId} WHERE id = ${mainBranchId}`.query();

      const featCommit =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'feature-1', ${baseCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const featCommitId = featCommit!.rows[0].id;
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${featCommitId}, ${"/feature.txt"}, ${"feature"})`.query();
      await sql`UPDATE fs.branches SET head_commit_id = ${featCommitId} WHERE id = ${featureBranchId}`.query();

      const mergeCommit =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id, merged_from_commit_id) VALUES (${repoId}, 'Merge feature into main', ${mainCommitId}, ${featCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const mergeCommitId = mergeCommit!.rows[0].id;

      const mergeResult =
        await sql`SELECT operation, merge_commit_id, new_target_head_commit_id, applied_file_count FROM fs.finalize_commit(${mergeCommitId}, ${mainBranchId})`.query<{
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
        mergeCommitId
      );
      assert.strictEqual(mergeResult?.rows[0].applied_file_count, 1);

      const snapshot =
        await sql`SELECT path FROM fs.get_commit_snapshot(${mergeCommitId}) ORDER BY path`.query<{
          path: string;
        }>();
      assert.deepStrictEqual(
        snapshot?.rows.map((r) => r.path),
        ["/feature.txt", "/main.txt"]
      );
    });

    it("should require conflict resolutions before finalizing", async () => {
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('merge-conflict-required') RETURNING id`.query<{
          id: string;
        }>();
      const repoId = repoResult!.rows[0].id;

      const mainBranch =
        await sql`SELECT id, head_commit_id FROM fs.branches WHERE repository_id = ${repoId} AND name = 'main'`.query<{
          id: string;
          head_commit_id: string;
        }>();
      const mainBranchId = mainBranch!.rows[0].id;
      const rootHeadId = mainBranch!.rows[0].head_commit_id;

      const baseCommit =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'base', ${rootHeadId}) RETURNING id`.query<{
          id: string;
        }>();
      const baseCommitId = baseCommit!.rows[0].id;
      await sql`UPDATE fs.branches SET head_commit_id = ${baseCommitId} WHERE id = ${mainBranchId}`.query();
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${baseCommitId}, ${"/same.txt"}, ${"base"})`.query();

      const featureBranch =
        await sql`INSERT INTO fs.branches (repository_id, name) VALUES (${repoId}, 'feature') RETURNING id`.query<{
          id: string;
        }>();
      const featureBranchId = featureBranch!.rows[0].id;

      const mainCommit =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'main-1', ${baseCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const mainCommitId = mainCommit!.rows[0].id;
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${mainCommitId}, ${"/same.txt"}, ${"main"})`.query();
      await sql`UPDATE fs.branches SET head_commit_id = ${mainCommitId} WHERE id = ${mainBranchId}`.query();

      const featCommit =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'feature-1', ${baseCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const featCommitId = featCommit!.rows[0].id;
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${featCommitId}, ${"/same.txt"}, ${"feature"})`.query();
      await sql`UPDATE fs.branches SET head_commit_id = ${featCommitId} WHERE id = ${featureBranchId}`.query();

      const mergeCommit =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id, merged_from_commit_id) VALUES (${repoId}, 'Merge with conflict', ${mainCommitId}, ${featCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const mergeCommitId = mergeCommit!.rows[0].id;

      try {
        await sql`SELECT * FROM fs.finalize_commit(${mergeCommitId}, ${mainBranchId})`.query();
        assert.fail("Expected merge to fail without conflict resolutions");
      } catch (err: any) {
        assert.match(err.message, /Merge requires resolutions/);
      }

      const mainHead =
        await sql`SELECT head_commit_id FROM fs.branches WHERE id = ${mainBranchId}`.query<{
          head_commit_id: string;
        }>();
      assert.strictEqual(mainHead?.rows[0].head_commit_id, mainCommitId);
    });

    it("should honor user-provided conflict resolutions", async () => {
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('merge-conflict-resolution') RETURNING id`.query<{
          id: string;
        }>();
      const repoId = repoResult!.rows[0].id;

      const mainBranch =
        await sql`SELECT id, head_commit_id FROM fs.branches WHERE repository_id = ${repoId} AND name = 'main'`.query<{
          id: string;
          head_commit_id: string;
        }>();
      const mainBranchId = mainBranch!.rows[0].id;
      const rootHeadId = mainBranch!.rows[0].head_commit_id;

      const baseCommit =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'base', ${rootHeadId}) RETURNING id`.query<{
          id: string;
        }>();
      const baseCommitId = baseCommit!.rows[0].id;
      await sql`UPDATE fs.branches SET head_commit_id = ${baseCommitId} WHERE id = ${mainBranchId}`.query();
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${baseCommitId}, ${"/same.txt"}, ${"base"})`.query();

      const featureBranch =
        await sql`INSERT INTO fs.branches (repository_id, name) VALUES (${repoId}, 'feature') RETURNING id`.query<{
          id: string;
        }>();
      const featureBranchId = featureBranch!.rows[0].id;

      const mainCommit =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'main-1', ${baseCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const mainCommitId = mainCommit!.rows[0].id;
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${mainCommitId}, ${"/same.txt"}, ${"main"})`.query();
      await sql`UPDATE fs.branches SET head_commit_id = ${mainCommitId} WHERE id = ${mainBranchId}`.query();

      const featCommit =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'feature-1', ${baseCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const featCommitId = featCommit!.rows[0].id;
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${featCommitId}, ${"/same.txt"}, ${"feature"})`.query();
      await sql`UPDATE fs.branches SET head_commit_id = ${featCommitId} WHERE id = ${featureBranchId}`.query();

      const mergeCommit =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id, merged_from_commit_id) VALUES (${repoId}, 'Merge with resolution', ${mainCommitId}, ${featCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const mergeCommitId = mergeCommit!.rows[0].id;

      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${mergeCommitId}, ${"/same.txt"}, ${"resolved"})`.query();

      const mergeResult =
        await sql`SELECT operation, merge_commit_id, new_target_head_commit_id FROM fs.finalize_commit(${mergeCommitId}, ${mainBranchId})`.query<{
          operation: string;
          merge_commit_id: string | null;
          new_target_head_commit_id: string;
        }>();

      assert.strictEqual(mergeResult?.rows.length, 1);
      assert.strictEqual(
        mergeResult?.rows[0].operation,
        "merged_with_conflicts_resolved"
      );
      assert.strictEqual(mergeResult?.rows[0].merge_commit_id, mergeCommitId);
      assert.strictEqual(
        mergeResult?.rows[0].new_target_head_commit_id,
        mergeCommitId
      );

      const resolved =
        await sql`SELECT fs.read_file(${mergeCommitId}, '/same.txt') as content`.query<{
          content: string | null;
        }>();
      assert.strictEqual(resolved?.rows[0].content, "resolved");
    });

    it("should report already_up_to_date when source is ancestor of target", async () => {
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('merge-already-up-to-date') RETURNING id`.query<{
          id: string;
        }>();
      const repoId = repoResult!.rows[0].id;

      const mainBranch =
        await sql`SELECT id, head_commit_id FROM fs.branches WHERE repository_id = ${repoId} AND name = 'main'`.query<{
          id: string;
          head_commit_id: string;
        }>();
      const mainBranchId = mainBranch!.rows[0].id;
      const rootHeadId = mainBranch!.rows[0].head_commit_id;

      const baseCommit =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'base', ${rootHeadId}) RETURNING id`.query<{
          id: string;
        }>();
      const baseCommitId = baseCommit!.rows[0].id;
      await sql`UPDATE fs.branches SET head_commit_id = ${baseCommitId} WHERE id = ${mainBranchId}`.query();
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${baseCommitId}, ${"/same.txt"}, ${"base"})`.query();

      const mainCommit =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'main-1', ${baseCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const mainCommitId = mainCommit!.rows[0].id;
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${mainCommitId}, ${"/same.txt"}, ${"same"})`.query();
      await sql`UPDATE fs.branches SET head_commit_id = ${mainCommitId} WHERE id = ${mainBranchId}`.query();

      const mergeCommit =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id, merged_from_commit_id) VALUES (${repoId}, 'Merge noop', ${mainCommitId}, ${baseCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const mergeCommitId = mergeCommit!.rows[0].id;

      const mergeResult =
        await sql`SELECT operation, merge_commit_id, new_target_head_commit_id, applied_file_count FROM fs.finalize_commit(${mergeCommitId}, ${mainBranchId})`.query<{
          operation: string;
          merge_commit_id: string | null;
          new_target_head_commit_id: string;
          applied_file_count: number;
        }>();

      assert.strictEqual(mergeResult?.rows.length, 1);
      assert.strictEqual(mergeResult?.rows[0].operation, "already_up_to_date");
      assert.strictEqual(mergeResult?.rows[0].merge_commit_id, mergeCommitId);
      assert.strictEqual(
        mergeResult?.rows[0].new_target_head_commit_id,
        mergeCommitId
      );
      assert.strictEqual(mergeResult?.rows[0].applied_file_count, 0);
    });

    it("should fast-forward rebase when branch is behind onto", async () => {
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('rebase-ff') RETURNING id`.query<{
          id: string;
        }>();
      const repoId = repoResult!.rows[0].id;

      const mainBranch =
        await sql`SELECT id, head_commit_id FROM fs.branches WHERE repository_id = ${repoId} AND name = 'main'`.query<{
          id: string;
          head_commit_id: string;
        }>();
      const mainBranchId = mainBranch!.rows[0].id;
      const rootHeadId = mainBranch!.rows[0].head_commit_id;

      const baseCommit =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'base', ${rootHeadId}) RETURNING id`.query<{
          id: string;
        }>();
      const baseCommitId = baseCommit!.rows[0].id;
      await sql`UPDATE fs.branches SET head_commit_id = ${baseCommitId} WHERE id = ${mainBranchId}`.query();

      const featureBranch =
        await sql`INSERT INTO fs.branches (repository_id, name) VALUES (${repoId}, 'feature') RETURNING id, head_commit_id`.query<{
          id: string;
          head_commit_id: string;
        }>();
      const featureBranchId = featureBranch!.rows[0].id;
      assert.strictEqual(featureBranch!.rows[0].head_commit_id, baseCommitId);

      const mainCommit =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'main-1', ${baseCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const mainCommitId = mainCommit!.rows[0].id;
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${mainCommitId}, ${"/main.txt"}, ${"main"})`.query();
      await sql`UPDATE fs.branches SET head_commit_id = ${mainCommitId} WHERE id = ${mainBranchId}`.query();

      const rebaseResult =
        await sql`SELECT operation, rebased_commit_id, new_branch_head_commit_id, applied_file_count FROM fs.rebase_branch(${featureBranchId}, ${mainBranchId}, ${"Rebase feature onto main"})`.query<{
          operation: string;
          rebased_commit_id: string | null;
          new_branch_head_commit_id: string;
          applied_file_count: number;
        }>();
      assert.strictEqual(rebaseResult?.rows[0].operation, "fast_forward");
      assert.strictEqual(rebaseResult?.rows[0].rebased_commit_id, null);
      assert.strictEqual(
        rebaseResult?.rows[0].new_branch_head_commit_id,
        mainCommitId
      );
      assert.strictEqual(rebaseResult?.rows[0].applied_file_count, 0);

      const featureHead =
        await sql`SELECT head_commit_id FROM fs.branches WHERE id = ${featureBranchId}`.query<{
          head_commit_id: string;
        }>();
      assert.strictEqual(featureHead?.rows[0].head_commit_id, mainCommitId);
    });

    it("should rebase diverged branch by creating a new linear commit (no conflicts)", async () => {
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('rebase-diverged') RETURNING id`.query<{
          id: string;
        }>();
      const repoId = repoResult!.rows[0].id;

      const mainBranch =
        await sql`SELECT id, head_commit_id FROM fs.branches WHERE repository_id = ${repoId} AND name = 'main'`.query<{
          id: string;
          head_commit_id: string;
        }>();
      const mainBranchId = mainBranch!.rows[0].id;
      const rootHeadId = mainBranch!.rows[0].head_commit_id;

      const baseCommit =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'base', ${rootHeadId}) RETURNING id`.query<{
          id: string;
        }>();
      const baseCommitId = baseCommit!.rows[0].id;
      await sql`UPDATE fs.branches SET head_commit_id = ${baseCommitId} WHERE id = ${mainBranchId}`.query();

      const featureBranch =
        await sql`INSERT INTO fs.branches (repository_id, name) VALUES (${repoId}, 'feature') RETURNING id, head_commit_id`.query<{
          id: string;
          head_commit_id: string;
        }>();
      const featureBranchId = featureBranch!.rows[0].id;
      assert.strictEqual(featureBranch!.rows[0].head_commit_id, baseCommitId);

      const featCommit =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'feature-1', ${baseCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const featCommitId = featCommit!.rows[0].id;
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${featCommitId}, ${"/feature.txt"}, ${"feature"})`.query();
      await sql`UPDATE fs.branches SET head_commit_id = ${featCommitId} WHERE id = ${featureBranchId}`.query();

      const mainCommit =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'main-1', ${baseCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const mainCommitId = mainCommit!.rows[0].id;
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${mainCommitId}, ${"/main.txt"}, ${"main"})`.query();
      await sql`UPDATE fs.branches SET head_commit_id = ${mainCommitId} WHERE id = ${mainBranchId}`.query();

      const rebaseResult =
        await sql`SELECT operation, rebased_commit_id, new_branch_head_commit_id, applied_file_count FROM fs.rebase_branch(${featureBranchId}, ${mainBranchId}, ${"Rebase feature onto main"})`.query<{
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
        rebaseResult?.rows[0].new_branch_head_commit_id
      );
      assert.strictEqual(rebaseResult?.rows[0].applied_file_count, 1);

      const rebasedCommitId = rebaseResult!.rows[0].rebased_commit_id!;

      const parent =
        await sql`SELECT parent_commit_id FROM fs.commits WHERE id = ${rebasedCommitId}`.query<{
          parent_commit_id: string;
        }>();
      assert.strictEqual(parent?.rows[0].parent_commit_id, mainCommitId);

      const snapshot =
        await sql`SELECT path FROM fs.get_commit_snapshot(${rebasedCommitId}) ORDER BY path`.query<{
          path: string;
        }>();
      assert.deepStrictEqual(
        snapshot?.rows.map((r) => r.path),
        ["/feature.txt", "/main.txt"]
      );

      const feature =
        await sql`SELECT fs.read_file(${rebasedCommitId}, '/feature.txt') as content`.query<{
          content: string | null;
        }>();
      assert.strictEqual(feature?.rows[0].content, "feature");

      const main =
        await sql`SELECT fs.read_file(${rebasedCommitId}, '/main.txt') as content`.query<{
          content: string | null;
        }>();
      assert.strictEqual(main?.rows[0].content, "main");
    });

    it("should fail rebase on conflict and leave branch head unchanged", async () => {
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('rebase-conflict') RETURNING id`.query<{
          id: string;
        }>();
      const repoId = repoResult!.rows[0].id;

      const mainBranch =
        await sql`SELECT id, head_commit_id FROM fs.branches WHERE repository_id = ${repoId} AND name = 'main'`.query<{
          id: string;
          head_commit_id: string;
        }>();
      const mainBranchId = mainBranch!.rows[0].id;
      const rootHeadId = mainBranch!.rows[0].head_commit_id;

      const baseCommit =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'base', ${rootHeadId}) RETURNING id`.query<{
          id: string;
        }>();
      const baseCommitId = baseCommit!.rows[0].id;
      await sql`UPDATE fs.branches SET head_commit_id = ${baseCommitId} WHERE id = ${mainBranchId}`.query();
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${baseCommitId}, ${"/same.txt"}, ${"base"})`.query();

      const featureBranch =
        await sql`INSERT INTO fs.branches (repository_id, name) VALUES (${repoId}, 'feature') RETURNING id`.query<{
          id: string;
        }>();
      const featureBranchId = featureBranch!.rows[0].id;

      const featCommit =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'feature-1', ${baseCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const featCommitId = featCommit!.rows[0].id;
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${featCommitId}, ${"/same.txt"}, ${"feature"})`.query();
      await sql`UPDATE fs.branches SET head_commit_id = ${featCommitId} WHERE id = ${featureBranchId}`.query();

      const mainCommit =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'main-1', ${baseCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const mainCommitId = mainCommit!.rows[0].id;
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${mainCommitId}, ${"/same.txt"}, ${"main"})`.query();
      await sql`UPDATE fs.branches SET head_commit_id = ${mainCommitId} WHERE id = ${mainBranchId}`.query();

      try {
        await sql`SELECT * FROM fs.rebase_branch(${featureBranchId}, ${mainBranchId}, ${"Rebase with conflict"})`.query();
        assert.fail("Expected rebase to fail on conflict");
      } catch (err: any) {
        assert.match(err.message, /Rebase blocked by/);
      }

      const featureHead =
        await sql`SELECT head_commit_id FROM fs.branches WHERE id = ${featureBranchId}`.query<{
          head_commit_id: string;
        }>();
      assert.strictEqual(featureHead?.rows[0].head_commit_id, featCommitId);
    });

    it("should noop rebase when onto head is already an ancestor of the branch head", async () => {
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('rebase-noop') RETURNING id`.query<{
          id: string;
        }>();
      const repoId = repoResult!.rows[0].id;

      const mainBranch =
        await sql`SELECT id, head_commit_id FROM fs.branches WHERE repository_id = ${repoId} AND name = 'main'`.query<{
          id: string;
          head_commit_id: string;
        }>();
      const mainBranchId = mainBranch!.rows[0].id;
      const rootHeadId = mainBranch!.rows[0].head_commit_id;

      const baseCommit =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'base', ${rootHeadId}) RETURNING id`.query<{
          id: string;
        }>();
      const baseCommitId = baseCommit!.rows[0].id;
      await sql`UPDATE fs.branches SET head_commit_id = ${baseCommitId} WHERE id = ${mainBranchId}`.query();

      const featureBranch =
        await sql`INSERT INTO fs.branches (repository_id, name) VALUES (${repoId}, 'feature') RETURNING id, head_commit_id`.query<{
          id: string;
          head_commit_id: string;
        }>();
      const featureBranchId = featureBranch!.rows[0].id;
      assert.strictEqual(featureBranch!.rows[0].head_commit_id, baseCommitId);

      const featCommit =
        await sql`INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES (${repoId}, 'feature-1', ${baseCommitId}) RETURNING id`.query<{
          id: string;
        }>();
      const featCommitId = featCommit!.rows[0].id;
      await sql`UPDATE fs.branches SET head_commit_id = ${featCommitId} WHERE id = ${featureBranchId}`.query();

      const rebaseResult =
        await sql`SELECT operation, rebased_commit_id, new_branch_head_commit_id FROM fs.rebase_branch(${featureBranchId}, ${mainBranchId}, ${"Rebase noop"})`.query<{
          operation: string;
          rebased_commit_id: string | null;
          new_branch_head_commit_id: string;
        }>();

      assert.strictEqual(rebaseResult?.rows[0].operation, "already_up_to_date");
      assert.strictEqual(rebaseResult?.rows[0].rebased_commit_id, null);
      assert.strictEqual(
        rebaseResult?.rows[0].new_branch_head_commit_id,
        featCommitId
      );
    });
  });

  describe("Content Browsing", () => {
    let repoId: string;
    let commitId: string;
    let branchId: string;

    beforeEach(async () => {
      // Create repository
      const repoResult =
        await sql`INSERT INTO fs.repositories (name) VALUES ('browse-test') RETURNING *`.query<{
          id: string;
          name: string;
          created_at: string;
        }>();
      repoId = repoResult!.rows[0].id;

      // Get the default branch ID (created by the AFTER INSERT trigger)
      const branchResult =
        await sql`SELECT default_branch_id FROM fs.repositories WHERE id = ${repoId}`.query<{
          default_branch_id: string;
        }>();
      branchId = branchResult!.rows[0].default_branch_id;

      // Create a commit with some files
      const commitResult =
        await sql`INSERT INTO fs.commits (repository_id, message) VALUES (${repoId}, 'Test commit') RETURNING id`.query<{
          id: string;
        }>();
      commitId = commitResult!.rows[0].id;

      // Manually update branch head
      await sql`UPDATE fs.branches SET head_commit_id = ${commitId} WHERE id = ${branchId}`.query();

      // Add some files
      await sql`INSERT INTO fs.files (commit_id, path, content) VALUES (${commitId}, '/index.html', '<h1>Hello</h1>'), (${commitId}, '/styles.css', 'body { color: red; }'), (${commitId}, '/script.js', 'console.log(\"hi\");')`.query();
    });

    it("should browse commit delta with fs.get_commit_delta", async () => {
      const contents =
        await sql`SELECT repository_id, repository_name, commit_id, path, is_deleted, is_symlink FROM fs.get_commit_delta(${commitId}) ORDER BY path`.query<{
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

      const html =
        await sql`SELECT fs.read_file(${commitId}, '/index.html') as content`.query<{
          content: string | null;
        }>();
      assert.strictEqual(html?.rows[0].content, "<h1>Hello</h1>");
    });

    it("should browse commit snapshot with fs.get_commit_snapshot", async () => {
      const snapshot =
        await sql`SELECT repository_id, repository_name, commit_id, path, is_symlink, commit_created_at, commit_message FROM fs.get_commit_snapshot(${commitId}) ORDER BY path`.query<{
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

      const html =
        await sql`SELECT fs.read_file(${commitId}, '/index.html') as content`.query<{
          content: string | null;
        }>();
      assert.strictEqual(html?.rows[0].content, "<h1>Hello</h1>");
    });

    it("should browse branch delta using fs.get_commit_delta with branch resolution", async () => {
      const contents =
        await sql`SELECT gcd.repository_id, gcd.repository_name, gcd.commit_id, gcd.path, gcd.is_deleted, gcd.is_symlink, b.name as branch_name FROM fs.get_commit_delta((SELECT head_commit_id FROM fs.branches WHERE id = ${branchId})) gcd CROSS JOIN fs.branches b WHERE b.id = ${branchId} ORDER BY gcd.path`.query<{
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
    });

    it("should return empty result for commit with no files", async () => {
      // Create a commit with no files
      const emptyCommitResult =
        await sql`INSERT INTO fs.commits (repository_id, message) VALUES (${repoId}, 'Empty commit') RETURNING id`.query<{
          id: string;
        }>();
      const emptyCommitId = emptyCommitResult!.rows[0].id;

      // Manually update branch head
      await sql`UPDATE fs.branches SET head_commit_id = ${emptyCommitId} WHERE id = ${branchId}`.query();

      const contents =
        await sql`SELECT * FROM fs.get_commit_delta(${emptyCommitId})`.query();

      assert.strictEqual(contents?.rows.length, 0);
    });

    it("should include commit metadata", async () => {
      const contents =
        await sql`SELECT commit_created_at, commit_message FROM fs.get_commit_delta(${commitId}) LIMIT 1`.query<{
          commit_created_at: string;
          commit_message: string;
        }>();

      assert.ok(contents?.rows[0].commit_created_at);
      assert.strictEqual(contents?.rows[0].commit_message, "Test commit");
    });
  });
});
