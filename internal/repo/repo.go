package repo

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Client struct {
	remoteURL string
	localPath string
	logger    *slog.Logger
}

func New(remoteURL, localPath string, logger *slog.Logger) *Client {
	return &Client{remoteURL: remoteURL, localPath: localPath, logger: logger}
}

func (c *Client) Ensure(ctx context.Context) error {
	fmt.Printf("[repo] ensuring local mirror at %s\n", c.localPath)
	if err := os.MkdirAll(filepath.Dir(c.localPath), 0o755); err != nil {
		return err
	}
	if _, err := os.Stat(c.localPath); err == nil {
		if _, checkErr := run(ctx, c.localPath, "git", "rev-parse", "--is-inside-work-tree"); checkErr == nil {
			_ = removeStaleGitLock(filepath.Join(c.localPath, ".git", "index.lock"), c.logger)
			if _, statErr := os.Stat(filepath.Join(c.localPath, "manifests")); statErr == nil {
				fmt.Println("[repo] local mirror already exists")
				return nil
			}
			fmt.Println("[repo] local mirror missing manifests folder, recloning")
			if rmErr := os.RemoveAll(c.localPath); rmErr != nil {
				return rmErr
			}
		} else {
			if hasManifestsDir(c.localPath) {
				fmt.Println("[repo] no .git metadata; using local manifests snapshot (updates disabled until repo is re-cloned)")
				return nil
			}
			fmt.Println("[repo] existing folder is not a valid git repo, recreating it")
			if rmErr := os.RemoveAll(c.localPath); rmErr != nil {
				return rmErr
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	fmt.Printf("[repo] cloning %s\n", c.remoteURL)
	err := c.runWithProgress(
		ctx,
		"",
		"clone",
		"git",
		"clone",
		"--progress",
		"--depth",
		"1",
		"--single-branch",
		"--no-tags",
		"--no-checkout",
		c.remoteURL,
		c.localPath,
	)
	if err == nil {
		fmt.Println("[repo] clone completed")
	}
	return err
}

func (c *Client) HeadCommit(ctx context.Context) (string, error) {
	out, err := run(ctx, c.localPath, "git", "rev-parse", "refs/remotes/origin/master")
	if err != nil {
		out, err = run(ctx, c.localPath, "git", "rev-parse", "HEAD")
	}
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
}

func (c *Client) Update(ctx context.Context) (string, string, error) {
	oldCommit, err := c.HeadCommit(ctx)
	if err != nil {
		if hasManifestsDir(c.localPath) {
			if c.logger != nil {
				c.logger.Warn("git metadata unavailable; staying on local snapshot")
			}
			return "local-snapshot", "local-snapshot", nil
		}
		return "", "", err
	}
	fmt.Printf("[repo] checking updates from %s\n", shortCommit(oldCommit))
	if err := c.runWithProgress(ctx, c.localPath, "fetch", "git", "fetch", "--progress", "--depth", "1", "--no-tags", "origin"); err != nil {
		return oldCommit, oldCommit, err
	}
	newCommit, err := c.HeadCommit(ctx)
	if err != nil {
		return oldCommit, oldCommit, err
	}
	if oldCommit == newCommit {
		fmt.Printf("[repo] no changes (%s)\n", shortCommit(newCommit))
	} else {
		fmt.Printf("[repo] updated %s -> %s\n", shortCommit(oldCommit), shortCommit(newCommit))
	}
	return oldCommit, newCommit, nil
}

func hasManifestsDir(repoPath string) bool {
	info, err := os.Stat(filepath.Join(repoPath, "manifests"))
	if err != nil {
		return false
	}
	return info.IsDir()
}

func (c *Client) DiffManifestPaths(ctx context.Context, fromCommit, toCommit string) (upserts []string, deletions []string, err error) {
	if fromCommit == "" || toCommit == "" {
		return upserts, deletions, nil
	}
	out, err := run(ctx, c.localPath, "git", "diff", "--name-status", fromCommit+".."+toCommit, "--", "manifests/")
	if err != nil {
		return nil, nil, err
	}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) < 2 {
			continue
		}
		status := parts[0]
		if strings.HasPrefix(status, "R") && len(parts) >= 3 {
			if isManifestFile(parts[1]) {
				deletions = append(deletions, filepath.ToSlash(parts[1]))
			}
			if isManifestFile(parts[2]) {
				upserts = append(upserts, filepath.ToSlash(parts[2]))
			}
			continue
		}
		path := filepath.ToSlash(parts[1])
		if !isManifestFile(path) {
			continue
		}
		if strings.HasPrefix(status, "D") {
			deletions = append(deletions, path)
			continue
		}
		upserts = append(upserts, path)
	}
	return upserts, deletions, nil
}

func (c *Client) ListManifestFiles(ctx context.Context) ([]string, error) {
	started := time.Now()
	if _, gitErr := run(ctx, c.localPath, "git", "rev-parse", "--is-inside-work-tree"); gitErr == nil {
		if c.logger != nil {
			c.logger.Info("manifest scan using git tree")
		}
		out, err := run(ctx, c.localPath, "git", "ls-tree", "-r", "--name-only", "refs/remotes/origin/master", "--", "manifests")
		if err != nil {
			out, err = run(ctx, c.localPath, "git", "ls-tree", "-r", "--name-only", "HEAD", "--", "manifests")
		}
		if err == nil {
			lines := strings.Split(strings.TrimSpace(out), "\n")
			paths := make([]string, 0, len(lines))
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}
				line = filepath.ToSlash(line)
				if isManifestFile(line) {
					paths = append(paths, line)
				}
			}
			if c.logger != nil {
				c.logger.Info("manifest scan complete", "files_seen", len(lines), "manifest_files", len(paths), "duration", time.Since(started).String())
			}
			return paths, nil
		}
		if c.logger != nil {
			c.logger.Warn("manifest tree scan failed, falling back to filesystem walk", "error", err)
		}
	}

	absRepoPath, err := filepath.Abs(c.localPath)
	if err != nil {
		return nil, err
	}
	root := filepath.Join(absRepoPath, "manifests")
	if c.logger != nil {
		c.logger.Info("manifest scan started", "root", root, "repo_path", absRepoPath)
	}
	if _, err := os.Stat(root); err != nil {
		if c.logger != nil {
			c.logger.Error("manifest scan root missing", "root", root, "error", err)
		}
		return nil, err
	}

	if c.logger != nil {
		c.logger.Info("manifest scan using filesystem walk (no git metadata)")
	}

	paths := make([]string, 0, 1024)
	filesSeen := 0
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		filesSeen++
		if c.logger != nil && filesSeen%50000 == 0 {
			c.logger.Info("manifest scan progress", "files_seen", filesSeen, "elapsed", time.Since(started).String())
		}
		rel, relErr := filepath.Rel(c.localPath, path)
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)
		if isManifestFile(rel) {
			paths = append(paths, rel)
		}
		return nil
	})
	if c.logger != nil {
		if err != nil {
			c.logger.Error("manifest scan failed", "files_seen", filesSeen, "elapsed", time.Since(started).String(), "error", err)
		} else {
			c.logger.Info("manifest scan complete", "files_seen", filesSeen, "manifest_files", len(paths), "duration", time.Since(started).String())
			if filesSeen == 0 {
				entries, readErr := os.ReadDir(root)
				if readErr != nil {
					c.logger.Warn("manifest root appears empty", "root", root, "error", readErr)
				} else {
					c.logger.Warn("manifest root has no files discovered", "root", root, "direct_entries", len(entries))
				}
			}
		}
	}
	return paths, err
}

func (c *Client) ReadFiles(ctx context.Context, relPaths []string) ([][]byte, error) {
	if len(relPaths) == 0 {
		return nil, nil
	}
	if _, gitErr := run(ctx, c.localPath, "git", "rev-parse", "--is-inside-work-tree"); gitErr == nil {
		ref := "refs/remotes/origin/master"
		if _, err := run(ctx, c.localPath, "git", "rev-parse", ref); err != nil {
			ref = "HEAD"
		}
		cmd := exec.CommandContext(ctx, "git", "cat-file", "--batch")
		cmd.Dir = c.localPath

		stdin, err := cmd.StdinPipe()
		if err != nil {
			return nil, err
		}
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return nil, err
		}
		if err := cmd.Start(); err != nil {
			return nil, err
		}

		go func() {
			w := bufio.NewWriter(stdin)
			for _, relPath := range relPaths {
				_, _ = fmt.Fprintf(w, "%s:%s\n", ref, filepath.ToSlash(relPath))
			}
			_ = w.Flush()
			_ = stdin.Close()
		}()

		started := time.Now()
		r := bufio.NewReader(stdout)
		out := make([][]byte, len(relPaths))
		for idx, relPath := range relPaths {
			if c.logger != nil && idx > 0 && idx%100 == 0 {
				c.logger.Info("git object read progress", "read", idx, "total", len(relPaths))
			}
			header, readErr := r.ReadString('\n')
			if readErr != nil {
				_ = cmd.Wait()
				return nil, readErr
			}
			header = strings.TrimSpace(header)
			if strings.HasSuffix(header, " missing") {
				_ = cmd.Wait()
				return nil, fmt.Errorf("missing object for %s", relPath)
			}
			parts := strings.Split(header, " ")
			if len(parts) < 3 {
				_ = cmd.Wait()
				return nil, fmt.Errorf("unexpected cat-file header for %s: %s", relPath, header)
			}
			size, convErr := strconv.Atoi(parts[2])
			if convErr != nil || size < 0 {
				_ = cmd.Wait()
				return nil, fmt.Errorf("invalid cat-file size for %s: %s", relPath, parts[2])
			}
			buf := make([]byte, size)
			if _, readErr = io.ReadFull(r, buf); readErr != nil {
				_ = cmd.Wait()
				return nil, readErr
			}
			if _, readErr = r.ReadByte(); readErr != nil {
				_ = cmd.Wait()
				return nil, readErr
			}
			out[idx] = buf
		}
		if err := cmd.Wait(); err != nil {
			return nil, fmt.Errorf("git cat-file failed: %w", err)
		}
		if c.logger != nil {
			c.logger.Info("git object read finished", "count", len(relPaths), "duration", time.Since(started).String())
		}
		return out, nil
	}

	started := time.Now()
	out := make([][]byte, len(relPaths))
	for idx, relPath := range relPaths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if c.logger != nil && idx > 0 && idx%100 == 0 {
			c.logger.Info("git object read progress", "read", idx, "total", len(relPaths))
		}
		fullPath := filepath.Join(c.localPath, filepath.FromSlash(relPath))
		buf, err := os.ReadFile(fullPath)
		if err != nil {
			return nil, err
		}
		out[idx] = buf
	}
	if c.logger != nil {
		c.logger.Info("git object read finished", "count", len(relPaths), "duration", time.Since(started).String())
	}

	return out, nil
}

func (c *Client) ReadFile(ctx context.Context, relPath string) ([]byte, error) {
	files, err := c.ReadFiles(ctx, []string{relPath})
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("empty read result for %s", relPath)
	}
	return files[0], nil
}

func isManifestFile(path string) bool {
	path = strings.ToLower(path)
	if !strings.HasPrefix(path, "manifests/") {
		return false
	}
	return strings.HasSuffix(path, ".yaml") || strings.HasSuffix(path, ".yml")
}

func run(ctx context.Context, workdir string, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	if workdir != "" {
		cmd.Dir = workdir
	}
	out, err := cmd.Output()
	if err != nil {
		var stderr string
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr = strings.TrimSpace(string(exitErr.Stderr))
		}
		if stderr != "" {
			return "", fmt.Errorf("%s %v: %w: %s", name, args, err, stderr)
		}
		return "", fmt.Errorf("%s %v: %w", name, args, err)
	}
	return string(out), nil
}

func (c *Client) runWithProgress(ctx context.Context, workdir, label, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	if workdir != "" {
		cmd.Dir = workdir
	}
	cmd.Env = append(os.Environ(), "GIT_PROGRESS=1")

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	started := time.Now()
	state := &progressState{lastBucket: -1}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		streamGitOutput(c.logger, label, stdout, state)
	}()
	go func() {
		defer wg.Done()
		streamGitOutput(c.logger, label, stderr, state)
	}()

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case err := <-done:
			wg.Wait()
			if err != nil {
				return fmt.Errorf("%s %v: %w", name, args, err)
			}
			if c.logger != nil {
				c.logger.Info("git operation finished", "operation", label, "duration", time.Since(started).String())
			}
			return nil
		case <-ticker.C:
			if c.logger != nil {
				c.logger.Info("git operation still running", "operation", label, "elapsed", time.Since(started).String())
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func streamGitOutput(logger *slog.Logger, label string, r io.Reader, state *progressState) {
	scanner := bufio.NewScanner(r)
	scanner.Split(scanCRLF)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if pct, ok := extractPercent(line); ok {
			bucket := pct / 10
			state.mu.Lock()
			if bucket <= state.lastBucket {
				state.mu.Unlock()
				continue
			}
			state.lastBucket = bucket
			state.mu.Unlock()
			if logger != nil {
				logger.Info("git progress", "operation", label, "progress_percent", bucket*10)
			} else {
				fmt.Printf("[repo:%s] %d%%\n", label, bucket*10)
			}
			continue
		}
		if logger != nil {
			logger.Info("git progress", "operation", label, "line", line)
		} else {
			fmt.Printf("[repo:%s] %s\n", label, line)
		}
	}
}

type progressState struct {
	mu         sync.Mutex
	lastBucket int
}

var percentRe = regexp.MustCompile(`(\d{1,3})%`)

func extractPercent(line string) (int, bool) {
	m := percentRe.FindStringSubmatch(line)
	if len(m) < 2 {
		return 0, false
	}
	pct, err := strconv.Atoi(m[1])
	if err != nil || pct < 0 {
		return 0, false
	}
	if pct > 100 {
		pct = 100
	}
	return pct, true
}

func removeStaleGitLock(path string, logger *slog.Logger) error {
	if _, err := os.Stat(path); err == nil {
		if logger != nil {
			logger.Warn("removing stale git lock", "path", path)
		}
		return os.Remove(path)
	}
	return nil
}

func scanCRLF(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	for i, b := range data {
		if b == '\n' || b == '\r' {
			if i == 0 {
				return 1, nil, nil
			}
			return i + 1, data[:i], nil
		}
	}
	if atEOF {
		return len(data), data, nil
	}
	return 0, nil, nil
}

func shortCommit(commit string) string {
	if len(commit) < 12 {
		return commit
	}
	return commit[:12]
}
