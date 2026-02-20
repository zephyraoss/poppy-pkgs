package indexer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/zephyraoss/poppy-pkgs/internal/repo"
	"github.com/zephyraoss/poppy-pkgs/internal/store"
	"gopkg.in/yaml.v3"
)

type Indexer struct {
	db     *store.DB
	repo   *repo.Client
	logger *slog.Logger

	mu              sync.Mutex
	running         bool
	lastRunStarted  time.Time
	lastRunFinished time.Time
	lastRunError    string
	lastRunType     string
	lastUpserts     int
	lastDeletes     int
	lastCommit      string
}

type Status struct {
	Running         bool
	LastRunStarted  time.Time
	LastRunFinished time.Time
	LastRunError    string
	LastRunType     string
	LastUpserts     int
	LastDeletes     int
	LastCommit      string
}

func New(db *store.DB, repoClient *repo.Client, logger *slog.Logger) *Indexer {
	return &Indexer{db: db, repo: repoClient, logger: logger}
}

func (i *Indexer) RunOnce(ctx context.Context) (runErr error) {
	started := time.Now()
	i.mu.Lock()
	if i.running {
		i.mu.Unlock()
		i.logger.Debug("indexer run skipped, already running")
		return nil
	}
	i.running = true
	i.lastRunStarted = started
	i.mu.Unlock()
	defer func() {
		i.mu.Lock()
		i.running = false
		i.lastRunFinished = time.Now()
		if runErr != nil {
			i.lastRunError = runErr.Error()
		} else {
			i.lastRunError = ""
		}
		i.mu.Unlock()
	}()

	lastIndexedCommit, err := i.db.GetState(ctx, "last_indexed_commit")
	if err != nil {
		return err
	}
	i.logger.Info("index run started", "last_indexed_commit", shortCommit(lastIndexedCommit))

	oldHead, newHead, err := i.repo.Update(ctx)
	if err != nil {
		i.logger.Warn("repo update failed, indexing existing state", "error", err)
		newHead, err = i.repo.HeadCommit(ctx)
		if err != nil {
			return err
		}
		oldHead = newHead
	}

	if lastIndexedCommit == "" {
		i.logger.Info("starting full index")
		i.logger.Info("listing manifest files from local mirror")
		paths, err := i.repo.ListManifestFiles(ctx)
		if err != nil {
			return err
		}
		i.logger.Info("discovered manifests for full index", "count", len(paths))
		recs, err := i.parseManifests(ctx, paths)
		if err != nil {
			return err
		}
		i.logger.Info("applying full index changes", "parsed_manifests", len(recs))
		if err := i.db.ApplyManifestChanges(ctx, recs, nil, newHead); err != nil {
			return err
		}
		i.setRunSummary("full", len(recs), 0, newHead)
		i.logger.Info("full index complete", "manifests", len(recs), "commit", shortCommit(newHead), "duration", time.Since(started).String())
		return nil
	}

	if lastIndexedCommit == newHead {
		i.setRunSummary("noop", 0, 0, newHead)
		i.logger.Debug("index is already up to date", "commit", shortCommit(newHead), "duration", time.Since(started).String())
		return nil
	}

	_ = oldHead
	upsertPaths, deletePaths, err := i.repo.DiffManifestPaths(ctx, lastIndexedCommit, newHead)
	if err != nil {
		return err
	}
	i.logger.Info("manifest diff computed", "from", shortCommit(lastIndexedCommit), "to", shortCommit(newHead), "upserts", len(upsertPaths), "deletes", len(deletePaths))
	if len(upsertPaths) == 0 && len(deletePaths) == 0 {
		i.logger.Info("no manifest changes, advancing commit pointer")
		if err := i.db.ApplyManifestChanges(ctx, nil, nil, newHead); err != nil {
			return err
		}
		i.setRunSummary("noop", 0, 0, newHead)
		return nil
	}

	recs, err := i.parseManifests(ctx, upsertPaths)
	if err != nil {
		return err
	}
	i.logger.Info("applying incremental index changes", "parsed_upserts", len(recs), "deletes", len(deletePaths))

	if err := i.db.ApplyManifestChanges(ctx, recs, deletePaths, newHead); err != nil {
		return err
	}
	i.setRunSummary("incremental", len(recs), len(deletePaths), newHead)

	i.logger.Info("incremental index complete", "upserts", len(recs), "deletes", len(deletePaths), "commit", shortCommit(newHead), "duration", time.Since(started).String())
	return nil
}

func (i *Indexer) Status() Status {
	i.mu.Lock()
	defer i.mu.Unlock()
	return Status{
		Running:         i.running,
		LastRunStarted:  i.lastRunStarted,
		LastRunFinished: i.lastRunFinished,
		LastRunError:    i.lastRunError,
		LastRunType:     i.lastRunType,
		LastUpserts:     i.lastUpserts,
		LastDeletes:     i.lastDeletes,
		LastCommit:      i.lastCommit,
	}
}

func (i *Indexer) setRunSummary(runType string, upserts, deletes int, commit string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.lastRunType = runType
	i.lastUpserts = upserts
	i.lastDeletes = deletes
	i.lastCommit = commit
}

func (i *Indexer) parseManifests(ctx context.Context, paths []string) ([]store.ManifestRecord, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	sort.Strings(paths)
	recs := make([]store.ManifestRecord, 0, len(paths))
	seenPackages := make(map[string]struct{}, 1024)
	packageCount := 0
	nextPercent := 10
	total := len(paths)

	chunkSize := 500
	workers := runtime.NumCPU()
	if workers < 2 {
		workers = 2
	}
	for start := 0; start < total; start += chunkSize {
		end := start + chunkSize
		if end > total {
			end = total
		}
		batchPaths := paths[start:end]
		i.logger.Info("manifest batch read", "start", start+1, "end", end, "total", total)
		batchRaw, err := i.repo.ReadFiles(ctx, batchPaths)
		if err != nil {
			return nil, err
		}

		type job struct {
			idx  int
			path string
			raw  []byte
		}
		type result struct {
			rec  store.ManifestRecord
			path string
			err  error
		}

		jobs := make(chan job, len(batchPaths))
		results := make(chan result, len(batchPaths))
		var wg sync.WaitGroup
		for n := 0; n < workers; n++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := range jobs {
					rec, parseErr := parseManifest(j.path, j.raw)
					if parseErr != nil {
						results <- result{path: j.path, err: parseErr}
						continue
					}
					if rec.PackageID == "" {
						results <- result{path: j.path, err: fmt.Errorf("missing PackageIdentifier")}
						continue
					}
					results <- result{rec: rec, path: j.path}
				}
			}()
		}

		for idx := range batchPaths {
			jobs <- job{idx: idx, path: batchPaths[idx], raw: batchRaw[idx]}
		}
		close(jobs)
		wg.Wait()
		close(results)

		for r := range results {
			if r.err != nil {
				i.logger.Warn("failed to parse manifest", "path", r.path, "error", r.err)
				continue
			}
			recs = append(recs, r.rec)
			if _, ok := seenPackages[r.rec.PackageID]; !ok {
				seenPackages[r.rec.PackageID] = struct{}{}
				packageCount++
				i.logger.Info("package processed", "package_id", r.rec.PackageID, "packages_seen", packageCount)
			}
		}

		processed := end
		percent := (processed * 100) / total
		for percent >= nextPercent {
			i.logger.Info("manifest parse progress", "progress_percent", nextPercent, "processed", processed, "total", total)
			nextPercent += 10
		}
	}
	i.logger.Info("manifest parse complete", "parsed", len(recs), "requested", len(paths))
	return recs, nil
}

func shortCommit(commit string) string {
	if len(commit) <= 12 {
		return commit
	}
	return fmt.Sprintf("%s", commit[:12])
}

func parseManifest(path string, raw []byte) (store.ManifestRecord, error) {
	var body map[string]any
	if err := yaml.Unmarshal(raw, &body); err != nil {
		return store.ManifestRecord{}, err
	}

	hash := sha256.Sum256(raw)
	return store.ManifestRecord{
		Path:             path,
		PackageID:        getString(body, "PackageIdentifier"),
		PackageVersion:   getString(body, "PackageVersion"),
		ManifestType:     getString(body, "ManifestType"),
		PackageLocale:    getString(body, "PackageLocale"),
		Channel:          getString(body, "Channel"),
		PackageName:      getString(body, "PackageName"),
		Publisher:        getString(body, "Publisher"),
		Moniker:          getString(body, "Moniker"),
		ShortDescription: getString(body, "ShortDescription"),
		Tags:             getStringList(body, "Tags"),
		SHA256:           hex.EncodeToString(hash[:]),
		RawYAML:          string(raw),
	}, nil
}

func getString(m map[string]any, key string) string {
	v, ok := m[key]
	if !ok {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(s)
}

func getStringList(m map[string]any, key string) []string {
	v, ok := m[key]
	if !ok {
		return nil
	}
	raw, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		s, ok := item.(string)
		if !ok {
			continue
		}
		s = strings.TrimSpace(s)
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}
