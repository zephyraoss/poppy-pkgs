package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/zephyraoss/poppy-pkgs/internal/indexer"
	"github.com/zephyraoss/poppy-pkgs/internal/store"
)

type Server struct {
	db     *store.DB
	idx    *indexer.Indexer
	logger *slog.Logger
}

func New(db *store.DB, idx *indexer.Indexer, logger *slog.Logger) *fiber.App {
	s := &Server{db: db, idx: idx, logger: logger}
	app := fiber.New(fiber.Config{AppName: "poppypkgs"})
	v1 := app.Group("/api/v1")

	v1.Get("/health", s.health)
	v1.Get("/status", s.status)
	v1.Get("/search", s.search)
	v1.Get("/packages/:packageId", s.getPackage)
	v1.Get("/packages/:packageId/versions", s.listVersions)
	v1.Get("/packages/:packageId/versions/:version/manifests", s.listManifests)
	v1.Get("/packages/:packageId/versions/:version/manifests/:manifestId/raw", s.rawManifest)
	v1.Get("/packages/:packageId/versions/:version/manifests/:manifestId/download", s.downloadManifest)

	return app
}

func (s *Server) health(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{"status": "ok"})
}

func (s *Server) status(c *fiber.Ctx) error {
	lastIndexedCommit, err := s.db.GetState(c.UserContext(), "last_indexed_commit")
	if err != nil {
		s.logger.Error("status lookup failed", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "internal error"})
	}
	idxStatus := s.idx.Status()

	return c.JSON(fiber.Map{
		"data": fiber.Map{
			"running":              idxStatus.Running,
			"last_indexed_commit":  lastIndexedCommit,
			"last_run_commit":      idxStatus.LastCommit,
			"last_run_type":        idxStatus.LastRunType,
			"last_upserts":         idxStatus.LastUpserts,
			"last_deletes":         idxStatus.LastDeletes,
			"last_run_started_at":  formatTime(idxStatus.LastRunStarted),
			"last_run_finished_at": formatTime(idxStatus.LastRunFinished),
			"last_run_error":       idxStatus.LastRunError,
		},
	})
}

func (s *Server) search(c *fiber.Ctx) error {
	q := c.Query("q")
	if q == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "q is required"})
	}
	limit := clampInt(c.QueryInt("limit", 20), 1, 100)
	offset := max(c.QueryInt("offset", 0), 0)

	results, err := s.db.SearchPackages(c.UserContext(), q, limit, offset)
	if err != nil {
		s.logger.Error("search failed", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "search failed"})
	}

	return c.JSON(fiber.Map{
		"data": results,
		"meta": fiber.Map{"q": q, "limit": limit, "offset": offset, "count": len(results)},
	})
}

func (s *Server) getPackage(c *fiber.Ctx) error {
	pkg, err := s.db.GetPackage(c.UserContext(), c.Params("packageId"))
	if err != nil {
		if err == sql.ErrNoRows {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "package not found"})
		}
		s.logger.Error("get package failed", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "internal error"})
	}
	return c.JSON(fiber.Map{"data": pkg})
}

func (s *Server) listVersions(c *fiber.Ctx) error {
	packageID := c.Params("packageId")
	versions, err := s.db.ListVersions(c.UserContext(), packageID)
	if err != nil {
		s.logger.Error("list versions failed", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "internal error"})
	}
	return c.JSON(fiber.Map{"data": versions, "meta": fiber.Map{"count": len(versions)}})
}

func (s *Server) listManifests(c *fiber.Ctx) error {
	packageID := c.Params("packageId")
	version, err := s.resolveVersion(c.UserContext(), packageID, c.Params("version"))
	if err != nil {
		return writeManifestError(c, err)
	}

	manifests, err := s.db.ListManifests(c.UserContext(), packageID, version)
	if err != nil {
		s.logger.Error("list manifests failed", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "internal error"})
	}

	base := c.BaseURL()
	out := make([]fiber.Map, 0, len(manifests))
	for _, m := range manifests {
		entry := fiber.Map{
			"manifest_id":       m.ManifestID,
			"path":              m.Path,
			"manifest_type":     m.ManifestType,
			"package_locale":    m.PackageLocale,
			"channel":           m.Channel,
			"package_name":      m.PackageName,
			"publisher":         m.Publisher,
			"moniker":           m.Moniker,
			"short_description": m.ShortDescription,
			"tags":              m.Tags,
			"sha256":            m.SHA256,
			"raw_url":           fmt.Sprintf("%s/api/v1/packages/%s/versions/%s/manifests/%d/raw", base, packageID, version, m.ManifestID),
			"download_url":      fmt.Sprintf("%s/api/v1/packages/%s/versions/%s/manifests/%d/download", base, packageID, version, m.ManifestID),
		}
		out = append(out, entry)
	}

	return c.JSON(fiber.Map{"data": out, "meta": fiber.Map{"count": len(out), "version": version}})
}

func (s *Server) rawManifest(c *fiber.Ctx) error {
	m, err := s.fetchManifest(c.UserContext(), c.Params("packageId"), c.Params("version"), c.Params("manifestId"))
	if err != nil {
		return writeManifestError(c, err)
	}
	c.Set(fiber.HeaderContentType, "application/x-yaml; charset=utf-8")
	return c.SendString(m.RawYAML)
}

func (s *Server) downloadManifest(c *fiber.Ctx) error {
	m, err := s.fetchManifest(c.UserContext(), c.Params("packageId"), c.Params("version"), c.Params("manifestId"))
	if err != nil {
		return writeManifestError(c, err)
	}
	filename := filepath.Base(m.Path)
	c.Set(fiber.HeaderContentType, "application/x-yaml; charset=utf-8")
	c.Set(fiber.HeaderContentDisposition, fmt.Sprintf("attachment; filename=%q", filename))
	return c.SendString(m.RawYAML)
}

var errInvalidManifestID = errors.New("invalid manifest id")
var errNoLatestVersion = errors.New("no latest version")

func (s *Server) fetchManifest(ctx context.Context, packageID, version, manifestIDParam string) (store.ManifestFile, error) {
	resolvedVersion, err := s.resolveVersion(ctx, packageID, version)
	if err != nil {
		return store.ManifestFile{}, err
	}
	manifestID, err := strconv.ParseInt(manifestIDParam, 10, 64)
	if err != nil {
		return store.ManifestFile{}, errInvalidManifestID
	}
	m, err := s.db.GetManifest(ctx, packageID, resolvedVersion, manifestID)
	if err != nil {
		return store.ManifestFile{}, err
	}
	return m, nil
}

func (s *Server) resolveVersion(ctx context.Context, packageID, version string) (string, error) {
	if version != "latest" {
		return version, nil
	}
	latest, err := s.db.GetLatestVersion(ctx, packageID)
	if err != nil {
		return "", err
	}
	if latest == "" {
		return "", errNoLatestVersion
	}
	return latest, nil
}

func writeManifestError(c *fiber.Ctx, err error) error {
	if errors.Is(err, errInvalidManifestID) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid manifest id"})
	}
	if errors.Is(err, sql.ErrNoRows) {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "manifest not found"})
	}
	if errors.Is(err, errNoLatestVersion) {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "latest version not found"})
	}
	return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "internal error"})
}

func clampInt(v, minV, maxV int) int {
	if v < minV {
		return minV
	}
	if v > maxV {
		return maxV
	}
	return v
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func formatTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC().Format(time.RFC3339)
}
