package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	_ "modernc.org/sqlite"
)

type DB struct {
	sql *sql.DB
}

type ManifestRecord struct {
	Path             string
	PackageID        string
	PackageVersion   string
	ManifestType     string
	PackageLocale    string
	Channel          string
	PackageName      string
	Publisher        string
	Moniker          string
	ShortDescription string
	Tags             []string
	SHA256           string
	RawYAML          string
}

type Package struct {
	PackageID        string   `json:"package_id"`
	PackageName      string   `json:"package_name,omitempty"`
	Publisher        string   `json:"publisher,omitempty"`
	Moniker          string   `json:"moniker,omitempty"`
	ShortDescription string   `json:"short_description,omitempty"`
	LatestVersion    string   `json:"latest_version,omitempty"`
	Tags             []string `json:"tags,omitempty"`
	Score            float64  `json:"score"`
	RankBucket       int      `json:"rank_bucket"`
}

type Version struct {
	PackageVersion string `json:"package_version"`
	Channel        string `json:"channel,omitempty"`
}

type ManifestFile struct {
	ManifestID       int64    `json:"manifest_id"`
	Path             string   `json:"path"`
	ManifestType     string   `json:"manifest_type,omitempty"`
	PackageLocale    string   `json:"package_locale,omitempty"`
	Channel          string   `json:"channel,omitempty"`
	PackageName      string   `json:"package_name,omitempty"`
	Publisher        string   `json:"publisher,omitempty"`
	Moniker          string   `json:"moniker,omitempty"`
	ShortDescription string   `json:"short_description,omitempty"`
	Tags             []string `json:"tags,omitempty"`
	SHA256           string   `json:"sha256"`
	UpdatedAt        string   `json:"-"`
	RawYAML          string   `json:"-"`
}

func Open(path string) (*DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	dsn := fmt.Sprintf("file:%s", filepath.ToSlash(path))
	sqlDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(1)
	pragmas := []string{
		"PRAGMA foreign_keys = ON;",
		"PRAGMA journal_mode = WAL;",
		"PRAGMA synchronous = NORMAL;",
		"PRAGMA temp_store = MEMORY;",
		"PRAGMA busy_timeout = 5000;",
	}
	for _, pragma := range pragmas {
		if _, err := sqlDB.Exec(pragma); err != nil {
			return nil, err
		}
	}
	return &DB{sql: sqlDB}, nil
}

func (d *DB) Close() error {
	return d.sql.Close()
}

func (d *DB) Migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS state (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL,
			updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE TABLE IF NOT EXISTS manifest_files (
			manifest_id INTEGER PRIMARY KEY AUTOINCREMENT,
			path TEXT NOT NULL UNIQUE,
			package_id TEXT NOT NULL,
			package_version TEXT NOT NULL DEFAULT '',
			manifest_type TEXT NOT NULL DEFAULT '',
			package_locale TEXT NOT NULL DEFAULT '',
			channel TEXT NOT NULL DEFAULT '',
			package_name TEXT NOT NULL DEFAULT '',
			publisher TEXT NOT NULL DEFAULT '',
			moniker TEXT NOT NULL DEFAULT '',
			short_description TEXT NOT NULL DEFAULT '',
			tags_json TEXT NOT NULL DEFAULT '[]',
			sha256 TEXT NOT NULL,
			raw_yaml TEXT NOT NULL,
			updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE INDEX IF NOT EXISTS idx_manifest_files_package ON manifest_files(package_id, package_version);`,
		`CREATE TABLE IF NOT EXISTS packages (
			package_id TEXT PRIMARY KEY,
			package_name TEXT NOT NULL DEFAULT '',
			publisher TEXT NOT NULL DEFAULT '',
			moniker TEXT NOT NULL DEFAULT '',
			short_description TEXT NOT NULL DEFAULT '',
			latest_version TEXT NOT NULL DEFAULT '',
			tags_json TEXT NOT NULL DEFAULT '[]',
			updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE INDEX IF NOT EXISTS idx_packages_id_nocase ON packages(package_id COLLATE NOCASE);`,
		`CREATE INDEX IF NOT EXISTS idx_packages_name_nocase ON packages(package_name COLLATE NOCASE);`,
		`CREATE INDEX IF NOT EXISTS idx_packages_moniker_nocase ON packages(moniker COLLATE NOCASE);`,
		`CREATE TABLE IF NOT EXISTS versions (
			package_id TEXT NOT NULL,
			package_version TEXT NOT NULL,
			channel TEXT NOT NULL DEFAULT '',
			package_locale TEXT NOT NULL DEFAULT '',
			updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (package_id, package_version, channel, package_locale),
			FOREIGN KEY(package_id) REFERENCES packages(package_id) ON DELETE CASCADE
		);`,
		`CREATE VIRTUAL TABLE IF NOT EXISTS package_search USING fts5(
			package_id,
			package_name,
			publisher,
			moniker,
			short_description,
			tags
		);`,
	}
	for _, stmt := range stmts {
		if _, err := d.sql.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	if err := ensurePackagesLatestVersionColumn(ctx, d.sql); err != nil {
		return err
	}
	return nil
}

func ensurePackagesLatestVersionColumn(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `PRAGMA table_info(packages)`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name string
		var colType string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &colType, &notnull, &dflt, &pk); err != nil {
			return err
		}
		if strings.EqualFold(name, "latest_version") {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `ALTER TABLE packages ADD COLUMN latest_version TEXT NOT NULL DEFAULT ''`)
	return err
}

func (d *DB) GetState(ctx context.Context, key string) (string, error) {
	var value string
	err := d.sql.QueryRowContext(ctx, `SELECT value FROM state WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return value, err
}

func (d *DB) ApplyManifestChanges(ctx context.Context, upserts []ManifestRecord, deletions []string, newCommit string) error {
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	affected := map[string]struct{}{}
	deleteStmt, err := tx.PrepareContext(ctx, `DELETE FROM manifest_files WHERE path = ?`)
	if err != nil {
		return err
	}
	defer deleteStmt.Close()

	upsertStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO manifest_files (
			path, package_id, package_version, manifest_type, package_locale, channel,
			package_name, publisher, moniker, short_description, tags_json, sha256, raw_yaml, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(path) DO UPDATE SET
			package_id=excluded.package_id,
			package_version=excluded.package_version,
			manifest_type=excluded.manifest_type,
			package_locale=excluded.package_locale,
			channel=excluded.channel,
			package_name=excluded.package_name,
			publisher=excluded.publisher,
			moniker=excluded.moniker,
			short_description=excluded.short_description,
			tags_json=excluded.tags_json,
			sha256=excluded.sha256,
			raw_yaml=excluded.raw_yaml,
			updated_at=CURRENT_TIMESTAMP
	`)
	if err != nil {
		return err
	}
	defer upsertStmt.Close()

	for _, path := range deletions {
		pkgID, _ := packageIDByPathTx(ctx, tx, path)
		if pkgID != "" {
			affected[pkgID] = struct{}{}
		}
		if _, err = deleteStmt.ExecContext(ctx, path); err != nil {
			return err
		}
	}

	for _, rec := range upserts {
		tagsJSON, marshalErr := json.Marshal(rec.Tags)
		if marshalErr != nil {
			return marshalErr
		}
		_, err = upsertStmt.ExecContext(ctx,
			rec.Path, rec.PackageID, rec.PackageVersion, rec.ManifestType, rec.PackageLocale, rec.Channel,
			rec.PackageName, rec.Publisher, rec.Moniker, rec.ShortDescription, string(tagsJSON), rec.SHA256, rec.RawYAML,
		)
		if err != nil {
			return err
		}
		affected[rec.PackageID] = struct{}{}
	}

	affectedIDs := make([]string, 0, len(affected))
	for pkgID := range affected {
		affectedIDs = append(affectedIDs, pkgID)
	}
	if err := rebuildPackagesTx(ctx, tx, affectedIDs); err != nil {
		return err
	}

	if newCommit != "" {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO state (key, value, updated_at) VALUES ('last_indexed_commit', ?, CURRENT_TIMESTAMP)
			ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
		`, newCommit)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func packageIDByPathTx(ctx context.Context, tx *sql.Tx, path string) (string, error) {
	var pkgID string
	err := tx.QueryRowContext(ctx, `SELECT package_id FROM manifest_files WHERE path = ?`, path).Scan(&pkgID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return pkgID, err
}

func rebuildPackagesTx(ctx context.Context, tx *sql.Tx, packageIDs []string) error {
	if len(packageIDs) == 0 {
		return nil
	}
	sort.Strings(packageIDs)

	if _, err := tx.ExecContext(ctx, `DROP TABLE IF EXISTS affected_packages`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `CREATE TEMP TABLE affected_packages (package_id TEXT PRIMARY KEY)`); err != nil {
		return err
	}
	insertAffected, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO affected_packages(package_id) VALUES (?)`)
	if err != nil {
		return err
	}
	for _, packageID := range packageIDs {
		if _, err := insertAffected.ExecContext(ctx, packageID); err != nil {
			insertAffected.Close()
			return err
		}
	}
	_ = insertAffected.Close()

	if _, err := tx.ExecContext(ctx, `DELETE FROM package_search WHERE package_id IN (SELECT package_id FROM affected_packages)`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM versions WHERE package_id IN (SELECT package_id FROM affected_packages)`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM packages WHERE package_id IN (SELECT package_id FROM affected_packages)`); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO packages (package_id, package_name, publisher, moniker, short_description, latest_version, tags_json, updated_at)
		SELECT package_id, package_name, publisher, moniker, short_description, '', tags_json, CURRENT_TIMESTAMP
		FROM (
			SELECT
				mf.package_id,
				mf.package_name,
				mf.publisher,
				mf.moniker,
				mf.short_description,
				mf.tags_json,
				ROW_NUMBER() OVER (
					PARTITION BY mf.package_id
					ORDER BY
						CASE mf.manifest_type
							WHEN 'defaultLocale' THEN 0
							WHEN 'singleton' THEN 1
							WHEN 'locale' THEN 2
							ELSE 3
						END,
						mf.updated_at DESC
				) AS rn
			FROM manifest_files mf
			WHERE mf.package_id IN (SELECT package_id FROM affected_packages)
		) ranked
		WHERE rn = 1
	`); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO versions (package_id, package_version, channel, package_locale, updated_at)
		SELECT DISTINCT package_id, package_version, channel, package_locale, CURRENT_TIMESTAMP
		FROM manifest_files
		WHERE package_id IN (SELECT package_id FROM affected_packages) AND package_version <> ''
	`); err != nil {
		return err
	}

	latestByPackage := map[string]string{}
	versionRows, err := tx.QueryContext(ctx, `
		SELECT package_id, package_version
		FROM versions
		WHERE package_id IN (SELECT package_id FROM affected_packages)
	`)
	if err != nil {
		return err
	}
	for versionRows.Next() {
		var pkgID string
		var version string
		if err := versionRows.Scan(&pkgID, &version); err != nil {
			_ = versionRows.Close()
			return err
		}
		cur, ok := latestByPackage[pkgID]
		if !ok || compareVersion(version, cur) > 0 {
			latestByPackage[pkgID] = version
		}
	}
	if err := versionRows.Err(); err != nil {
		_ = versionRows.Close()
		return err
	}
	_ = versionRows.Close()

	updateLatestStmt, err := tx.PrepareContext(ctx, `UPDATE packages SET latest_version = ? WHERE package_id = ?`)
	if err != nil {
		return err
	}
	defer updateLatestStmt.Close()
	for pkgID, latest := range latestByPackage {
		if _, err := updateLatestStmt.ExecContext(ctx, latest, pkgID); err != nil {
			return err
		}
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO package_search (package_id, package_name, publisher, moniker, short_description, tags)
		SELECT package_id, package_name, publisher, moniker, short_description, tags_json
		FROM packages
		WHERE package_id IN (SELECT package_id FROM affected_packages)
	`); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `DROP TABLE IF EXISTS affected_packages`); err != nil {
		return err
	}

	return nil
}

func (d *DB) SearchPackages(ctx context.Context, q string, limit, offset int) ([]Package, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	prefix := q + "%"
	namePrefix := q + "%"
	fts := buildFTSQuery(q)

	var rows *sql.Rows
	var err error
	if fts == "" {
		rows, err = d.sql.QueryContext(ctx, `
			WITH ranked AS (
				SELECT package_id, package_name, publisher, moniker, short_description, latest_version, tags_json, 0 AS bucket, 0.0 AS score
				FROM packages
				WHERE package_id = ? COLLATE NOCASE
				UNION ALL
				SELECT package_id, package_name, publisher, moniker, short_description, latest_version, tags_json, 1 AS bucket, 0.0 AS score
				FROM packages
				WHERE package_name = ? COLLATE NOCASE OR moniker = ? COLLATE NOCASE
				UNION ALL
				SELECT package_id, package_name, publisher, moniker, short_description, latest_version, tags_json, 2 AS bucket, 0.0 AS score
				FROM packages
				WHERE (
					package_name LIKE ? COLLATE NOCASE OR
					moniker LIKE ? COLLATE NOCASE
				)
				AND package_name <> ? COLLATE NOCASE
				AND moniker <> ? COLLATE NOCASE
				UNION ALL
				SELECT package_id, package_name, publisher, moniker, short_description, latest_version, tags_json, 3 AS bucket, 0.0 AS score
				FROM packages
				WHERE package_id LIKE ? COLLATE NOCASE AND package_id <> ? COLLATE NOCASE
			), dedup AS (
				SELECT *, ROW_NUMBER() OVER (PARTITION BY package_id ORDER BY bucket, score) AS rn
				FROM ranked
			)
			SELECT package_id, package_name, publisher, moniker, short_description, latest_version, tags_json, score, bucket
			FROM dedup
			WHERE rn = 1
			ORDER BY bucket, score, package_id
			LIMIT ? OFFSET ?
		`, q, q, q, namePrefix, namePrefix, q, q, prefix, q, limit, offset)
	} else {
		rows, err = d.sql.QueryContext(ctx, `
			WITH ranked AS (
				SELECT package_id, package_name, publisher, moniker, short_description, latest_version, tags_json, 0 AS bucket, 0.0 AS score
				FROM packages
				WHERE package_id = ? COLLATE NOCASE
				UNION ALL
				SELECT package_id, package_name, publisher, moniker, short_description, latest_version, tags_json, 1 AS bucket, 0.0 AS score
				FROM packages
				WHERE package_name = ? COLLATE NOCASE OR moniker = ? COLLATE NOCASE
				UNION ALL
				SELECT package_id, package_name, publisher, moniker, short_description, latest_version, tags_json, 2 AS bucket, 0.0 AS score
				FROM packages
				WHERE (
					package_name LIKE ? COLLATE NOCASE OR
					moniker LIKE ? COLLATE NOCASE
				)
				AND package_name <> ? COLLATE NOCASE
				AND moniker <> ? COLLATE NOCASE
				UNION ALL
				SELECT package_id, package_name, publisher, moniker, short_description, latest_version, tags_json, 3 AS bucket, 0.0 AS score
				FROM packages
				WHERE package_id LIKE ? COLLATE NOCASE AND package_id <> ? COLLATE NOCASE
				UNION ALL
				SELECT
					p.package_id,
					p.package_name,
					p.publisher,
					p.moniker,
					p.short_description,
					p.latest_version,
					p.tags_json,
					CASE
						WHEN p.package_name LIKE ? COLLATE NOCASE OR p.moniker LIKE ? COLLATE NOCASE THEN 2
						ELSE 4
					END AS bucket,
					bm25(package_search, 8.0, 16.0, 0.25, 6.0, 1.0, 0.5) AS score
				FROM package_search
				JOIN packages p ON p.package_id = package_search.package_id
				WHERE package_search MATCH ?
			), dedup AS (
				SELECT *, ROW_NUMBER() OVER (PARTITION BY package_id ORDER BY bucket, score) AS rn
				FROM ranked
			)
			SELECT package_id, package_name, publisher, moniker, short_description, latest_version, tags_json, score, bucket
			FROM dedup
			WHERE rn = 1
			ORDER BY bucket, score, package_id
			LIMIT ? OFFSET ?
		`, q, q, q, namePrefix, namePrefix, q, q, prefix, q, namePrefix, namePrefix, fts, limit, offset)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]Package, 0, limit)
	for rows.Next() {
		var p Package
		var tagsJSON string
		if err := rows.Scan(&p.PackageID, &p.PackageName, &p.Publisher, &p.Moniker, &p.ShortDescription, &p.LatestVersion, &tagsJSON, &p.Score, &p.RankBucket); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(tagsJSON), &p.Tags)
		out = append(out, p)
	}
	return out, rows.Err()
}

func (d *DB) GetPackage(ctx context.Context, packageID string) (Package, error) {
	var p Package
	var tagsJSON string
	err := d.sql.QueryRowContext(ctx, `
		SELECT package_id, package_name, publisher, moniker, short_description, latest_version, tags_json
		FROM packages
		WHERE package_id = ?
	`, packageID).Scan(&p.PackageID, &p.PackageName, &p.Publisher, &p.Moniker, &p.ShortDescription, &p.LatestVersion, &tagsJSON)
	if err != nil {
		return Package{}, err
	}
	_ = json.Unmarshal([]byte(tagsJSON), &p.Tags)
	return p, nil
}

func (d *DB) ListVersions(ctx context.Context, packageID string) ([]Version, error) {
	rows, err := d.sql.QueryContext(ctx, `
		SELECT DISTINCT package_version, channel
		FROM versions
		WHERE package_id = ?
		ORDER BY package_version DESC, channel
	`, packageID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]Version, 0, 8)
	for rows.Next() {
		var v Version
		if err := rows.Scan(&v.PackageVersion, &v.Channel); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (d *DB) GetLatestVersion(ctx context.Context, packageID string) (string, error) {
	var latest string
	err := d.sql.QueryRowContext(ctx, `SELECT latest_version FROM packages WHERE package_id = ?`, packageID).Scan(&latest)
	if err != nil {
		return "", err
	}
	if latest != "" {
		return latest, nil
	}

	rows, err := d.sql.QueryContext(ctx, `SELECT package_version FROM versions WHERE package_id = ?`, packageID)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return "", err
		}
		if latest == "" || compareVersion(v, latest) > 0 {
			latest = v
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return latest, nil
}

func compareVersion(a, b string) int {
	if a == b {
		return 0
	}
	ta := versionTokens(a)
	tb := versionTokens(b)
	n := len(ta)
	if len(tb) > n {
		n = len(tb)
	}
	for i := 0; i < n; i++ {
		if i >= len(ta) {
			if tokenHasValue(tb[i]) {
				return -1
			}
			continue
		}
		if i >= len(tb) {
			if tokenHasValue(ta[i]) {
				return 1
			}
			continue
		}
		if cmp := compareToken(ta[i], tb[i]); cmp != 0 {
			return cmp
		}
	}
	aa := strings.ToLower(a)
	bb := strings.ToLower(b)
	if aa > bb {
		return 1
	}
	if aa < bb {
		return -1
	}
	return 0
}

func versionTokens(v string) []string {
	parts := make([]string, 0, 8)
	var b strings.Builder
	mode := 0
	flush := func() {
		if b.Len() > 0 {
			parts = append(parts, b.String())
			b.Reset()
		}
	}
	for _, r := range v {
		switch {
		case r >= '0' && r <= '9':
			if mode != 1 {
				flush()
				mode = 1
			}
			b.WriteRune(r)
		case (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z'):
			if mode != 2 {
				flush()
				mode = 2
			}
			b.WriteRune(r)
		default:
			flush()
			mode = 0
		}
	}
	flush()
	return parts
}

func tokenHasValue(t string) bool {
	for _, r := range t {
		if r >= '1' && r <= '9' {
			return true
		}
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			return true
		}
	}
	return false
}

func compareToken(a, b string) int {
	aNum := isNumericToken(a)
	bNum := isNumericToken(b)
	if aNum && bNum {
		aa := strings.TrimLeft(a, "0")
		bb := strings.TrimLeft(b, "0")
		if aa == "" {
			aa = "0"
		}
		if bb == "" {
			bb = "0"
		}
		if len(aa) != len(bb) {
			if len(aa) > len(bb) {
				return 1
			}
			return -1
		}
		if aa > bb {
			return 1
		}
		if aa < bb {
			return -1
		}
		return 0
	}
	if aNum && !bNum {
		return 1
	}
	if !aNum && bNum {
		return -1
	}
	aa := strings.ToLower(a)
	bb := strings.ToLower(b)
	if aa > bb {
		return 1
	}
	if aa < bb {
		return -1
	}
	return 0
}

func isNumericToken(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func (d *DB) ListManifests(ctx context.Context, packageID, packageVersion string) ([]ManifestFile, error) {
	rows, err := d.sql.QueryContext(ctx, `
		SELECT manifest_id, path, manifest_type, package_locale, channel,
			package_name, publisher, moniker, short_description, tags_json, sha256
		FROM manifest_files
		WHERE package_id = ? AND package_version = ?
		ORDER BY path
	`, packageID, packageVersion)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]ManifestFile, 0, 8)
	for rows.Next() {
		var m ManifestFile
		var tagsJSON string
		if err := rows.Scan(
			&m.ManifestID,
			&m.Path,
			&m.ManifestType,
			&m.PackageLocale,
			&m.Channel,
			&m.PackageName,
			&m.Publisher,
			&m.Moniker,
			&m.ShortDescription,
			&tagsJSON,
			&m.SHA256,
		); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(tagsJSON), &m.Tags)
		out = append(out, m)
	}
	return out, rows.Err()
}

func (d *DB) GetManifest(ctx context.Context, packageID, packageVersion string, manifestID int64) (ManifestFile, error) {
	var m ManifestFile
	var tagsJSON string
	err := d.sql.QueryRowContext(ctx, `
		SELECT manifest_id, path, manifest_type, package_locale, channel,
			package_name, publisher, moniker, short_description, tags_json, sha256, updated_at, raw_yaml
		FROM manifest_files
		WHERE package_id = ? AND package_version = ? AND manifest_id = ?
	`, packageID, packageVersion, manifestID).Scan(
		&m.ManifestID,
		&m.Path,
		&m.ManifestType,
		&m.PackageLocale,
		&m.Channel,
		&m.PackageName,
		&m.Publisher,
		&m.Moniker,
		&m.ShortDescription,
		&tagsJSON,
		&m.SHA256,
		&m.UpdatedAt,
		&m.RawYAML,
	)
	if err != nil {
		return ManifestFile{}, err
	}
	_ = json.Unmarshal([]byte(tagsJSON), &m.Tags)
	return m, nil
}

func buildFTSQuery(input string) string {
	input = strings.TrimSpace(strings.ToLower(input))
	if input == "" {
		return ""
	}
	re := regexp.MustCompile(`[^a-z0-9._-]+`)
	parts := strings.Fields(re.ReplaceAllString(input, " "))
	if len(parts) == 0 {
		return ""
	}
	for i := range parts {
		parts[i] = parts[i] + "*"
	}
	return strings.Join(parts, " AND ")
}
