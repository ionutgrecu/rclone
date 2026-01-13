// Package dbwrapper implements a virtual backend that manages file replication
// across multiple remotes using a MySQL/MariaDB database for configuration and tracking.
package dbwrapper

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/hash"
	"golang.org/x/sync/errgroup"
)

// Register with Fs
func init() {
	fsi := &fs.RegInfo{
		Name:        "dbwrapper",
		Description: "Database-managed multi-remote storage with replication",
		NewFs:       NewFs,
		Options: []fs.Option{
			{
				Name:     "db_dsn",
				Help:     "MySQL/MariaDB DSN (Data Source Name).\n\nFormat: user:password@tcp(host:port)/database",
				Required: true,
			},
			{
				Name:     "db_max_open_conns",
				Help:     "Maximum number of open connections to the database.",
				Default:  10,
				Advanced: true,
			},
			{
				Name:     "db_max_idle_conns",
				Help:     "Maximum number of idle connections in the pool.",
				Default:  5,
				Advanced: true,
			},
			{
				Name:     "db_conn_max_lifetime",
				Help:     "Maximum lifetime of a connection (in minutes).",
				Default:  30,
				Advanced: true,
			},
		},
	}
	fs.Register(fsi)
}

// Options defines the configuration for this backend
type Options struct {
	DbDSN             string `config:"db_dsn"`
	DbMaxOpenConns    int    `config:"db_max_open_conns"`
	DbMaxIdleConns    int    `config:"db_max_idle_conns"`
	DbConnMaxLifetime int    `config:"db_conn_max_lifetime"`
}

// remoteInfo holds information about a configured remote
type remoteInfo struct {
	ID        int64
	Name      string
	RclonePath string
	IsActive  bool
	Weight    int
	Fs        fs.Fs
}

// Fs represents a database-managed multi-remote filesystem
type Fs struct {
	name     string
	root     string
	opt      Options
	features *fs.Features
	db       *sql.DB
	remotes  []*remoteInfo
	mu       sync.RWMutex // protects remotes slice
}

// Object represents a file in the dbwrapper filesystem
type Object struct {
	fs       *Fs
	remote   string
	size     int64
	modTime  time.Time
	hash     string
	fileID   int64
	replicas []replicaInfo
}

// replicaInfo holds information about a file replica
type replicaInfo struct {
	RemoteID   int64
	RemoteName string
	RclonePath string
	Status     string
}

// Directory represents a directory derived from file paths in the database
type Directory struct {
	fs     *Fs
	remote string
}

// Fs returns the parent Fs
func (d *Directory) Fs() fs.Info {
	return d.fs
}

// String returns the name of the directory
func (d *Directory) String() string {
	return d.remote
}

// Remote returns the remote path
func (d *Directory) Remote() string {
	return d.remote
}

// ModTime returns the modification time (unknown for derived directories)
func (d *Directory) ModTime(ctx context.Context) time.Time {
	return time.Time{}
}

// Size returns the size (0 for directories)
func (d *Directory) Size() int64 {
	return 0
}

// Items returns the count of items in this directory (unknown)
func (d *Directory) Items() int64 {
	return -1
}

// ID returns the internal ID of this directory (empty for derived)
func (d *Directory) ID() string {
	return ""
}

// ------------------------------------------------------------
// Fs implementation
// ------------------------------------------------------------

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	return fmt.Sprintf("dbwrapper root '%s'", f.root)
}

// Precision returns the precision of the filesystem
func (f *Fs) Precision() time.Duration {
	// Return the lowest precision among all remotes
	var precision time.Duration = time.Second
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, r := range f.remotes {
		if r.Fs != nil {
			p := r.Fs.Precision()
			if p > precision {
				precision = p
			}
		}
	}
	return precision
}

// Hashes returns the supported hash types
func (f *Fs) Hashes() hash.Set {
	// Always support MD5 since we compute it locally during upload
	// This ensures hash verification works even when underlying remotes
	// don't return hashes (e.g., S3 multipart uploads)
	return hash.NewHashSet(hash.MD5)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// NewFs constructs an Fs from the path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, fmt.Errorf("dbwrapper: failed to parse options: %w", err)
	}

	if opt.DbDSN == "" {
		return nil, errors.New("dbwrapper: db_dsn is required")
	}

	// Ensure parseTime=true is in the DSN for proper time.Time scanning
	dsn := opt.DbDSN
	if !strings.Contains(dsn, "parseTime=") {
		if strings.Contains(dsn, "?") {
			dsn = dsn + "&parseTime=true"
		} else {
			dsn = dsn + "?parseTime=true"
		}
	}

	// Initialize database connection with pooling
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("dbwrapper: failed to open database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(opt.DbMaxOpenConns)
	db.SetMaxIdleConns(opt.DbMaxIdleConns)
	db.SetConnMaxLifetime(time.Duration(opt.DbConnMaxLifetime) * time.Minute)

	// Test the connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("dbwrapper: failed to connect to database: %w", err)
	}

	root = strings.Trim(root, "/")

	f := &Fs{
		name: name,
		root: root,
		opt:  *opt,
		db:   db,
	}

	// Load remotes from database
	if err := f.loadRemotes(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("dbwrapper: failed to load remotes: %w", err)
	}

	if len(f.remotes) == 0 {
		db.Close()
		return nil, errors.New("dbwrapper: no active remotes found in database")
	}

	// Set up features
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
		DuplicateFiles:          false,
	}).Fill(ctx, f)

	// Mark as overlay
	f.features.Overlay = true

	return f, nil
}

// loadRemotes loads all active remotes from the database and instantiates their Fs
func (f *Fs) loadRemotes(ctx context.Context) error {
	rows, err := f.db.QueryContext(ctx, `
		SELECT id, name, rclone_path, is_active, weight
		FROM remotes
		WHERE is_active = TRUE
		ORDER BY weight DESC
	`)
	if err != nil {
		return fmt.Errorf("failed to query remotes: %w", err)
	}
	defer rows.Close()

	var remotes []*remoteInfo
	for rows.Next() {
		r := &remoteInfo{}
		if err := rows.Scan(&r.ID, &r.Name, &r.RclonePath, &r.IsActive, &r.Weight); err != nil {
			return fmt.Errorf("failed to scan remote row: %w", err)
		}

		// Build the full path including root
		remotePath := r.RclonePath
		if f.root != "" {
			remotePath = path.Join(r.RclonePath, f.root)
		}

		// Instantiate the child Fs
		childFs, err := fs.NewFs(ctx, remotePath)
		if err != nil && err != fs.ErrorIsFile {
			fs.Errorf(nil, "dbwrapper: failed to create Fs for remote %q (%s): %v", r.Name, remotePath, err)
			continue
		}
		r.Fs = childFs
		remotes = append(remotes, r)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating remote rows: %w", err)
	}

	f.mu.Lock()
	f.remotes = remotes
	f.mu.Unlock()

	fs.Debugf(f, "loaded %d active remotes", len(remotes))
	return nil
}

// getRedundancyLevel fetches the current redundancy level from the database
func (f *Fs) getRedundancyLevel(ctx context.Context) (int, error) {
	var value string
	err := f.db.QueryRowContext(ctx, "SELECT `value` FROM settings WHERE `key` = 'redundancy_level'").Scan(&value)
	if err != nil {
		if err == sql.ErrNoRows {
			return 1, nil // Default to 1 if not set
		}
		return 0, fmt.Errorf("failed to query redundancy_level: %w", err)
	}
	level, err := strconv.Atoi(value)
	if err != nil {
		return 1, nil // Default to 1 if invalid
	}
	return level, nil
}

// getActiveRemotes returns a copy of the active remotes slice
func (f *Fs) getActiveRemotes() []*remoteInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()
	remotes := make([]*remoteInfo, len(f.remotes))
	copy(remotes, f.remotes)
	return remotes
}

// List the objects and directories in dir from the database
func (f *Fs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	// Build the full path prefix for querying
	prefix := ""
	if f.root != "" {
		prefix = f.root
		if dir != "" {
			prefix = path.Join(f.root, dir)
		}
	} else {
		prefix = dir
	}

	// Normalize prefix for DB query
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	// Query files from database that are in this directory
	// We need to find:
	// 1. Files that are direct children of this directory
	// 2. Directories (derived from file paths that have subdirectories)
	var query string
	var args []interface{}

	if prefix == "" {
		// Root directory - find all files
		query = `SELECT id, remote_path, size, hash, created_at FROM files`
	} else {
		// Specific directory - find files that start with this prefix
		query = `SELECT id, remote_path, size, hash, created_at FROM files WHERE remote_path LIKE ?`
		args = append(args, prefix+"%")
	}

	rows, err := f.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query files: %w", err)
	}
	defer rows.Close()

	var result fs.DirEntries
	seenDirs := make(map[string]bool)

	for rows.Next() {
		var fileID int64
		var remotePath string
		var size int64
		var hashValue sql.NullString
		var createdAt time.Time

		if err := rows.Scan(&fileID, &remotePath, &size, &hashValue, &createdAt); err != nil {
			return nil, fmt.Errorf("failed to scan file row: %w", err)
		}

		// Calculate the relative path from the listing directory
		var relativePath string
		if prefix == "" {
			relativePath = remotePath
		} else {
			if !strings.HasPrefix(remotePath, prefix) {
				continue
			}
			relativePath = strings.TrimPrefix(remotePath, prefix)
		}

		// Check if this is a direct child or in a subdirectory
		parts := strings.SplitN(relativePath, "/", 2)
		if len(parts) == 1 {
			// Direct child file
			remote := relativePath
			if dir != "" {
				remote = path.Join(dir, relativePath)
			}

			obj := &Object{
				fs:      f,
				remote:  remote,
				size:    size,
				modTime: createdAt,
				fileID:  fileID,
			}
			if hashValue.Valid {
				obj.hash = hashValue.String
			}
			// Load replicas for the object
			obj.loadReplicas(ctx)
			result = append(result, obj)
		} else {
			// File is in a subdirectory - record the directory
			subDir := parts[0]
			if !seenDirs[subDir] {
				seenDirs[subDir] = true
				dirRemote := subDir
				if dir != "" {
					dirRemote = path.Join(dir, subDir)
				}
				result = append(result, &Directory{
					fs:     f,
					remote: dirRemote,
				})
			}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating file rows: %w", err)
	}

	return result, nil
}

// NewObject finds the Object at remote from the database
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	// Find the file in the database only
	fullPath := remote
	if f.root != "" {
		fullPath = path.Join(f.root, remote)
	}

	// Query file info from database
	var fileID int64
	var size int64
	var hashValue sql.NullString
	var createdAt time.Time
	err := f.db.QueryRowContext(ctx, `
		SELECT id, size, hash, created_at
		FROM files
		WHERE remote_path = ?
	`, fullPath).Scan(&fileID, &size, &hashValue, &createdAt)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, fmt.Errorf("failed to query file: %w", err)
	}

	// File found in database
	obj := &Object{
		fs:      f,
		remote:  remote,
		fileID:  fileID,
		size:    size,
		modTime: createdAt,
	}
	if hashValue.Valid {
		obj.hash = hashValue.String
	}

	// Load replicas
	if err := obj.loadReplicas(ctx); err != nil {
		fs.Debugf(f, "failed to load replicas for %q: %v", remote, err)
	}

	// Try to get a more accurate modTime from one of the replicas
	for _, replica := range obj.replicas {
		remoteFs := f.getRemoteByID(replica.RemoteID)
		if remoteFs != nil && remoteFs.Fs != nil {
			childObj, err := remoteFs.Fs.NewObject(ctx, remote)
			if err == nil {
				obj.modTime = childObj.ModTime(ctx)
				break
			}
		}
	}

	return obj, nil
}

// getRemoteByID finds a remote by its database ID
func (f *Fs) getRemoteByID(id int64) *remoteInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, r := range f.remotes {
		if r.ID == id {
			return r
		}
	}
	return nil
}

// Put uploads a file to multiple remotes based on redundancy_level
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	remote := src.Remote()
	fullPath := remote
	if f.root != "" {
		fullPath = path.Join(f.root, remote)
	}

	// Get the redundancy level
	redundancyLevel, err := f.getRedundancyLevel(ctx)
	if err != nil {
		fs.Errorf(f, "failed to get redundancy level, using 1: %v", err)
		redundancyLevel = 1
	}

	remotes := f.getActiveRemotes()
	if len(remotes) == 0 {
		return nil, errors.New("no active remotes available")
	}

	// Cap redundancy level to available remotes
	targetReplicas := redundancyLevel
	if targetReplicas > len(remotes) {
		targetReplicas = len(remotes)
	}

	// Read the entire content into memory for multi-upload
	// In production, consider using a temp file for large files
	data, err := io.ReadAll(in)
	if err != nil {
		return nil, fmt.Errorf("failed to read input: %w", err)
	}

	// Compute MD5 hash locally - this ensures we have a valid hash
	// even when underlying remotes don't return one (e.g., S3 multipart uploads)
	md5Hash := md5.Sum(data)
	hashValue := hex.EncodeToString(md5Hash[:])

	// Use errgroup for parallel uploads
	g, gCtx := errgroup.WithContext(ctx)
	var uploadMu sync.Mutex
	successfulRemotes := make([]*remoteInfo, 0, targetReplicas)
	uploadedObjects := make([]fs.Object, 0, targetReplicas)

	// Select remotes for upload (ordered by weight)
	uploadRemotes := remotes
	if len(uploadRemotes) > targetReplicas {
		uploadRemotes = uploadRemotes[:targetReplicas]
	}

	for _, r := range uploadRemotes {
		r := r // capture for goroutine
		g.Go(func() error {
			if r.Fs == nil {
				return fmt.Errorf("remote %q has no filesystem", r.Name)
			}

			// Create a new reader for this upload
			reader := &readSeekCloser{data: data}

			// Wrap src with our reader
			wrappedSrc := &objectInfoWrapper{
				ObjectInfo: src,
				size:       int64(len(data)),
			}

			obj, err := r.Fs.Put(gCtx, reader, wrappedSrc, options...)
			if err != nil {
				fs.Errorf(f, "upload to remote %q failed: %v", r.Name, err)
				return err
			}

			uploadMu.Lock()
			successfulRemotes = append(successfulRemotes, r)
			uploadedObjects = append(uploadedObjects, obj)
			uploadMu.Unlock()

			return nil
		})
	}

	// Wait for all uploads (don't fail on partial success)
	uploadErr := g.Wait()

	if len(successfulRemotes) == 0 {
		return nil, fmt.Errorf("all uploads failed: %w", uploadErr)
	}

	if len(successfulRemotes) < targetReplicas {
		fs.Logf(f, "WARNING: only %d of %d replicas created for %q", len(successfulRemotes), targetReplicas, remote)
	}

	// Record file and replicas in database
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Insert or update file record
	result, err := tx.ExecContext(ctx, `
		INSERT INTO files (remote_path, size, hash)
		VALUES (?, ?, ?)
		ON DUPLICATE KEY UPDATE size = VALUES(size), hash = VALUES(hash)
	`, fullPath, src.Size(), hashValue)
	if err != nil {
		return nil, fmt.Errorf("failed to insert file record: %w", err)
	}

	fileID, err := result.LastInsertId()
	if err != nil || fileID == 0 {
		// Get the existing file ID
		err = tx.QueryRowContext(ctx, "SELECT id FROM files WHERE remote_path = ?", fullPath).Scan(&fileID)
		if err != nil {
			return nil, fmt.Errorf("failed to get file ID: %w", err)
		}
	}

	// Delete old replicas
	_, err = tx.ExecContext(ctx, "DELETE FROM file_replicas WHERE file_id = ?", fileID)
	if err != nil {
		return nil, fmt.Errorf("failed to delete old replicas: %w", err)
	}

	// Insert new replicas
	for _, r := range successfulRemotes {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO file_replicas (file_id, remote_id, status)
			VALUES (?, ?, 'active')
		`, fileID, r.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to insert replica record: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Return the wrapper object
	obj := &Object{
		fs:      f,
		remote:  remote,
		size:    src.Size(),
		modTime: src.ModTime(ctx),
		hash:    hashValue,
		fileID:  fileID,
	}

	// Load replicas for the object
	obj.loadReplicas(ctx)

	return obj, nil
}

// Mkdir creates the directory
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	remotes := f.getActiveRemotes()
	if len(remotes) == 0 {
		return errors.New("no active remotes available")
	}

	// Create directory on all active remotes
	var lastErr error
	successCount := 0
	for _, r := range remotes {
		if r.Fs == nil {
			continue
		}
		err := r.Fs.Mkdir(ctx, dir)
		if err != nil {
			fs.Debugf(f, "mkdir failed on remote %q: %v", r.Name, err)
			lastErr = err
		} else {
			successCount++
		}
	}

	if successCount == 0 {
		return fmt.Errorf("mkdir failed on all remotes: %w", lastErr)
	}

	return nil
}

// Rmdir removes the directory
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	remotes := f.getActiveRemotes()
	if len(remotes) == 0 {
		return errors.New("no active remotes available")
	}

	var lastErr error
	successCount := 0
	for _, r := range remotes {
		if r.Fs == nil {
			continue
		}
		err := r.Fs.Rmdir(ctx, dir)
		if err != nil {
			if err != fs.ErrorDirNotFound {
				fs.Debugf(f, "rmdir failed on remote %q: %v", r.Name, err)
				lastErr = err
			}
		} else {
			successCount++
		}
	}

	if successCount == 0 && lastErr != nil {
		return lastErr
	}

	return nil
}

// Shutdown closes the database connection
func (f *Fs) Shutdown(ctx context.Context) error {
	if f.db != nil {
		return f.db.Close()
	}
	return nil
}

// ------------------------------------------------------------
// Object implementation
// ------------------------------------------------------------

// loadReplicas loads replica information from the database
func (o *Object) loadReplicas(ctx context.Context) error {
	if o.fileID == 0 {
		return nil
	}

	rows, err := o.fs.db.QueryContext(ctx, `
		SELECT fr.remote_id, r.name, r.rclone_path, fr.status
		FROM file_replicas fr
		JOIN remotes r ON fr.remote_id = r.id
		WHERE fr.file_id = ? AND fr.status = 'active'
		ORDER BY r.weight DESC
	`, o.fileID)
	if err != nil {
		return err
	}
	defer rows.Close()

	var replicas []replicaInfo
	for rows.Next() {
		var r replicaInfo
		if err := rows.Scan(&r.RemoteID, &r.RemoteName, &r.RclonePath, &r.Status); err != nil {
			return err
		}
		replicas = append(replicas, r)
	}

	o.replicas = replicas
	return rows.Err()
}

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// String returns the name of the object
func (o *Object) String() string {
	return o.remote
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// ModTime returns the modification time
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

// Size returns the size of the object
func (o *Object) Size() int64 {
	return o.size
}

// Hash returns the hash of the object from replicas
func (o *Object) Hash(ctx context.Context, ty hash.Type) (string, error) {
	// Check if this hash type is supported by the filesystem
	if !o.fs.Hashes().Contains(ty) {
		return "", hash.ErrUnsupported
	}

	// Return stored MD5 hash if available and requested
	if ty == hash.MD5 && o.hash != "" {
		return o.hash, nil
	}

	// Try to get the hash from replicas
	for _, replica := range o.replicas {
		remoteInfo := o.fs.getRemoteByID(replica.RemoteID)
		if remoteInfo == nil || remoteInfo.Fs == nil {
			continue
		}
		childObj, err := remoteInfo.Fs.NewObject(ctx, o.remote)
		if err != nil {
			continue
		}
		hashVal, err := childObj.Hash(ctx, ty)
		if err == nil && hashVal != "" {
			// Cache MD5 hash if we got one
			if ty == hash.MD5 {
				o.hash = hashVal
			}
			return hashVal, nil
		}
	}

	return "", hash.ErrUnsupported
}

// Storable returns whether this object is storable
func (o *Object) Storable() bool {
	return true
}

// SetModTime sets the modification time
func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	// Set modtime on all replicas
	for _, replica := range o.replicas {
		remoteInfo := o.fs.getRemoteByID(replica.RemoteID)
		if remoteInfo != nil && remoteInfo.Fs != nil {
			childObj, err := remoteInfo.Fs.NewObject(ctx, o.remote)
			if err == nil {
				childObj.SetModTime(ctx, t)
			}
		}
	}
	o.modTime = t
	return nil
}

// Open opens the object for reading by choosing a replica from the database
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	// Must have replicas from the database to read
	if len(o.replicas) == 0 {
		return nil, errors.New("no replicas available for this file")
	}

	// Try replicas in order (ordered by weight from loadReplicas)
	var lastErr error
	for _, replica := range o.replicas {
		remoteInfo := o.fs.getRemoteByID(replica.RemoteID)
		if remoteInfo == nil || remoteInfo.Fs == nil {
			fs.Debugf(o.fs, "remote %q not available for replica", replica.RemoteName)
			continue
		}

		childObj, err := remoteInfo.Fs.NewObject(ctx, o.remote)
		if err != nil {
			fs.Debugf(o.fs, "failed to get object from remote %q: %v", replica.RemoteName, err)
			lastErr = err
			continue
		}

		rc, err := childObj.Open(ctx, options...)
		if err != nil {
			fs.Debugf(o.fs, "failed to open object from remote %q: %v", replica.RemoteName, err)
			lastErr = err
			continue
		}

		fs.Debugf(o.fs, "successfully opened %q from replica on remote %q", o.remote, replica.RemoteName)
		return rc, nil
	}

	if lastErr != nil {
		return nil, fmt.Errorf("failed to open from any replica: %w", lastErr)
	}

	return nil, errors.New("no working replicas found")
}

// Update updates the object with new content
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	// Re-upload using Put
	newObj, err := o.fs.Put(ctx, in, src, options...)
	if err != nil {
		return err
	}

	// Update our state from the new object
	if newO, ok := newObj.(*Object); ok {
		o.size = newO.size
		o.modTime = newO.modTime
		o.hash = newO.hash
		o.fileID = newO.fileID
		o.replicas = newO.replicas
	}

	return nil
}

// Remove removes the object from all remotes and the database
func (o *Object) Remove(ctx context.Context) error {
	// Remove from all replicas
	var lastErr error
	for _, replica := range o.replicas {
		remoteInfo := o.fs.getRemoteByID(replica.RemoteID)
		if remoteInfo != nil && remoteInfo.Fs != nil {
			childObj, err := remoteInfo.Fs.NewObject(ctx, o.remote)
			if err == nil {
				if err := childObj.Remove(ctx); err != nil {
					fs.Debugf(o.fs, "failed to remove from remote %q: %v", replica.RemoteName, err)
					lastErr = err
				}
			}
		}
	}

	// Also try to remove from all active remotes (in case DB is out of sync)
	remotes := o.fs.getActiveRemotes()
	for _, r := range remotes {
		if r.Fs == nil {
			continue
		}
		childObj, err := r.Fs.NewObject(ctx, o.remote)
		if err == nil {
			if err := childObj.Remove(ctx); err != nil && err != fs.ErrorObjectNotFound {
				fs.Debugf(o.fs, "failed to remove from remote %q: %v", r.Name, err)
				lastErr = err
			}
		}
	}

	// Remove from database
	if o.fileID > 0 {
		_, err := o.fs.db.ExecContext(ctx, "DELETE FROM files WHERE id = ?", o.fileID)
		if err != nil {
			fs.Errorf(o.fs, "failed to delete file record: %v", err)
		}
	} else {
		fullPath := o.remote
		if o.fs.root != "" {
			fullPath = path.Join(o.fs.root, o.remote)
		}
		_, err := o.fs.db.ExecContext(ctx, "DELETE FROM files WHERE remote_path = ?", fullPath)
		if err != nil {
			fs.Errorf(o.fs, "failed to delete file record: %v", err)
		}
	}

	return lastErr
}

// ------------------------------------------------------------
// Helper types
// ------------------------------------------------------------

// readSeekCloser wraps a byte slice as io.Reader
type readSeekCloser struct {
	data   []byte
	offset int
}

func (r *readSeekCloser) Read(p []byte) (n int, err error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}

func (r *readSeekCloser) Close() error {
	return nil
}

// objectInfoWrapper wraps an ObjectInfo with an overridden size
type objectInfoWrapper struct {
	fs.ObjectInfo
	size int64
}

func (w *objectInfoWrapper) Size() int64 {
	return w.size
}

// ------------------------------------------------------------
// Interface checks
// ------------------------------------------------------------

var (
	_ fs.Fs         = (*Fs)(nil)
	_ fs.Object     = (*Object)(nil)
	_ fs.Directory  = (*Directory)(nil)
	_ fs.Shutdowner = (*Fs)(nil)
)
