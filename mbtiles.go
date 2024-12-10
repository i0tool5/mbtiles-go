package mbtiles

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"modernc.org/sqlite"
	_ "modernc.org/sqlite"
)

// MBtiles provides a basic handle for an mbtiles file.
type MBtiles struct {
	filename  string
	pool      *sql.DB
	format    TileFormat
	timestamp time.Time
	tilesize  uint32
}

// FindMBtiles recursively finds all mbtiles files within a given path.
func FindMBtiles(path string) ([]string, error) {
	var filenames []string
	err := filepath.Walk(path, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Ignore any that have an associated -journal file; these are incomplete
		if _, err := os.Stat(p + "-journal"); err == nil {
			return nil
		}
		if ext := filepath.Ext(p); ext == ".mbtiles" {
			filenames = append(filenames, p)

		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return filenames, err
}

type backupCloser interface {
	NewBackup(dstUri string) (*sqlite.Backup, error)
	NewRestore(srcUri string) (*sqlite.Backup, error)
	Close() error
}

// OpenInMemory opens an MBtiles file for reading, and validates that it has the correct
// structure. Then it loads it to in-memory database. Use this function only with files small enough to be
// loaded in-memory.
func OpenInMemory(path string) (*MBtiles, error) {
	modTime, err := getModTime(path)
	if err != nil {
		return nil, err
	}

	// open a single connection first while we are verifying the database
	// since there are issues closing out a connection pool on error here
	// we also use this connection to copy existing DB to in memory DB
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	srcConn, err := db.Conn(context.Background())
	if err != nil {
		return nil, err
	}
	defer srcConn.Close()

	inMemoryPath := "file::mem:?mode=memory"
	var inMemDB *sql.DB
	err = srcConn.Raw(func(driverConn any) error {
		driver, ok := driverConn.(backupCloser)
		if !ok {
			return errors.New("driver does not support backups")
		}
		bck, err := driver.NewBackup(inMemoryPath)
		if err != nil {
			return err
		}

		for more := true; more; {
			more, err = bck.Step(-1)
			if err != nil {
				return err
			}
		}
		conn, err := bck.Commit()
		if err != nil {
			return err
		}
		inMemDB = sql.OpenDB(newsqliteConnection(conn, db.Driver()))
		if inMemDB == nil {
			return errors.New("can not open in memory database backup")
		}

		return nil

	})
	if err != nil {
		return nil, err
	}

	dstConn, err := inMemDB.Conn(context.Background())
	if err != nil {
		return nil, err
	}

	err = validateRequiredTables(dstConn)
	if err != nil {
		return nil, err
	}
	format, tilesize, err := getTileFormatAndSize(dstConn)
	if err != nil {
		return nil, err
	}

	return &MBtiles{
		filename:  inMemoryPath,
		pool:      inMemDB,
		timestamp: modTime,
		format:    format,
		tilesize:  tilesize,
	}, nil
}

type sqliteConnection struct {
	conn   driver.Conn
	driver driver.Driver
}

func newsqliteConnection(conn driver.Conn, driver driver.Driver) sqliteConnection {
	return sqliteConnection{conn, driver}
}

// Connect implements method of driver.Connector interface.
func (s sqliteConnection) Connect(context.Context) (driver.Conn, error) {
	return s.conn, nil
}

// Driver implements method of driver.Connector interface.
func (s sqliteConnection) Driver() driver.Driver {
	return s.driver
}

// Open opens an MBtiles file for reading, and validates that it has the correct
// structure.
func Open(path string) (*MBtiles, error) {
	modTime, err := getModTime(path)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	con, err := db.Conn(context.Background())
	if err != nil {
		return nil, err
	}

	var version string
	err = con.QueryRowContext(context.TODO(), "select sqlite_version()").Scan(&version)
	if err != nil {
		return nil, err
	}

	err = validateRequiredTables(con)
	if err != nil {
		return nil, err
	}
	format, tilesize, err := getTileFormatAndSize(con)
	if err != nil {
		return nil, err
	}

	tilesDB := &MBtiles{
		filename:  path,
		pool:      db,
		timestamp: modTime,
		format:    format,
		tilesize:  tilesize,
	}

	return tilesDB, nil
}

func getModTime(path string) (time.Time, error) {
	stat, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return time.Time{}, fmt.Errorf("path does not exist: %q", path)
		}
		return time.Time{}, err
	}
	// there must not be a corresponding *-journal file (tileset is still being created)
	if _, err := os.Stat(path + "-journal"); err == nil {
		return time.Time{}, fmt.Errorf("refusing to open mbtiles file with associated -journal file (incomplete tileset)")
	}
	return stat.ModTime().Round(time.Second), nil
}

// Close closes a MBtiles file
func (db *MBtiles) Close() {
	if db.pool != nil {
		db.pool.Close()
	}
}

// ReadTile reads a tile for z, x, y into the provided *[]byte.
// data will be nil if the tile does not exist in the database
func (db *MBtiles) ReadTile(z int64, x int64, y int64, data *[]byte) error {
	if db == nil || db.pool == nil {
		return errors.New("cannot read tile from closed mbtiles database")
	}

	con, err := db.getConnection(context.TODO())
	defer db.closeConnection(con)
	if err != nil {
		return err
	}

	query, err := con.PrepareContext(context.TODO(), "select tile_data from tiles where zoom_level = $1 and tile_column = $2 and tile_row = $3")
	if err != nil {
		return err
	}
	defer query.Close()

	row := query.QueryRow(z, x, y)
	if row == nil {
		*data = nil
		return nil
	}
	if row.Err() != nil {
		return row.Err()
	}

	err = row.Scan(data)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}

	return nil
}

// ReadMetadata reads the metadata table into a map, casting their values into
// the appropriate type
func (db *MBtiles) ReadMetadata() (map[string]interface{}, error) {
	if db == nil || db.pool == nil {
		return nil, errors.New("cannot read tile from closed mbtiles database")
	}

	con, err := db.getConnection(context.TODO())
	defer db.closeConnection(con)
	if err != nil {
		return nil, err
	}

	metadata := make(map[string]interface{})

	query, err := con.PrepareContext(context.TODO(), "select name, value from metadata where value is not ''")
	if err != nil {
		return nil, err
	}
	defer query.Close()

	rows, err := query.Query()
	if err != nil {
		return nil, err
	}
	var (
		name  sql.NullString
		value sql.NullString
	)
	for rows.Next() {
		err := rows.Scan(&name, &value)
		if err != nil {
			return nil, err
		}

		switch name.String {
		case "maxzoom", "minzoom":
			metadata[name.String], err = strconv.Atoi(value.String)
			if err != nil {
				return nil, fmt.Errorf("cannot read metadata item %s: %v", name.String, err)
			}
		case "bounds", "center":
			metadata[name.String], err = parseFloats(value.String)
			if err != nil {
				return nil, fmt.Errorf("cannot read metadata item %s: %v", name.String, err)
			}
		case "json":
			err = json.Unmarshal([]byte(value.String), &metadata)
			if err != nil {
				return nil, fmt.Errorf("unable to parse JSON metadata item: %v", err)
			}
		default:
			metadata[name.String] = value.String
		}
	}

	// Supplement missing values by inferring from available data
	_, hasMinZoom := metadata["minzoom"]
	_, hasMaxZoom := metadata["maxzoom"]
	if !(hasMinZoom && hasMaxZoom) {
		q2, err := con.PrepareContext(context.TODO(), "select min(zoom_level), max(zoom_level) from tiles")
		if err != nil {
			return nil, err
		}
		defer q2.Close()
		row := q2.QueryRow()
		var (
			minZoom, maxZoom int
		)
		row.Scan(&minZoom, &maxZoom)

		metadata["minzoom"] = minZoom
		metadata["maxzoom"] = maxZoom
	}
	return metadata, nil
}

func (db *MBtiles) GetFilename() string {
	return db.filename
}

// GetTileFormat returns the TileFormat of the mbtiles file.
func (db *MBtiles) GetTileFormat() TileFormat {
	return db.format
}

// GetTileSize returns the tile size in pixels of the mbtiles file, if detected.
// Returns 0 if tile size is not detected.
func (db *MBtiles) GetTileSize() uint32 {
	return db.tilesize
}

// Timestamp returns the time stamp of the mbtiles file.
func (db *MBtiles) GetTimestamp() time.Time {
	return db.timestamp
}

// getConnection gets a sqlite.Conn from an open connection pool.
// closeConnection(con) must be called to release the connection.
func (db *MBtiles) getConnection(ctx context.Context) (*sql.Conn, error) {
	con, _ := db.pool.Conn(ctx)
	if con == nil {
		return nil, errors.New("connection could not be opened")
	}
	return con, nil
}

// closeConnection closes an open sqlite.Conn and returns it to the pool.
func (db *MBtiles) closeConnection(con *sql.Conn) {
	con.Close()
}

// validateRequiredTables checks that both 'tiles' and 'metadata' tables are
// present in the database
func validateRequiredTables(con *sql.Conn) error {
	query, err := con.PrepareContext(context.Background(), "SELECT count(*) as c FROM sqlite_master WHERE name in ('tiles', 'metadata')")
	if err != nil {
		return err
	}
	defer query.Close()

	result := query.QueryRow()
	if err := result.Err(); err != nil {
		return err
	}

	count := 0
	result.Scan(&count)
	if count < 2 {
		return errors.New("missing one or more required tables: tiles, metadata")
	}
	return nil
}

// getTileFormatAndSize reads the first tile in the database to detect the tile
// format and if PNG also the size.
// See TileFormat for list of supported tile formats.
func getTileFormatAndSize(con *sql.Conn) (TileFormat, uint32, error) {
	var tilesize uint32 = 0 // not detected for all formats

	query, err := con.PrepareContext(context.Background(), "select tile_data from tiles limit 1")
	if err != nil {
		return UNKNOWN, tilesize, err
	}
	defer query.Close()

	hasRow := query.QueryRow()

	tileData := make([]byte, 0)
	hasRow.Scan(&tileData)

	format, err := detectTileFormat(tileData)
	if err != nil {
		return UNKNOWN, tilesize, err
	}

	// GZIP masks PBF, which is only expected type for tiles in GZIP format
	if format == GZIP {
		format = PBF
	}

	tilesize, err = detectTileSize(format, tileData)
	if err != nil {
		return format, tilesize, err
	}

	return format, tilesize, nil
}

// parseFloats converts a commma-delimited string of floats to a slice of
// float64 and returns it and the first error that was encountered.
// Example: "1.5,2.1" => [1.5, 2.1]
func parseFloats(str string) ([]float64, error) {
	split := strings.Split(str, ",")
	var out []float64
	for _, v := range split {
		value, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return out, fmt.Errorf("could not parse %q to floats: %v", str, err)
		}
		out = append(out, value)
	}
	return out, nil
}
