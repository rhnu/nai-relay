package relay

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// Store owns a single SQLite connection and an exclusive process lock.
type Store struct {
	db      *sql.DB
	n       int
	reserve float64
	policy  string
}

func OpenStore(path string, n int, reserve float64, policy string) (*Store, error) {
	if err := NewLedger(n, reserve, policy).validate(); err != nil {
		return nil, err
	}
	if path != "" && path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return nil, fmt.Errorf("create database directory: %w", err)
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	s := &Store{db: db, n: n, reserve: reserve, policy: NewLedger(n, reserve, policy).Policy}
	// DELETE journaling retains the exclusive lock until this connection closes.
	for _, stmt := range []string{"PRAGMA busy_timeout=1000", "PRAGMA journal_mode=DELETE", "PRAGMA locking_mode=EXCLUSIVE", "BEGIN EXCLUSIVE", "CREATE TABLE IF NOT EXISTS ledger (id INTEGER PRIMARY KEY CHECK (id=1), user_count INTEGER NOT NULL, state BLOB NOT NULL)", "COMMIT"} {
		if _, err = db.Exec(stmt); err != nil {
			db.Close()
			return nil, fmt.Errorf("open quota database: %w", err)
		}
	}
	return s, nil
}

func (s *Store) Load() (*Ledger, error) {
	var count int
	var data []byte
	err := s.db.QueryRow("SELECT user_count,state FROM ledger WHERE id=1").Scan(&count, &data)
	if errors.Is(err, sql.ErrNoRows) {
		return NewLedger(s.n, s.reserve, s.policy), nil
	}
	if err != nil {
		return nil, err
	}
	if count != s.n {
		return nil, fmt.Errorf("database has %d users; configuration has %d", count, s.n)
	}
	l := new(Ledger)
	if err = json.Unmarshal(data, l); err != nil {
		return nil, fmt.Errorf("decode quota database: %w", err)
	}
	if len(l.Users) != s.n {
		return nil, errors.New("database user count differs from configuration")
	}
	if err = l.validate(); err != nil {
		return nil, err
	}
	if l.Pending == nil {
		l.Pending = make(map[string]reservation)
	}
	if err = l.RecoverPending(); err != nil {
		return nil, err
	}
	if err = l.Reconfigure(s.reserve, s.policy); err != nil {
		return nil, err
	}
	l.AwaitingFresh = true
	if err = s.Save(l); err != nil {
		return nil, fmt.Errorf("persist reservation recovery: %w", err)
	}
	return l, nil
}

func (s *Store) Save(l *Ledger) error {
	if len(l.Users) != s.n {
		return errors.New("cannot save different user count")
	}
	// Upstream raw response fields are useful only for the live response rewrite.
	// Persist the typed financial snapshot, never arbitrary account metadata.
	copyLedger := *l
	copyLedger.Latest = l.Latest
	copyLedger.Latest.Raw = nil
	data, err := json.Marshal(&copyLedger)
	if err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec("INSERT INTO ledger(id,user_count,state) VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET user_count=excluded.user_count,state=excluded.state", s.n, data); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Close() error { return s.db.Close() }
