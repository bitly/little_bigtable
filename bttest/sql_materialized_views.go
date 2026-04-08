package bttest

import (
	"database/sql"
	"log"
)

// SqlMaterializedViews persists materialized view metadata to materialized_views_t.
type SqlMaterializedViews struct {
	db *sql.DB
}

// NewSqlMaterializedViews returns a SqlMaterializedViews backed by the given DB.
func NewSqlMaterializedViews(db *sql.DB) *SqlMaterializedViews {
	return &SqlMaterializedViews{db: db}
}

type storedMaterializedView struct {
	name               string
	query              string
	deletionProtection bool
}

// GetAll returns all persisted materialized views, used to restore state on startup.
func (m *SqlMaterializedViews) GetAll() []storedMaterializedView {
	rows, err := m.db.Query("SELECT name, query, deletion_protection FROM materialized_views_t")
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var result []storedMaterializedView
	for rows.Next() {
		var v storedMaterializedView
		var dp int
		if err := rows.Scan(&v.name, &v.query, &dp); err != nil {
			log.Fatal(err)
		}
		v.deletionProtection = dp != 0
		result = append(result, v)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	return result
}

// Save upserts a materialized view record. Called on CreateMaterializedView and
// UpdateMaterializedView to keep the persisted state in sync with in-memory state.
func (m *SqlMaterializedViews) Save(name, query string, deletionProtection bool) {
	dp := 0
	if deletionProtection {
		dp = 1
	}
	_, err := m.db.Exec(
		"INSERT INTO materialized_views_t (name, query, deletion_protection) VALUES (?, ?, ?)"+
			" ON CONFLICT(name) DO UPDATE SET query=excluded.query, deletion_protection=excluded.deletion_protection",
		name, query, dp,
	)
	if err != nil {
		log.Fatalf("saving materialized view %q: %v", name, err)
	}
}

// Delete removes a materialized view record by its full resource name.
func (m *SqlMaterializedViews) Delete(name string) {
	_, err := m.db.Exec("DELETE FROM materialized_views_t WHERE name = ?", name)
	if err != nil {
		log.Fatal(err)
	}
}
