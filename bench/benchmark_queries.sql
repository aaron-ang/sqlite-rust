-- Benchmark read queries for sqlite-rust
-- Maximized diversity: each DB gets different query-type emphasis to avoid redundancy.
-- Run with: sqlite3 <db_path> < benchmark_queries.sql (or run per-section against the right DB)

-- =============================================================================
-- sample.db (tiny: apples 4 rows, oranges 6 rows)
-- Focus: COUNT baseline, multi-table, filtered scan
-- =============================================================================

-- COUNT (both tables)
SELECT COUNT(*) FROM apples;
SELECT COUNT(*) FROM oranges;

-- Multi-column full scan
SELECT id, name, color FROM apples;
SELECT id, name, description FROM oranges;

-- Filtered (WHERE text)
SELECT name, color FROM apples WHERE color = 'Yellow';
SELECT name FROM oranges WHERE name = 'Clementine';


-- =============================================================================
-- companies.db (~56k rows, index on country)
-- Focus: large full scan, indexed vs non-indexed WHERE
-- =============================================================================

-- Full-table count
SELECT COUNT(*) FROM companies;

-- Single-column full scan (minimal payload)
SELECT name FROM companies;

-- Filtered on indexed column (country)
SELECT name, country FROM companies WHERE country = 'dominican republic';

-- Filtered on indexed column with ORDER BY matching index (exercises skip-sort)
SELECT name, country FROM companies WHERE country = 'dominican republic' ORDER BY country;

-- Filtered on non-indexed column (industry)
SELECT name, industry FROM companies WHERE industry = 'computer software';


-- =============================================================================
-- superheroes.db (~7k rows, no index)
-- Focus: multi-column full scan, multiple filtered columns
-- =============================================================================

-- Multi-column full scan
SELECT id, name, eye_color, hair_color FROM superheroes;

-- Filtered on eye_color
SELECT name, eye_color FROM superheroes WHERE eye_color = 'Blue Eyes';

-- Filtered on hair_color (different column)
SELECT name, hair_color FROM superheroes WHERE hair_color = 'Black Hair';
