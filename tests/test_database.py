"""Tests for database module."""

import pytest
import duckdb
from pathlib import Path


class TestDatabaseConnection:
    """Tests for database connection functions."""

    def test_get_memory_connection(self):
        """Test in-memory connection."""
        from src.database import get_memory_connection

        conn = get_memory_connection()
        assert conn is not None

        # Test that we can execute queries
        result = conn.execute("SELECT 1").fetchone()
        assert result[0] == 1

        conn.close()

    def test_get_connection_creates_file(self, tmp_path):
        """Test file-based connection."""
        from src.database import get_connection

        db_path = tmp_path / "test.duckdb"

        with get_connection(db_path) as conn:
            conn.execute("CREATE TABLE test (id INTEGER)")
            conn.execute("INSERT INTO test VALUES (1)")

        assert db_path.exists()


class TestSchema:
    """Tests for database schema functions."""

    def test_create_all_tables(self):
        """Test table creation."""
        from src.database import create_all_tables, get_memory_connection

        conn = get_memory_connection()
        create_all_tables(conn)

        # Check that tables exist
        tables = conn.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'main'
        """).fetchall()

        table_names = [t[0] for t in tables]
        assert "master_events" in table_names
        assert "devices" in table_names
        assert "patients" in table_names
        assert "mdr_text" in table_names

        conn.close()

    def test_get_table_counts(self, test_db):
        """Test table count retrieval."""
        from src.database import get_table_counts

        counts = get_table_counts(test_db)

        assert isinstance(counts, dict)
        assert counts.get("master_events") == 5
        assert counts.get("devices") == 5

    def test_initialize_database(self):
        """Test full database initialization."""
        from src.database import initialize_database, get_memory_connection

        conn = get_memory_connection()
        initialize_database(conn)

        # Verify tables exist
        tables = conn.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'main'
        """).fetchone()[0]

        assert tables > 0

        conn.close()


class TestMaintenance:
    """Tests for database maintenance functions."""

    def test_vacuum_database(self, tmp_path):
        """Test vacuum operation."""
        from src.database import vacuum_database, get_connection, initialize_database

        db_path = tmp_path / "test.duckdb"

        # Create and populate database
        with get_connection(db_path) as conn:
            initialize_database(conn)
            conn.execute("INSERT INTO master_events (mdr_report_key) VALUES ('TEST001')")

        # Run vacuum
        result = vacuum_database(db_path)

        assert result.success == True
        assert result.operation == "vacuum"

    def test_analyze_tables(self, tmp_path):
        """Test analyze operation."""
        from src.database import analyze_tables, get_connection, initialize_database

        db_path = tmp_path / "test.duckdb"

        with get_connection(db_path) as conn:
            initialize_database(conn)

        result = analyze_tables(db_path)

        assert result.success == True
        assert result.operation == "analyze"

    def test_create_backup(self, tmp_path):
        """Test backup creation."""
        from src.database import create_backup, get_connection, initialize_database

        db_path = tmp_path / "test.duckdb"
        backup_dir = tmp_path / "backups"

        with get_connection(db_path) as conn:
            initialize_database(conn)

        result = create_backup(db_path, backup_dir)

        assert result.success == True
        assert result.operation == "backup"
        assert backup_dir.exists()
        assert len(list(backup_dir.glob("*.duckdb"))) == 1

    def test_list_backups(self, tmp_path):
        """Test backup listing."""
        from src.database import list_backups, create_backup, get_connection, initialize_database

        db_path = tmp_path / "test.duckdb"
        backup_dir = tmp_path / "backups"

        with get_connection(db_path) as conn:
            initialize_database(conn)

        # Create a backup
        create_backup(db_path, backup_dir)

        # List backups
        backups = list_backups(backup_dir)

        assert len(backups) == 1
        assert "filename" in backups[0]
        assert "size_mb" in backups[0]

    def test_get_table_statistics(self, test_db, tmp_path):
        """Test table statistics retrieval."""
        from src.database import get_table_statistics

        # This test uses the fixture which creates an in-memory db
        # We need a file-based db for this test
        from src.database import get_connection, initialize_database

        db_path = tmp_path / "test_stats.duckdb"

        with get_connection(db_path) as conn:
            initialize_database(conn)
            conn.execute("INSERT INTO master_events (mdr_report_key, date_received) VALUES ('TEST001', '2024-01-15')")

        stats = get_table_statistics(db_path)

        assert isinstance(stats, dict)
        assert "master_events" in stats
        assert stats["master_events"]["row_count"] == 1
