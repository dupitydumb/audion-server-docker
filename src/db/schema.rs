use sqlx::sqlite::SqlitePool;
use tracing::info;

pub async fn init_db(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    info!("Initializing database schema...");

    // Enable foreign keys
    sqlx::query("PRAGMA foreign_keys = ON;").execute(pool).await?;

    // Create users table
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT DEFAULT 'User',
            listenbrainz_token TEXT,
            is_enabled INTEGER DEFAULT 1,
            subsonic_password TEXT,
            storage_quota_bytes INTEGER DEFAULT NULL,
            can_upload INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );"
    ).execute(pool).await?;

    // Try to alter users table for existing databases to add role, listenbrainz_token, is_enabled, subsonic_password, storage_quota_bytes, and can_upload
    let _ = sqlx::query("ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'User'").execute(pool).await;
    let _ = sqlx::query("ALTER TABLE users ADD COLUMN listenbrainz_token TEXT").execute(pool).await;
    let _ = sqlx::query("ALTER TABLE users ADD COLUMN is_enabled INTEGER DEFAULT 1").execute(pool).await;
    let _ = sqlx::query("ALTER TABLE users ADD COLUMN subsonic_password TEXT").execute(pool).await;
    let _ = sqlx::query("ALTER TABLE users ADD COLUMN storage_quota_bytes INTEGER DEFAULT NULL").execute(pool).await;
    let _ = sqlx::query("ALTER TABLE users ADD COLUMN can_upload INTEGER DEFAULT 1").execute(pool).await;

    // Alter tracks table for new metadata fields
    let _ = sqlx::query("ALTER TABLE tracks ADD COLUMN album_artist TEXT").execute(pool).await;
    let _ = sqlx::query("ALTER TABLE tracks ADD COLUMN composer TEXT").execute(pool).await;
    let _ = sqlx::query("ALTER TABLE tracks ADD COLUMN year TEXT").execute(pool).await;
    let _ = sqlx::query("ALTER TABLE tracks ADD COLUMN comment TEXT").execute(pool).await;
    let _ = sqlx::query("ALTER TABLE tracks ADD COLUMN bpm INTEGER").execute(pool).await;
    let _ = sqlx::query("ALTER TABLE tracks ADD COLUMN isrc TEXT").execute(pool).await;
    let _ = sqlx::query("ALTER TABLE tracks ADD COLUMN lyrics TEXT").execute(pool).await;

    // Alter albums table for new metadata fields
    let _ = sqlx::query("ALTER TABLE albums ADD COLUMN album_artist TEXT").execute(pool).await;
    let _ = sqlx::query("ALTER TABLE albums ADD COLUMN year TEXT").execute(pool).await;

    // Create albums table (matching client)
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS albums (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            artist TEXT,
            album_artist TEXT,
            year TEXT,
            art_data TEXT,
            art_path TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );"
    ).execute(pool).await?;

    // Create tracks table (matching client exactly + genre and size)
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT UNIQUE NOT NULL,
            title TEXT,
            artist TEXT,
            album TEXT,
            album_artist TEXT,
            composer TEXT,
            year TEXT,
            comment TEXT,
            bpm INTEGER,
            isrc TEXT,
            lyrics TEXT,
            track_number INTEGER,
            disc_number INTEGER,
            duration INTEGER,
            album_id INTEGER,
            format TEXT,
            bitrate INTEGER,
            source_type TEXT DEFAULT 'server',
            cover_url TEXT,
            external_id TEXT,
            content_hash TEXT,
            local_src TEXT,
            track_cover TEXT,
            track_cover_path TEXT,
            metadata_json TEXT,
            genre TEXT,
            size INTEGER,
            date_added TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (album_id) REFERENCES albums(id) ON DELETE CASCADE
        );"
    ).execute(pool).await?;

    // Create indexes on tracks
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_tracks_artist ON tracks(artist);").execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_tracks_album ON tracks(album);").execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_tracks_album_id ON tracks(album_id);").execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_tracks_content_hash ON tracks(content_hash);").execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_tracks_sort ON tracks(artist, album, track_number, title);").execute(pool).await?;

    // Create playlists table (owned by user)
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS playlists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            name TEXT NOT NULL,
            cover_url TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );"
    ).execute(pool).await?;

    // Create playlist tracks junction table
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS playlist_tracks (
            playlist_id INTEGER NOT NULL,
            track_id INTEGER NOT NULL,
            position INTEGER,
            PRIMARY KEY (playlist_id, track_id),
            FOREIGN KEY (playlist_id) REFERENCES playlists(id) ON DELETE CASCADE,
            FOREIGN KEY (track_id) REFERENCES tracks(id) ON DELETE CASCADE
        );"
    ).execute(pool).await?;

    // Create liked tracks table (per user)
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS liked_tracks (
            user_id TEXT NOT NULL,
            track_id INTEGER NOT NULL,
            liked_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, track_id),
            FOREIGN KEY (track_id) REFERENCES tracks(id) ON DELETE CASCADE
        );"
    ).execute(pool).await?;

    // Create events table for SSE streaming and sync replay
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );"
    ).execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at);").execute(pool).await?;

    // Create play_log table for server-side listening history
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS play_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            track_id INTEGER NOT NULL,
            timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
            duration_played INTEGER NOT NULL,
            client TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (track_id) REFERENCES tracks(id) ON DELETE CASCADE
        );"
    ).execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_play_log_user ON play_log(user_id);").execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_play_log_track ON play_log(track_id);").execute(pool).await?;

    // Create settings table for persistence of server configurations (e.g. tunnel config)
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );"
    ).execute(pool).await?;

    // Prune events older than 24 hours on startup
    if let Err(e) = sqlx::query("DELETE FROM events WHERE datetime(created_at) < datetime('now', '-24 hours')")
        .execute(pool)
        .await 
    {
        tracing::error!("Failed to prune old events: {}", e);
    } else {
        info!("Pruned database events older than 24 hours.");
    }

    info!("Database schema initialization complete.");
    Ok(())
}

