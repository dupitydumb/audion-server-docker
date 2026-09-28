use axum::{
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    body::Body,
};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use tokio_util::io::ReaderStream;
use std::path::PathBuf;

use sqlx::Row;
use crate::state::AppState;
use crate::auth::Claims;
use crate::events::ServerEvent;

fn parse_range(range_header: &str, file_size: u64) -> Option<(u64, u64)> {
    if !range_header.starts_with("bytes=") {
        return None;
    }
    let range_str = &range_header[6..];
    let parts: Vec<&str> = range_str.split('-').collect();
    if parts.len() != 2 {
        return None;
    }
    
    let start_str = parts[0].trim();
    let end_str = parts[1].trim();

    let start = if start_str.is_empty() {
        0
    } else {
        start_str.parse::<u64>().ok()?
    };

    let end = if end_str.is_empty() {
        file_size - 1
    } else {
        end_str.parse::<u64>().ok()?
    };

    if start > end || start >= file_size {
        return None;
    }

    let end = end.min(file_size - 1);
    Some((start, end))
}

fn mime_for_format(format: Option<&str>) -> &'static str {
    match format {
        Some(f) => match f.to_lowercase().as_str() {
            "mp3" => "audio/mpeg",
            "flac" => "audio/flac",
            "alac" | "m4a" | "aac" => "audio/mp4",
            "ogg" | "vorbis" => "audio/ogg",
            "wav" => "audio/wav",
            _ => "audio/mpeg",
        },
        None => "audio/mpeg",
    }
}

pub async fn stream_track(
    _claims: Claims,
    headers: HeaderMap,
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Response {
    let user_pool = match state.get_user_pool(&_claims.sub).await {
        Ok(p) => p,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let (path, format) = match sqlx::query("SELECT path, format FROM tracks WHERE id = ?")
        .bind(id)
        .fetch_optional(&user_pool)
        .await
    {
        Ok(Some(row)) => {
            let path: String = row.get("path");
            let format: Option<String> = row.get("format");
            (path, format)
        }
        _ => return StatusCode::NOT_FOUND.into_response(),
    };

    let storage = state.storage_backend.read().await;
    let is_remote = matches!(&*storage, crate::storage::StorageBackend::S3 { .. } | crate::storage::StorageBackend::Azure { .. });
    let is_s3 = is_remote;
    let needs_transcode = format.as_deref().map(|f| f.to_lowercase()) == Some("flac".to_string()) && state.has_ffmpeg;

    if is_remote && !needs_transcode {
        // Try presigned redirect first (zero-copy, fastest path)
        if let Ok(url) = storage.get_presigned_url(&path, 3600).await {
            return Response::builder()
                .status(StatusCode::FOUND)
                .header(header::LOCATION, url)
                .body(Body::empty())
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
        }
        // Presign failed (e.g. Azure, or S3 error) — fall back to proxying bytes through server
        match storage.get_object(&path).await {
            Ok(bytes) => {
                let file_size = bytes.len() as u64;
                let mime = mime_for_format(format.as_deref());
                return Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_LENGTH, file_size)
                    .header(header::ACCEPT_RANGES, "none")
                    .header(header::CONTENT_TYPE, mime)
                    .body(Body::from(bytes))
                    .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
            }
            Err(e) => {
                tracing::error!("Failed to fetch remote object '{}': {}", path, e);
                return StatusCode::NOT_FOUND.into_response();
            }
        }
    }

    let mut stream_path = state.config.data_dir.join(&path);
    let mut mime_type = mime_for_format(format.as_deref());

    if needs_transcode {
        let cache_dir = state.config.data_dir.join("transcoded");
        if std::fs::create_dir_all(&cache_dir).is_ok() {
            let cache_path = cache_dir.join(format!("{}.mp3", id));
            let mut transcode_success = cache_path.exists();

            if !transcode_success {
                tracing::info!("Transcoding FLAC track {} to MP3 on-demand...", id);

                // Broadcast that transcoding has started
                state.event_bus.broadcast(ServerEvent {
                    id: 0,
                    event_type: "track.transcoding".to_string(),
                    payload: serde_json::json!({ "id": id, "status": "started" }),
                    created_at: chrono::Utc::now().to_rfc3339(),
                });

                let temp_path = cache_dir.join(format!("{}.temp.mp3", id));
                let temp_s3_input = cache_dir.join(format!("{}.s3_temp.flac", id));
                
                let mut prepared = true;
                if is_s3 {
                    match storage.get_object(&path).await {
                        Ok(bytes) => {
                            if let Err(e) = std::fs::write(&temp_s3_input, bytes) {
                                tracing::error!("Failed to write S3 temp file: {}", e);
                                prepared = false;
                            }
                        }
                        Err(e) => {
                            tracing::error!("Failed to download FLAC from S3: {}", e);
                            prepared = false;
                        }
                    }
                }

                let rename_success = if prepared {
                    let input_file = if is_s3 { &temp_s3_input } else { &stream_path };
                    let status = tokio::process::Command::new("ffmpeg")
                        .args(&[
                            "-y",
                            "-i", &input_file.to_string_lossy(),
                            "-codec:a", "libmp3lame",
                            "-b:a", "320k",
                            &temp_path.to_string_lossy(),
                        ])
                        .stdout(std::process::Stdio::null())
                        .stderr(std::process::Stdio::null())
                        .status()
                        .await;

                    match status {
                        Ok(s) if s.success() => {
                            if tokio::fs::rename(&temp_path, &cache_path).await.is_ok() {
                                tracing::info!("Transcoding of track {} complete.", id);
                                transcode_success = true;
                                true
                            } else {
                                tracing::error!("Failed to rename temp transcoded file for track {}", id);
                                let _ = tokio::fs::remove_file(&temp_path).await;
                                false
                            }
                        }
                        _ => {
                            tracing::error!("FFmpeg transcoding failed for track {}", id);
                            let _ = tokio::fs::remove_file(&temp_path).await;
                            false
                        }
                    }
                } else {
                    false
                };

                if is_s3 && temp_s3_input.exists() {
                    let _ = std::fs::remove_file(&temp_s3_input);
                }

                // Broadcast final transcoding status
                let final_status = if rename_success { "complete" } else { "failed" };
                state.event_bus.broadcast(ServerEvent {
                    id: 0,
                    event_type: "track.transcoding".to_string(),
                    payload: serde_json::json!({ "id": id, "status": final_status }),
                    created_at: chrono::Utc::now().to_rfc3339(),
                });
            }

            if transcode_success {
                stream_path = cache_path;
                mime_type = "audio/mpeg";
            }
        }
    }

    let mut file = match File::open(&stream_path).await {
        Ok(f) => f,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };

    let file_size = match file.metadata().await {
        Ok(m) => m.len(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    // Check for Range header
    let range = headers.get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| parse_range(s, file_size));

    match range {
        Some((start, end)) => {
            let len = end - start + 1;
            if file.seek(SeekFrom::Start(start)).await.is_err() {
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }

            let stream = ReaderStream::new(file.take(len));
            let body = Body::from_stream(stream);

            Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(header::CONTENT_RANGE, format!("bytes {}-{}/{}", start, end, file_size))
                .header(header::CONTENT_LENGTH, len)
                .header(header::ACCEPT_RANGES, "bytes")
                .header(header::CONTENT_TYPE, mime_type)
                .body(body)
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
        None => {
            let stream = ReaderStream::new(file);
            let body = Body::from_stream(stream);

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_LENGTH, file_size)
                .header(header::ACCEPT_RANGES, "bytes")
                .header(header::CONTENT_TYPE, mime_type)
                .body(body)
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
    }
}

pub async fn get_track_cover(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    let cover_path = state.find_track_cover_path_for_user(&claims.sub, id).await;

    let cover_path = match cover_path {
        Some(path) => Some(path),
        None => state.find_track_cover_path(id).await,
    };

    if let Some(ref path) = cover_path {
        let storage = state.storage_backend.read().await;
        match &*storage {
            crate::storage::StorageBackend::Local { .. } => {
                let full_path = state.config.data_dir.join(path);
                if full_path.exists() {
                    if let Ok(bytes) = std::fs::read(&full_path) {
                        let mime = mime_guess::from_path(&full_path).first_or_octet_stream();
                        return (
                            StatusCode::OK,
                            [(header::CONTENT_TYPE, mime.to_string())],
                            bytes,
                        ).into_response();
                    }
                }
            }
            crate::storage::StorageBackend::S3 { .. } => {
                match storage.get_presigned_url(path, 3600).await {
                    Ok(url) => {
                        return Response::builder()
                            .status(StatusCode::FOUND)
                            .header(header::LOCATION, url)
                            .body(Body::empty())
                            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
                            .into_response();
                    }
                    Err(e) => {
                        tracing::error!("Failed to generate cover presigned URL: {}", e);
                    }
                }
            }
            crate::storage::StorageBackend::Azure { .. } => {
                // Azure: download and serve directly (no SAS URL support)
                match storage.get_object(path).await {
                    Ok(bytes) => {
                        let mime = mime_guess::from_path(path).first_or(mime_guess::mime::IMAGE_JPEG);
                        return (
                            StatusCode::OK,
                            [(header::CONTENT_TYPE, mime.to_string())],
                            bytes,
                        ).into_response();
                    }
                    Err(e) => {
                        tracing::error!("Failed to fetch Azure cover: {}", e);
                    }
                }
            }
        }
    }

    StatusCode::NOT_FOUND.into_response()
}

pub async fn stream_track_subsonic(
    _claims: Claims,
    headers: HeaderMap,
    state: &AppState,
    id: i64,
    max_bitrate: Option<i32>,
    target_format: Option<&str>,
) -> Response {
    let user_pool = match state.get_user_pool(&_claims.sub).await {
        Ok(p) => p,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let (path, format) = match sqlx::query("SELECT path, format FROM tracks WHERE id = ?")
        .bind(id)
        .fetch_optional(&user_pool)
        .await
    {
        Ok(Some(row)) => {
            let path: String = row.get("path");
            let format: Option<String> = row.get("format");
            (path, format)
        }
        _ => return StatusCode::NOT_FOUND.into_response(),
    };

    let storage = state.storage_backend.read().await;
    let is_s3 = matches!(&*storage, crate::storage::StorageBackend::S3 { .. } | crate::storage::StorageBackend::Azure { .. });

    // "raw" means serve original file without transcoding
    let want_raw = target_format.map(|f| f.eq_ignore_ascii_case("raw")).unwrap_or(false);

    // Determine if transcoding is requested
    let mut needs_transcode = false;
    let mut codec_name = "libmp3lame";
    let mut mux_format = "mp3";
    let mut mime_type = "audio/mpeg";

    let req_format = target_format.map(|f| f.to_lowercase());
    let current_format = format.as_deref().map(|f| f.to_lowercase());

    if !want_raw {
        if let Some(ref target) = req_format {
            if current_format.as_ref() != Some(target) {
                needs_transcode = true;
                match target.as_str() {
                    "opus" | "ogg" => {
                        codec_name = "libopus";
                        mux_format = "ogg";
                        mime_type = "audio/ogg";
                    }
                    "aac" | "m4a" => {
                        codec_name = "aac";
                        mux_format = "adts";
                        mime_type = "audio/aac";
                    }
                    _ => {
                        codec_name = "libmp3lame";
                        mux_format = "mp3";
                        mime_type = "audio/mpeg";
                    }
                }
            }
        }

        let requested_bitrate_bps = max_bitrate.unwrap_or(0) * 1000;
        if requested_bitrate_bps > 0 {
            needs_transcode = true;
        } else if current_format == Some("flac".to_string()) || current_format == Some("alac".to_string()) {
            needs_transcode = true;
        }
    }

    if needs_transcode && state.has_ffmpeg {
        let bitrate_str = if requested_bitrate_bps > 0 {
            format!("{}k", max_bitrate.unwrap())
        } else {
            "320k".to_string()
        };

        tracing::info!("On-the-fly Subsonic transcoding track {} using ffmpeg with target bitrate {} and format {}...", id, bitrate_str, mux_format);

        let temp_input = state.config.data_dir.join(format!("transcode_temp_{}.tmp", uuid::Uuid::new_v4()));
        let mut prepared = true;

        if is_s3 {
            match storage.get_object(&path).await {
                Ok(bytes) => {
                    if let Err(e) = std::fs::write(&temp_input, bytes) {
                        tracing::error!("Failed to write subsonic S3 temp file: {}", e);
                        prepared = false;
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to fetch subsonic S3 file: {}", e);
                    prepared = false;
                }
            }
        }

        if prepared {
            let input_file = if is_s3 { temp_input.clone() } else { state.config.data_dir.join(&path) };
            let status = tokio::process::Command::new("ffmpeg")
                .args(&[
                    "-y",
                    "-i", &input_file.to_string_lossy(),
                    "-codec:a", codec_name,
                    "-b:a", &bitrate_str,
                    "-f", mux_format,
                    "pipe:1",
                ])
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::null())
                .spawn();

            match status {
                Ok(mut child) => {
                    let stdout = child.stdout.take();
                    if is_s3 {
                        tokio::spawn(async move {
                            let _ = child.wait().await;
                            let _ = tokio::fs::remove_file(temp_input).await;
                        });
                    }
                    if let Some(stdout) = stdout {
                        let stream = ReaderStream::new(stdout);
                        let body = Body::from_stream(stream);
                        return Response::builder()
                            .status(StatusCode::OK)
                            .header(header::CONTENT_TYPE, mime_type)
                            .header(header::ACCEPT_RANGES, "none")
                            .body(body)
                            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to spawn ffmpeg: {}", e);
                    if is_s3 {
                        let _ = std::fs::remove_file(temp_input);
                    }
                }
            }
        }
    }

    if is_s3 {
        // Direct stream fallback from S3 (download into memory)
        match storage.get_object(&path).await {
            Ok(bytes) => {
                let file_size = bytes.len() as u64;
                let mime_type = mime_for_format(format.as_deref());
                let body = Body::from(bytes);

                Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_LENGTH, file_size)
                    .header(header::ACCEPT_RANGES, "none")
                    .header(header::CONTENT_TYPE, mime_type)
                    .body(body)
                    .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
            }
            Err(_) => StatusCode::NOT_FOUND.into_response(),
        }
    } else {
        // Direct stream fallback local — supports Range requests
        let stream_path = state.config.data_dir.join(&path);
        let mut file = match File::open(&stream_path).await {
            Ok(f) => f,
            Err(_) => return StatusCode::NOT_FOUND.into_response(),
        };

        let file_size = match file.metadata().await {
            Ok(m) => m.len(),
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };

        let mime_type = mime_for_format(format.as_deref());

        let range = headers.get(header::RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| parse_range(s, file_size));

        match range {
            Some((start, end)) => {
                let len = end - start + 1;
                if file.seek(SeekFrom::Start(start)).await.is_err() {
                    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
                let stream = ReaderStream::new(file.take(len));
                let body = Body::from_stream(stream);
                Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header(header::CONTENT_RANGE, format!("bytes {}-{}/{}", start, end, file_size))
                    .header(header::CONTENT_LENGTH, len)
                    .header(header::ACCEPT_RANGES, "bytes")
                    .header(header::CONTENT_TYPE, mime_type)
                    .body(body)
                    .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
            }
            None => {
                let stream = ReaderStream::new(file);
                let body = Body::from_stream(stream);
                Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_LENGTH, file_size)
                    .header(header::ACCEPT_RANGES, "bytes")
                    .header(header::CONTENT_TYPE, mime_type)
                    .body(body)
                    .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
            }
        }
    }
}
