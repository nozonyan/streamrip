import requests
import asyncio
import logging
import os
from dataclasses import dataclass
import json

FAILED_LOG = "failed_booklets.json"

from .. import progress
from ..client import Client
from ..config import Config
from ..db import Database
from ..exceptions import NonStreamableError
from ..filepath_utils import clean_filepath
from ..metadata import AlbumMetadata
from ..metadata.util import get_album_track_ids
from .artwork import download_artwork
from .media import Media, Pending
from .track import PendingTrack
from pypdf import PdfReader

logger = logging.getLogger("streamrip")


def validate_pdf(filepath: str) -> bool:
    try:
        with open(filepath, "rb") as pdf_file:
            pdf_reader = PdfReader(pdf_file)
            if len(pdf_reader.pages) > 0:
                return True
            return False
    except Exception:
        return False


def is_xml_response(filepath: str) -> bool:
    try:
        with open(filepath, "rb") as f:
            start = f.read(100).lstrip()
            return start.startswith(b"<?xml")
    except Exception:
        return False


@dataclass(slots=True)
class Album(Media):
    meta: AlbumMetadata
    tracks: list[PendingTrack]
    config: Config
    # folder where the tracks will be downloaded
    folder: str
    db: Database

    def _register_failed_booklet(self, url: str):
        data = []

        if os.path.exists(FAILED_LOG):
            with open(FAILED_LOG, "r") as f:
                data = json.load(f)

        data.append(
            {
                "album": self.meta.album,
                # "album_id": self.meta.id,
                "url": url,
            }
        )

        with open(FAILED_LOG, "w") as f:
            json.dump(data, f, indent=2)

    async def preprocess(self):
        progress.add_title(self.meta.album)

    async def download(self):
        async def _resolve_and_download(pending: Pending):
            try:
                track = await pending.resolve()
                if track is None:
                    return
                await track.rip()
            except Exception as e:
                logger.error(f"Error downloading track: {e}")

        if self.config.session.qobuz.download_booklets:
            booklet = None
            booklet_path = None

            try:
                booklet = self.meta.info.booklets[0]["url"]
                booklet_filename = booklet.rsplit("/")[-1]
                booklet_path = os.path.join(self.folder, f"booklet_{booklet_filename}")
            except (IndexError, KeyError, TypeError):
                booklet = None

            if booklet and booklet_path and not os.path.isfile(booklet_path):
                logger.info("Downloading booklet")

                try:
                    r = requests.get(booklet, timeout=15)

                    if r.status_code != 200:
                        logger.error(f"Failed to download booklet ({r.status_code})")
                        self._register_failed_booklet(booklet)
                        return

                    content_type = r.headers.get("Content-Type", "").lower()

                    # Reject non-PDF responses early
                    if "pdf" not in content_type:
                        logger.error(f"Server returned non-PDF content: {content_type}")
                        self._register_failed_booklet(booklet)
                        return

                    with open(booklet_path, "wb") as f:
                        f.write(r.content)

                    # Validate PDF structure
                    if is_xml_response(booklet_path) or not validate_pdf(booklet_path):
                        logger.error(f"Invalid booklet PDF: {booklet_path}")
                        os.remove(booklet_path)
                        self._register_failed_booklet(booklet)

                except requests.RequestException as e:
                    logger.error(f"Booklet request failed: {e}")
                    self._register_failed_booklet(booklet)

        else:
            logger.debug("Booklet present but download disabled by config")
            ##

        results = await asyncio.gather(
            *[_resolve_and_download(p) for p in self.tracks], return_exceptions=True
        )

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Album track processing error: {result}")

    async def postprocess(self, failed: bool = False):
        progress.remove_title(self.meta.album)


@dataclass(slots=True)
class PendingAlbum(Pending):
    id: str
    client: Client
    config: Config
    db: Database

    async def resolve(self) -> Album | None:
        try:
            resp = await self.client.get_metadata(self.id, "album")
        except NonStreamableError as e:
            logger.error(
                f"Album {self.id} not available to stream on {self.client.source} ({e})",
            )
            return None

        try:
            meta = AlbumMetadata.from_album_resp(resp, self.client.source)
        except Exception as e:
            logger.error(f"Error building album metadata for {id=}: {e}")
            return None

        if meta is None:
            logger.error(
                f"Album {self.id} not available to stream on {self.client.source}",
            )
            return None

        tracklist = get_album_track_ids(self.client.source, resp)
        folder = self.config.session.downloads.folder
        album_folder = self._album_folder(folder, meta)
        os.makedirs(album_folder, exist_ok=True)
        embed_cover, _ = await download_artwork(
            self.client.session,
            album_folder,
            meta.covers,
            self.config.session.artwork,
            for_playlist=False,
        )
        pending_tracks = [
            PendingTrack(
                id,
                album=meta,
                client=self.client,
                config=self.config,
                folder=album_folder,
                db=self.db,
                cover_path=embed_cover,
            )
            for id in tracklist
        ]
        logger.debug("Pending tracks: %s", pending_tracks)
        return Album(meta, pending_tracks, self.config, album_folder, self.db)

    def _album_folder(self, parent: str, meta: AlbumMetadata) -> str:
        config = self.config.session
        if config.downloads.source_subdirectories:
            parent = os.path.join(parent, self.client.source.capitalize())
        formatter = config.filepaths.folder_format
        folder = clean_filepath(
            meta.format_folder_path(formatter), config.filepaths.restrict_characters
        )

        return os.path.join(parent, folder)
