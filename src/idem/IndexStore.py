from .models import FileMetadata
    def upsert_file_metadata(self, meta: FileMetadata) -> None:
        self.db.execute(
            sql=files.UPSERT_FILE_METEDATA,
            params=(
                meta.path,
                meta.dir_id,
                meta.size,
                meta.mtime_ns,
                meta.inode,
                meta.device,
                meta.hash_id,
            ),
        )
