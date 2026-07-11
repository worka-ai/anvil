use super::{FormatError, Hash32, hash32};
use std::convert::TryInto;

pub const WRITER_BODY_TABLE_DIRECTORY_MAGIC: &[u8; 8] = b"ANTDIR1\0";
pub const WRITER_BODY_TABLE_DIRECTORY_VERSION: u16 = 1;
pub const TABLE_PAGE_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRow {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterBodyTable {
    pub table_id: u16,
    pub row_type_id: u16,
    pub rows: Vec<TableRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedWriterBodyTable {
    pub table_id: u16,
    pub row_type_id: u16,
    pub table_hash: Hash32,
    pub rows: Vec<TableRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterBodyTableDirectory {
    pub directory_len: usize,
    pub entries: Vec<WriterBodyTableDirectoryEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterBodyTableDirectoryEntry {
    pub table_id: u16,
    pub row_type_id: u16,
    pub offset: u64,
    pub length: u64,
    pub page_count: u32,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub table_hash: Hash32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePageRange {
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub page_offset: u64,
    pub page_length: u64,
    pub row_count: u32,
    pub page_hash: Hash32,
}

#[derive(Debug, Clone)]
struct EncodedTableBody {
    table_id: u16,
    row_type_id: u16,
    offset: u64,
    bytes: Vec<u8>,
    page_count: u32,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    table_hash: Hash32,
}

pub fn table_page_magic(table_id: u16, version: u16) -> [u8; 8] {
    let mut magic = [0u8; 8];
    magic[0..4].copy_from_slice(b"ANTP");
    magic[4..6].copy_from_slice(&table_id.to_be_bytes());
    magic[6..8].copy_from_slice(&version.to_be_bytes());
    magic
}

pub fn encode_writer_body_tables(tables: &[WriterBodyTable]) -> Result<Vec<u8>, FormatError> {
    if tables.is_empty() {
        return Ok(Vec::new());
    }
    let mut tables = tables.to_vec();
    tables.sort_by_key(|table| table.table_id);
    for pair in tables.windows(2) {
        if pair[0].table_id == pair[1].table_id {
            return Err(FormatError::InvalidHeaderField { field: "table_id" });
        }
    }

    let mut encoded = tables
        .iter()
        .map(|table| encode_table_body(table, 1024))
        .collect::<Result<Vec<_>, _>>()?;
    let directory_len = encoded_directory_len(&encoded)?;
    let mut cursor = directory_len as u64;
    for table in &mut encoded {
        table.offset = cursor;
        cursor = cursor.checked_add(table.bytes.len() as u64).ok_or(
            FormatError::InvalidDeclaredLength {
                context: "writer body table offset",
            },
        )?;
    }

    let mut out = encode_table_directory(&encoded)?;
    for table in encoded {
        out.extend_from_slice(&table.bytes);
    }
    Ok(out)
}

pub fn decode_writer_body_tables(body: &[u8]) -> Result<Vec<DecodedWriterBodyTable>, FormatError> {
    if body.is_empty() {
        return Ok(Vec::new());
    }
    let (entries, directory_len) = decode_table_directory(body)?;
    let mut previous_end = directory_len as u64;
    let mut tables = Vec::with_capacity(entries.len());
    for entry in entries {
        if entry.offset < previous_end {
            return Err(FormatError::InvalidRangeIndexOrder);
        }
        let end =
            entry
                .offset
                .checked_add(entry.length)
                .ok_or(FormatError::InvalidDeclaredLength {
                    context: "writer body table length",
                })?;
        if end > body.len() as u64 {
            return Err(FormatError::InvalidDeclaredLength {
                context: "writer body table range",
            });
        }
        previous_end = end;
        let table_bytes = &body[entry.offset as usize..end as usize];
        if hash32(table_bytes) != entry.table_hash {
            return Err(FormatError::HashMismatch {
                context: "writer body table",
            });
        }
        let rows = decode_table_body(
            entry.table_id,
            entry.row_type_id,
            table_bytes,
            entry.page_count,
        )?;
        tables.push(DecodedWriterBodyTable {
            table_id: entry.table_id,
            row_type_id: entry.row_type_id,
            table_hash: entry.table_hash,
            rows,
        });
    }
    Ok(tables)
}

pub fn decode_writer_body_table_directory(
    body_prefix: &[u8],
) -> Result<(WriterBodyTableDirectory, usize), FormatError> {
    let (entries, directory_len) = decode_table_directory(body_prefix)?;
    let entries = entries
        .into_iter()
        .map(WriterBodyTableDirectoryEntry::from)
        .collect::<Vec<_>>();
    Ok((
        WriterBodyTableDirectory {
            directory_len,
            entries,
        },
        directory_len,
    ))
}

pub fn decode_writer_body_table(
    entry: &WriterBodyTableDirectoryEntry,
    table_bytes: &[u8],
) -> Result<DecodedWriterBodyTable, FormatError> {
    if table_bytes.len() as u64 != entry.length {
        return Err(FormatError::InvalidDeclaredLength {
            context: "writer body table range",
        });
    }
    if hash32(table_bytes) != entry.table_hash {
        return Err(FormatError::HashMismatch {
            context: "writer body table",
        });
    }
    let rows = decode_table_body(
        entry.table_id,
        entry.row_type_id,
        table_bytes,
        entry.page_count,
    )?;
    Ok(DecodedWriterBodyTable {
        table_id: entry.table_id,
        row_type_id: entry.row_type_id,
        table_hash: entry.table_hash,
        rows,
    })
}

pub fn decode_writer_body_table_page_ranges(
    entry: &WriterBodyTableDirectoryEntry,
    table_prefix: &[u8],
) -> Result<Vec<TablePageRange>, FormatError> {
    decode_table_page_directory(
        entry.table_id,
        entry.row_type_id,
        table_prefix,
        entry.page_count,
    )
}

pub fn decode_table_page_rows(
    entry: &WriterBodyTableDirectoryEntry,
    page: &TablePageRange,
    page_bytes: &[u8],
) -> Result<Vec<TableRow>, FormatError> {
    if page_bytes.len() as u64 != page.page_length {
        return Err(FormatError::InvalidDeclaredLength {
            context: "table page range",
        });
    }
    if table_page_payload_hash(page_bytes)? != page.page_hash {
        return Err(FormatError::HashMismatch {
            context: "table page",
        });
    }
    decode_table_page(
        entry.table_id,
        entry.row_type_id,
        page_bytes,
        page.row_count,
        &page.min_key,
        &page.max_key,
    )
}

fn encode_table_body(
    table: &WriterBodyTable,
    max_rows_per_page: usize,
) -> Result<EncodedTableBody, FormatError> {
    if table.row_type_id != table.table_id {
        return Err(FormatError::InvalidHeaderField {
            field: "row_type_id",
        });
    }
    ensure_rows_sorted(&table.rows)?;
    let mut pages = Vec::new();
    for chunk in table.rows.chunks(max_rows_per_page.max(1)) {
        pages.push(encode_table_page(table.table_id, table.row_type_id, chunk)?);
    }

    let mut page_directory_len = 4usize;
    for page in &pages {
        page_directory_len = page_directory_len
            .checked_add(page_directory_entry_len(page)?)
            .ok_or(FormatError::InvalidDeclaredLength {
                context: "table page directory",
            })?;
    }

    let mut page_offset = page_directory_len as u64;
    let mut out = Vec::new();
    out.extend_from_slice(&(pages.len() as u32).to_le_bytes());
    for page in &pages {
        push_len_u16(&mut out, &page.min_key, "table page min_key")?;
        push_len_u16(&mut out, &page.max_key, "table page max_key")?;
        out.extend_from_slice(&page_offset.to_le_bytes());
        out.extend_from_slice(&(page.bytes.len() as u64).to_le_bytes());
        out.extend_from_slice(&page.row_count.to_le_bytes());
        out.extend_from_slice(&page.page_hash);
        page_offset = page_offset.checked_add(page.bytes.len() as u64).ok_or(
            FormatError::InvalidDeclaredLength {
                context: "table page offset",
            },
        )?;
    }
    for page in &pages {
        out.extend_from_slice(&page.bytes);
    }
    let crc = crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    let table_hash = hash32(&out);
    let min_key = table
        .rows
        .first()
        .map(|row| row.key.clone())
        .unwrap_or_default();
    let max_key = table
        .rows
        .last()
        .map(|row| row.key.clone())
        .unwrap_or_default();
    Ok(EncodedTableBody {
        table_id: table.table_id,
        row_type_id: table.row_type_id,
        offset: 0,
        bytes: out,
        page_count: pages.len() as u32,
        min_key,
        max_key,
        table_hash,
    })
}

#[derive(Debug, Clone)]
struct EncodedPage {
    bytes: Vec<u8>,
    row_count: u32,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    page_hash: Hash32,
}

fn encode_table_page(
    table_id: u16,
    key_schema_id: u16,
    rows: &[TableRow],
) -> Result<EncodedPage, FormatError> {
    ensure_rows_sorted(rows)?;
    let min_key = rows.first().map(|row| row.key.clone()).unwrap_or_default();
    let max_key = rows.last().map(|row| row.key.clone()).unwrap_or_default();
    let mut row_bytes = Vec::new();
    let mut row_offsets = Vec::with_capacity(rows.len());
    for row in rows {
        row_offsets.push(row_bytes.len() as u32);
        encode_table_row(row, &mut row_bytes)?;
    }

    let mut out = Vec::new();
    out.extend_from_slice(&table_page_magic(table_id, TABLE_PAGE_VERSION));
    out.extend_from_slice(&TABLE_PAGE_VERSION.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&key_schema_id.to_le_bytes());
    out.extend_from_slice(&(rows.len() as u32).to_le_bytes());
    push_len_u16(&mut out, &min_key, "table page min_key")?;
    push_len_u16(&mut out, &max_key, "table page max_key")?;
    for offset in row_offsets {
        out.extend_from_slice(&offset.to_le_bytes());
    }
    out.extend_from_slice(&row_bytes);
    let crc = crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    let page_hash = hash32(&out);
    out.extend_from_slice(&page_hash);
    Ok(EncodedPage {
        bytes: out,
        row_count: rows.len() as u32,
        min_key,
        max_key,
        page_hash,
    })
}

fn decode_table_page_directory(
    _table_id: u16,
    _row_type_id: u16,
    input: &[u8],
    expected_pages: u32,
) -> Result<Vec<TablePageRange>, FormatError> {
    let mut offset = 0usize;
    let page_count = read_u32(input, &mut offset)?;
    if page_count != expected_pages {
        return Err(FormatError::InvalidHeaderField {
            field: "page_count",
        });
    }
    let mut pages = Vec::with_capacity(page_count as usize);
    for _ in 0..page_count {
        let min_key = read_len_u16(input, &mut offset, "table page min_key")?.to_vec();
        let max_key = read_len_u16(input, &mut offset, "table page max_key")?.to_vec();
        let page_offset = read_u64(input, &mut offset)?;
        let page_length = read_u64(input, &mut offset)?;
        let row_count = read_u32(input, &mut offset)?;
        let page_hash = read_hash(input, &mut offset)?;
        pages.push(TablePageRange {
            min_key,
            max_key,
            page_offset,
            page_length,
            row_count,
            page_hash,
        });
    }
    Ok(pages)
}

fn decode_table_body(
    table_id: u16,
    row_type_id: u16,
    input: &[u8],
    expected_pages: u32,
) -> Result<Vec<TableRow>, FormatError> {
    let mut offset = 0usize;
    let page_count = read_u32(input, &mut offset)?;
    if page_count != expected_pages {
        return Err(FormatError::InvalidHeaderField {
            field: "page_count",
        });
    }
    let mut pages = Vec::with_capacity(page_count as usize);
    for _ in 0..page_count {
        let min_key = read_len_u16(input, &mut offset, "table page min_key")?.to_vec();
        let max_key = read_len_u16(input, &mut offset, "table page max_key")?.to_vec();
        let page_offset = read_u64(input, &mut offset)?;
        let page_length = read_u64(input, &mut offset)?;
        let row_count = read_u32(input, &mut offset)?;
        let page_hash = read_hash(input, &mut offset)?;
        pages.push((
            min_key,
            max_key,
            page_offset,
            page_length,
            row_count,
            page_hash,
        ));
    }
    let crc_end = input.len().checked_sub(4).ok_or(FormatError::TooShort {
        context: "table body crc",
        needed: 4,
        actual: input.len(),
    })?;
    let expected_crc = u32::from_le_bytes(input[crc_end..].try_into().unwrap());
    if crc32c(&input[..crc_end]) != expected_crc {
        return Err(FormatError::HashMismatch {
            context: "table body crc32c",
        });
    }

    let mut rows = Vec::new();
    let mut previous_page_end = offset as u64;
    for (min_key, max_key, page_offset, page_length, row_count, page_hash) in pages {
        if page_offset < previous_page_end {
            return Err(FormatError::InvalidRangeIndexOrder);
        }
        let page_end =
            page_offset
                .checked_add(page_length)
                .ok_or(FormatError::InvalidDeclaredLength {
                    context: "table page range",
                })?;
        if page_end > crc_end as u64 {
            return Err(FormatError::InvalidDeclaredLength {
                context: "table page range",
            });
        }
        previous_page_end = page_end;
        let page_bytes = &input[page_offset as usize..page_end as usize];
        if table_page_payload_hash(page_bytes)? != page_hash {
            return Err(FormatError::HashMismatch {
                context: "table page",
            });
        }
        rows.extend(decode_table_page(
            table_id,
            row_type_id,
            page_bytes,
            row_count,
            &min_key,
            &max_key,
        )?);
    }
    ensure_rows_sorted(&rows)?;
    Ok(rows)
}

fn table_page_payload_hash(input: &[u8]) -> Result<Hash32, FormatError> {
    let hash_start = input.len().checked_sub(32).ok_or(FormatError::TooShort {
        context: "table page hash",
        needed: 32,
        actual: input.len(),
    })?;
    Ok(hash32(&input[..hash_start]))
}

fn decode_table_page(
    table_id: u16,
    key_schema_id: u16,
    input: &[u8],
    expected_rows: u32,
    expected_min_key: &[u8],
    expected_max_key: &[u8],
) -> Result<Vec<TableRow>, FormatError> {
    let hash_start = input.len().checked_sub(32).ok_or(FormatError::TooShort {
        context: "table page hash",
        needed: 32,
        actual: input.len(),
    })?;
    if hash32(&input[..hash_start]).as_slice() != &input[hash_start..] {
        return Err(FormatError::HashMismatch {
            context: "table page hash",
        });
    }
    let crc_start = hash_start.checked_sub(4).ok_or(FormatError::TooShort {
        context: "table page crc",
        needed: 4,
        actual: input.len(),
    })?;
    let expected_crc = u32::from_le_bytes(input[crc_start..hash_start].try_into().unwrap());
    if crc32c(&input[..crc_start]) != expected_crc {
        return Err(FormatError::HashMismatch {
            context: "table page crc32c",
        });
    }

    let mut offset = 0usize;
    let magic = read_exact(input, &mut offset, 8)?;
    if magic != table_page_magic(table_id, TABLE_PAGE_VERSION).as_slice() {
        return Err(FormatError::InvalidMagic {
            context: "table page",
        });
    }
    let version = read_u16(input, &mut offset)?;
    if version != TABLE_PAGE_VERSION {
        return Err(FormatError::UnsupportedMajorVersion(version));
    }
    let _flags = read_u16(input, &mut offset)?;
    let actual_key_schema_id = read_u16(input, &mut offset)?;
    if actual_key_schema_id != key_schema_id {
        return Err(FormatError::InvalidHeaderField {
            field: "key_schema_id",
        });
    }
    let row_count = read_u32(input, &mut offset)?;
    if row_count != expected_rows {
        return Err(FormatError::InvalidHeaderField { field: "row_count" });
    }
    let min_key = read_len_u16(input, &mut offset, "table page min_key")?;
    let max_key = read_len_u16(input, &mut offset, "table page max_key")?;
    if min_key != expected_min_key || max_key != expected_max_key {
        return Err(FormatError::InvalidHeaderField {
            field: "page key bounds",
        });
    }
    let mut row_offsets = Vec::with_capacity(row_count as usize);
    for _ in 0..row_count {
        row_offsets.push(read_u32(input, &mut offset)? as usize);
    }
    let row_region = &input[offset..crc_start];
    let mut rows = Vec::with_capacity(row_count as usize);
    for idx in 0..row_offsets.len() {
        let start = row_offsets[idx];
        let end = row_offsets
            .get(idx + 1)
            .copied()
            .unwrap_or(row_region.len());
        if start > end || end > row_region.len() {
            return Err(FormatError::InvalidDeclaredLength {
                context: "table row offset",
            });
        }
        rows.push(decode_table_row(&row_region[start..end])?);
    }
    ensure_rows_sorted(&rows)?;
    Ok(rows)
}

fn encode_table_row(row: &TableRow, out: &mut Vec<u8>) -> Result<(), FormatError> {
    let start = out.len();
    write_uleb(out, row.key.len() as u64);
    out.extend_from_slice(&row.key);
    write_uleb(out, row.value.len() as u64);
    out.extend_from_slice(&row.value);
    let crc = crc32c(&out[start..]);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok(())
}

fn decode_table_row(input: &[u8]) -> Result<TableRow, FormatError> {
    let crc_start = input.len().checked_sub(4).ok_or(FormatError::TooShort {
        context: "table row crc",
        needed: 4,
        actual: input.len(),
    })?;
    let expected_crc = u32::from_le_bytes(input[crc_start..].try_into().unwrap());
    if crc32c(&input[..crc_start]) != expected_crc {
        return Err(FormatError::HashMismatch {
            context: "table row crc32c",
        });
    }
    let mut offset = 0usize;
    let key_len = read_uleb(input, &mut offset)? as usize;
    let key = read_exact(input, &mut offset, key_len)?.to_vec();
    let value_len = read_uleb(input, &mut offset)? as usize;
    let value = read_exact(input, &mut offset, value_len)?.to_vec();
    if offset != crc_start {
        return Err(FormatError::InvalidDeclaredLength {
            context: "table row",
        });
    }
    Ok(TableRow { key, value })
}

#[derive(Debug, Clone)]
struct DirectoryEntry {
    table_id: u16,
    row_type_id: u16,
    offset: u64,
    length: u64,
    page_count: u32,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    table_hash: Hash32,
}

impl From<DirectoryEntry> for WriterBodyTableDirectoryEntry {
    fn from(value: DirectoryEntry) -> Self {
        Self {
            table_id: value.table_id,
            row_type_id: value.row_type_id,
            offset: value.offset,
            length: value.length,
            page_count: value.page_count,
            min_key: value.min_key,
            max_key: value.max_key,
            table_hash: value.table_hash,
        }
    }
}

fn encode_table_directory(tables: &[EncodedTableBody]) -> Result<Vec<u8>, FormatError> {
    let mut out = Vec::new();
    out.extend_from_slice(WRITER_BODY_TABLE_DIRECTORY_MAGIC);
    out.extend_from_slice(&WRITER_BODY_TABLE_DIRECTORY_VERSION.to_le_bytes());
    out.extend_from_slice(&(tables.len() as u16).to_le_bytes());
    for table in tables {
        out.extend_from_slice(&table.table_id.to_le_bytes());
        out.extend_from_slice(&table.row_type_id.to_le_bytes());
        out.extend_from_slice(&table.offset.to_le_bytes());
        out.extend_from_slice(&(table.bytes.len() as u64).to_le_bytes());
        out.extend_from_slice(&table.page_count.to_le_bytes());
        push_len_u16(&mut out, &table.min_key, "table min_key")?;
        push_len_u16(&mut out, &table.max_key, "table max_key")?;
        out.extend_from_slice(&table.table_hash);
    }
    let crc = crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok(out)
}

fn decode_table_directory(input: &[u8]) -> Result<(Vec<DirectoryEntry>, usize), FormatError> {
    let mut offset = 0usize;
    let magic = read_exact(input, &mut offset, 8)?;
    if magic != WRITER_BODY_TABLE_DIRECTORY_MAGIC {
        return Err(FormatError::InvalidMagic {
            context: "writer body table directory",
        });
    }
    let version = read_u16(input, &mut offset)?;
    if version != WRITER_BODY_TABLE_DIRECTORY_VERSION {
        return Err(FormatError::UnsupportedMajorVersion(version));
    }
    let table_count = read_u16(input, &mut offset)?;
    let mut entries = Vec::with_capacity(table_count as usize);
    for _ in 0..table_count {
        let table_id = read_u16(input, &mut offset)?;
        let row_type_id = read_u16(input, &mut offset)?;
        let table_offset = read_u64(input, &mut offset)?;
        let length = read_u64(input, &mut offset)?;
        let page_count = read_u32(input, &mut offset)?;
        let min_key = read_len_u16(input, &mut offset, "table min_key")?.to_vec();
        let max_key = read_len_u16(input, &mut offset, "table max_key")?.to_vec();
        let table_hash = read_hash(input, &mut offset)?;
        entries.push(DirectoryEntry {
            table_id,
            row_type_id,
            offset: table_offset,
            length,
            page_count,
            min_key,
            max_key,
            table_hash,
        });
    }
    let crc_end = offset
        .checked_add(4)
        .ok_or(FormatError::InvalidDeclaredLength {
            context: "writer body table directory",
        })?;
    if crc_end > input.len() {
        return Err(FormatError::TooShort {
            context: "writer body table directory crc",
            needed: crc_end,
            actual: input.len(),
        });
    }
    let expected_crc = u32::from_le_bytes(input[offset..crc_end].try_into().unwrap());
    if crc32c(&input[..offset]) != expected_crc {
        return Err(FormatError::HashMismatch {
            context: "writer body table directory crc32c",
        });
    }
    for pair in entries.windows(2) {
        if pair[0].table_id >= pair[1].table_id {
            return Err(FormatError::RecordsNotSorted);
        }
    }
    Ok((entries, crc_end))
}

fn encoded_directory_len(tables: &[EncodedTableBody]) -> Result<usize, FormatError> {
    let mut len: usize = 8 + 2 + 2 + 4;
    for table in tables {
        len = len
            .checked_add(2 + 2 + 8 + 8 + 4 + 2 + table.min_key.len() + 2 + table.max_key.len() + 32)
            .ok_or(FormatError::InvalidDeclaredLength {
                context: "writer body table directory",
            })?;
    }
    Ok(len)
}

fn page_directory_entry_len(page: &EncodedPage) -> Result<usize, FormatError> {
    2usize
        .checked_add(page.min_key.len())
        .and_then(|value| value.checked_add(2 + page.max_key.len()))
        .and_then(|value| value.checked_add(8 + 8 + 4 + 32))
        .ok_or(FormatError::InvalidDeclaredLength {
            context: "table page directory entry",
        })
}

fn ensure_rows_sorted(rows: &[TableRow]) -> Result<(), FormatError> {
    for pair in rows.windows(2) {
        if pair[0].key >= pair[1].key {
            return Err(FormatError::RecordsNotSorted);
        }
    }
    Ok(())
}

fn push_len_u16(out: &mut Vec<u8>, bytes: &[u8], context: &'static str) -> Result<(), FormatError> {
    let len =
        u16::try_from(bytes.len()).map_err(|_| FormatError::InvalidDeclaredLength { context })?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn read_len_u16<'a>(
    input: &'a [u8],
    offset: &mut usize,
    context: &'static str,
) -> Result<&'a [u8], FormatError> {
    let len = read_u16(input, offset)? as usize;
    read_exact_context(input, offset, len, context)
}

fn read_hash(input: &[u8], offset: &mut usize) -> Result<Hash32, FormatError> {
    Ok(read_exact(input, offset, 32)?.try_into().unwrap())
}

fn read_u16(input: &[u8], offset: &mut usize) -> Result<u16, FormatError> {
    Ok(u16::from_le_bytes(
        read_exact(input, offset, 2)?.try_into().unwrap(),
    ))
}

fn read_u32(input: &[u8], offset: &mut usize) -> Result<u32, FormatError> {
    Ok(u32::from_le_bytes(
        read_exact(input, offset, 4)?.try_into().unwrap(),
    ))
}

fn read_u64(input: &[u8], offset: &mut usize) -> Result<u64, FormatError> {
    Ok(u64::from_le_bytes(
        read_exact(input, offset, 8)?.try_into().unwrap(),
    ))
}

fn read_exact<'a>(
    input: &'a [u8],
    offset: &mut usize,
    len: usize,
) -> Result<&'a [u8], FormatError> {
    read_exact_context(input, offset, len, "table bytes")
}

fn read_exact_context<'a>(
    input: &'a [u8],
    offset: &mut usize,
    len: usize,
    context: &'static str,
) -> Result<&'a [u8], FormatError> {
    let end = offset
        .checked_add(len)
        .ok_or(FormatError::InvalidDeclaredLength { context })?;
    if end > input.len() {
        return Err(FormatError::TooShort {
            context,
            needed: end,
            actual: input.len(),
        });
    }
    let bytes = &input[*offset..end];
    *offset = end;
    Ok(bytes)
}

fn write_uleb(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn read_uleb(input: &[u8], offset: &mut usize) -> Result<u64, FormatError> {
    let mut shift = 0u32;
    let mut value = 0u64;
    loop {
        let byte = *read_exact(input, offset, 1)?
            .first()
            .ok_or(FormatError::TooShort {
                context: "uleb128",
                needed: 1,
                actual: 0,
            })?;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            return Err(FormatError::InvalidDeclaredLength { context: "uleb128" });
        }
    }
}

fn crc32c(bytes: &[u8]) -> u32 {
    const POLY: u32 = 0x82f63b78;
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ POLY
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_page_magic_is_deterministic() {
        assert_eq!(table_page_magic(0x0101, 1), *b"ANTP\x01\x01\x00\x01");
    }

    #[test]
    fn writer_body_tables_round_trip() {
        let encoded = encode_writer_body_tables(&[
            WriterBodyTable {
                table_id: 0x0102,
                row_type_id: 0x0102,
                rows: vec![TableRow {
                    key: b"b".to_vec(),
                    value: b"body".to_vec(),
                }],
            },
            WriterBodyTable {
                table_id: 0x0101,
                row_type_id: 0x0101,
                rows: vec![TableRow {
                    key: b"a".to_vec(),
                    value: b"head".to_vec(),
                }],
            },
        ])
        .unwrap();
        let decoded = decode_writer_body_tables(&encoded).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].table_id, 0x0101);
        assert_eq!(decoded[0].rows[0].value, b"head");
        assert_eq!(decoded[1].table_id, 0x0102);
        assert_eq!(decoded[1].rows[0].value, b"body");
    }
}
