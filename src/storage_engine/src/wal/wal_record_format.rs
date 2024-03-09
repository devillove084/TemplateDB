pub const BLOCK_SIZE: usize = 32768;

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum RecordType {
    Zero = 0, // used for mmap based storage
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
}

impl From<usize> for RecordType {
    fn from(v: usize) -> Self {
        match v {
            0 => RecordType::Zero,
            1 => RecordType::Full,
            2 => RecordType::First,
            3 => RecordType::Middle,
            4 => RecordType::Last,
            _ => panic!("invalid RecordType: {}", v),
        }
    }
}

/// The format of a record header :
///
/// ```text
///
/// | ----- 4bytes ----- | -- 2bytes -- | - 1byte - |
///      CRC checksum         length     record type
/// ```
pub const HEADER_SIZE: usize = 7;

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, cmp::min, io::SeekFrom, rc::Rc};

    use rand::Rng;

    use super::{BLOCK_SIZE, HEADER_SIZE};
    use crate::{
        error::{TemplateKVError, TemplateResult},
        storage::File,
        util::{
            coding::encode_fixed_32,
            crc32::{hash, mask},
        },
        wal::{
            wal_record_format::RecordType::{First, Last, Middle},
            wal_record_reader::{Reader, Reporter},
            wal_record_writer::Writer,
        },
    };

    // Construct a string of the specified length made out of the supplied
    // partial string.
    fn big_string(partial_str: &str, n: usize) -> String {
        let mut s = String::new();
        while s.len() < n {
            s.push_str(partial_str);
        }
        s.truncate(n);
        s
    }

    // Construct a String from a number
    fn num_to_string(n: usize) -> String {
        n.to_string()
    }

    // Return a skewed potentially long string
    fn random_skewed_string(i: usize) -> String {
        let r = rand::thread_rng().gen_range(0, 1 << 17);
        big_string(&num_to_string(i), r)
    }

    #[derive(Clone)]
    struct StringFile {
        contents: Rc<RefCell<Vec<u8>>>,
        force_err: Rc<RefCell<bool>>,
        returned_partial: bool,
    }

    // Just for satisfy rustc
    unsafe impl Send for StringFile {}
    unsafe impl Sync for StringFile {}

    impl StringFile {
        pub fn new(data: Rc<RefCell<Vec<u8>>>) -> Self {
            Self {
                contents: data,
                force_err: Rc::new(RefCell::new(false)),
                returned_partial: false,
            }
        }
    }

    impl File for StringFile {
        fn write(&mut self, buf: &[u8]) -> TemplateResult<usize> {
            self.contents.borrow_mut().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> TemplateResult<()> {
            Ok(())
        }

        fn close(&mut self) -> TemplateResult<()> {
            unimplemented!()
        }

        fn seek(&mut self, pos: SeekFrom) -> TemplateResult<u64> {
            match pos {
                SeekFrom::Start(p) => {
                    if p > (self.contents.borrow().len() - 1) as u64 {
                        return Err(TemplateKVError::NotFound(Some(
                            "in-memory file seeking pasts the end".to_owned(),
                        )));
                    }
                    self.contents
                        .borrow_mut()
                        .drain(0..usize::try_from(p).expect("truncate error"));
                    Ok(p)
                }
                _ => panic!("only support seeking from starting point"),
            }
        }

        fn read(&mut self, buf: &mut [u8]) -> TemplateResult<usize> {
            assert!(!self.returned_partial, "must not read() after eof/error");
            if *self.force_err.borrow() {
                *self.force_err.borrow_mut() = false;
                self.returned_partial = true;
                return Err(TemplateKVError::Corruption("read error".to_owned()));
            }
            if self.contents.borrow().len() < buf.len() {
                self.returned_partial = true;
            }
            let length = min(self.contents.borrow().len(), buf.len());
            for i in 0..length {
                buf[i] = self.contents.borrow()[i];
            }
            self.contents.borrow_mut().drain(0..length);
            Ok(length)
        }

        fn read_all(&mut self, _buf: &mut Vec<u8>) -> TemplateResult<usize> {
            unimplemented!()
        }

        fn len(&self) -> TemplateResult<u64> {
            unimplemented!()
        }

        fn lock(&self) -> TemplateResult<()> {
            unimplemented!()
        }

        fn unlock(&self) -> TemplateResult<()> {
            unimplemented!()
        }

        fn read_at(&self, _buf: &mut [u8], _offset: u64) -> TemplateResult<usize> {
            unimplemented!()
        }
    }

    #[derive(Clone)]
    struct ReportCollector {
        dropped_bytes: Rc<RefCell<u64>>,
        message: Rc<RefCell<String>>,
    }

    impl Reporter for ReportCollector {
        fn corruption(&mut self, bytes: u64, reason: &str) {
            *self.dropped_bytes.borrow_mut() += bytes;
            self.message.borrow_mut().push_str(reason);
        }
    }

    impl ReportCollector {
        pub fn new() -> Self {
            Self {
                dropped_bytes: Rc::new(RefCell::new(0)),
                message: Rc::new(RefCell::new(String::default())),
            }
        }
    }

    // `read_source`, `writer` and `reader` all share the `source`
    struct RecordTest {
        source: Rc<RefCell<Vec<u8>>>,
        read_source: StringFile,
        reporter: ReportCollector,
        reading: bool,
        reader: Reader<StringFile>,
        writer: Writer<StringFile>,
    }
    const INITIAL_OFFSET_RECORD_SIZES: [usize; 6] = [
        10000,
        10000,
        2 * BLOCK_SIZE - 1000,
        1,
        13716,
        BLOCK_SIZE - HEADER_SIZE,
    ];

    const INITIAL_OFFSET_LAST_RECORD_OFFSETS: [usize; 6] = [
        0,
        HEADER_SIZE + 10000,
        2 * (HEADER_SIZE + 10000),
        2 * (HEADER_SIZE + 10000) + (2 * BLOCK_SIZE - 1000) + 3 * HEADER_SIZE,
        2 * (HEADER_SIZE + 10000) + (2 * BLOCK_SIZE - 1000) + 3 * HEADER_SIZE + HEADER_SIZE + 1,
        3 * BLOCK_SIZE,
    ];

    const EOF: &str = "EOF";

    impl RecordTest {
        pub fn new(reporter: &ReportCollector) -> Self {
            let data = Rc::new(RefCell::new(vec![]));
            let f = StringFile::new(data.clone());
            let writer = Writer::new(f.clone());
            Self {
                source: data.clone(),
                read_source: f.clone(),
                reporter: reporter.clone(),
                reading: false,
                reader: Reader::new(f.clone(), Some(Box::new(reporter.clone())), true, 0),
                writer,
            }
        }

        // Replace the current writer with a new one created from the current StringFile
        pub fn reopen_for_append(&mut self) {
            let writer = Writer::new(StringFile::new(self.source.clone()));
            self.writer = writer;
        }

        pub fn write(&mut self, msg: &str) {
            assert!(!self.reading, "cannot write() when some others are reading");
            self.writer
                .add_record(msg.as_bytes())
                .expect("fail to write: ");
        }

        pub fn written_bytes(&self) -> usize {
            self.source.borrow().len()
        }

        pub fn read(&mut self) -> String {
            if !self.reading {
                self.reading = true;
            };
            let mut buf = vec![];
            if self.reader.read_record(&mut buf) {
                unsafe { String::from_utf8_unchecked(buf) }
            } else {
                String::from(EOF)
            }
        }

        pub fn increment_byte(&mut self, offset: usize, delta: u8) {
            self.source.borrow_mut()[offset] += delta;
        }

        pub fn set_byte(&mut self, offset: usize, byte: u8) {
            self.source.borrow_mut()[offset] = byte;
        }

        pub fn shrink_size(&mut self, bytes: usize) {
            let written_bytes = self.source.borrow().len();
            self.source.borrow_mut().truncate(written_bytes - bytes);
        }

        pub fn fix_checksum(&mut self, header_offset: usize, len: usize) {
            let mut borrowed = self.source.borrow_mut();
            let contents = borrowed.as_mut_slice();
            // 6 = actual crc (4) + data length (2)
            let mut crc = hash(&contents[header_offset + 6..header_offset + 6 + len + 1]);
            crc = mask(crc);
            encode_fixed_32(&mut contents[header_offset..header_offset + 4], crc)
        }

        pub fn force_error(&mut self) {
            *self.read_source.force_err.borrow_mut() = true;
        }

        pub fn dropped_bytes(&self) -> u64 {
            *self.reporter.dropped_bytes.borrow()
        }

        pub fn reported_msg(&self) -> String {
            self.reporter.message.borrow().clone()
        }

        pub fn match_error(&self, msg: &str) -> bool {
            self.reporter.message.borrow().find(msg).is_some()
        }

        pub fn write_initial_offset_log(&mut self) {
            for i in 0..INITIAL_OFFSET_RECORD_SIZES.len() {
                let record = (0..INITIAL_OFFSET_RECORD_SIZES[i])
                    .map(|_| (b'a' + u8::try_from(i).expect("truncate error")) as char)
                    .collect::<String>();
                self.write(record.as_str());
            }
        }

        pub fn start_reading_at(&mut self, initial_offset: u64) {
            self.reader = Reader::new(
                self.read_source.clone(),
                Some(Box::new(self.reporter.clone())),
                true,
                initial_offset,
            );
        }

        // ensure that a reader never read a record from a offset beyond the whole file
        pub fn check_offset_past_end_returns_no_records(&mut self, offset_past_end: u64) {
            self.write_initial_offset_log();
            self.reading = true;
            let size = self.written_bytes() as u64;
            let mut reader = Reader::new(
                self.read_source.clone(),
                Some(Box::new(self.reporter.clone())),
                true,
                size + offset_past_end,
            );
            let mut buf = vec![];
            assert!(!reader.read_record(&mut buf));
        }

        // ensure that every records after the initial_offset matches
        pub fn check_initial_offset_record(
            &mut self,
            initial_offset: u64,
            mut expected_record_index: usize,
        ) {
            self.write_initial_offset_log();
            self.reading = true;
            let mut reader = Reader::new(
                self.read_source.clone(),
                Some(Box::new(self.reporter.clone())),
                true,
                initial_offset,
            );
            assert!(expected_record_index < INITIAL_OFFSET_LAST_RECORD_OFFSETS.len());
            let mut record = vec![];
            while expected_record_index < INITIAL_OFFSET_LAST_RECORD_OFFSETS.len() {
                assert!(reader.read_record(&mut record), "read_record() should work");
                assert_eq!(
                    record.len(),
                    INITIAL_OFFSET_RECORD_SIZES[expected_record_index],
                    "record length should match"
                );
                assert_eq!(
                    reader.last_record_offset(),
                    INITIAL_OFFSET_LAST_RECORD_OFFSETS[expected_record_index] as u64,
                    "last record offset should match"
                );
                assert_eq!(
                    b'a' + u8::try_from(expected_record_index).expect("truncate error"),
                    record[0],
                    "record content should match"
                );
                expected_record_index += 1;
            }
        }
    }

    fn new_record_test() -> RecordTest {
        RecordTest::new(&ReportCollector::new())
    }
    #[test]
    fn test_read_eof() {
        let mut log = new_record_test();
        assert_eq!(EOF, log.read().as_str());
    }

    #[test]
    fn test_read_write() {
        let mut log = new_record_test();
        log.write("foo");
        log.write("bar");
        log.write("");
        log.write("xxxx");
        assert_eq!("foo", log.read().as_str());
        assert_eq!("bar", log.read().as_str());
        assert_eq!("", log.read().as_str());
        assert_eq!("xxxx", log.read().as_str());
        assert_eq!(EOF, log.read().as_str());
        assert_eq!(EOF, log.read().as_str());
    }

    #[test]
    fn test_many_blocks() {
        let mut log = new_record_test();
        for i in 0..100_000 {
            log.write(num_to_string(i).as_str());
        }
        for i in 0..100_000 {
            let s = log.read();
            assert_eq!(num_to_string(i), s);
        }
        assert_eq!(EOF, log.read());
    }

    #[test]
    fn test_fragmentation_records() {
        let mut log = new_record_test();
        log.write("small");
        log.write(big_string("medium", 50_000).as_str());
        log.write(big_string("large", 100_000).as_str());
        assert_eq!("small", log.read());
        assert_eq!(big_string("medium", 50_000).as_str(), log.read());
        assert_eq!(big_string("large", 100_000).as_str(), log.read());
    }

    #[test]
    fn test_marginal_trailer() {
        let mut log = new_record_test();
        // make a trailer that is exactly the same length as an empty record
        let n = BLOCK_SIZE - 2 * HEADER_SIZE;
        log.write(big_string("foo", n).as_str());
        assert_eq!(BLOCK_SIZE - HEADER_SIZE, log.written_bytes());
        log.write("");
        log.write("bar");
        assert_eq!(big_string("foo", n).as_str(), log.read());
        assert_eq!("", log.read());
        assert_eq!("bar", log.read());
        assert_eq!(EOF, log.read());
    }

    // ensure no dropped bytes
    #[test]
    fn test_marginal_trailer2() {
        let mut log = new_record_test();
        // make a trailer that is exactly the same length as an empty record
        let n = BLOCK_SIZE - 2 * HEADER_SIZE;
        log.write(big_string("foo", n).as_str());
        assert_eq!(BLOCK_SIZE - HEADER_SIZE, log.written_bytes());
        log.write("bar");
        assert_eq!(big_string("foo", n).as_str(), log.read());
        assert_eq!("bar", log.read());
        assert_eq!(EOF, log.read());
        assert_eq!(0, log.dropped_bytes());
        assert_eq!("", log.reported_msg().as_str());
    }

    #[test]
    fn test_short_trailer() {
        let mut log = new_record_test();
        let n = BLOCK_SIZE - 2 * HEADER_SIZE + 4;
        log.write(big_string("foo", n).as_str());
        assert_eq!(BLOCK_SIZE - HEADER_SIZE + 4, log.written_bytes());
        log.write("");
        log.write("bar");
        assert_eq!(big_string("foo", n).as_str(), log.read());
        assert_eq!("", log.read());
        assert_eq!("bar", log.read());
        assert_eq!(EOF, log.read());
    }

    #[test]
    fn test_aligned_eof() {
        let mut log = new_record_test();
        let n = BLOCK_SIZE - 2 * HEADER_SIZE + 4;
        log.write(big_string("foo", n).as_str());
        assert_eq!(BLOCK_SIZE - HEADER_SIZE + 4, log.written_bytes());
        assert_eq!(big_string("foo", n).as_str(), log.read());
        assert_eq!(EOF, log.read());
    }

    #[test]
    fn test_open_for_append() {
        let mut log = new_record_test();
        log.write("hello");
        log.reopen_for_append();
        log.write("world");
        assert_eq!("hello", log.read());
        assert_eq!("world", log.read());
        assert_eq!(EOF, log.read());
    }

    #[test]
    fn test_random_read() {
        let mut log = new_record_test();
        let n = 100;
        let mut skewed_strings = vec![];
        for i in 0..n {
            skewed_strings.push(random_skewed_string(i));
        }
        for s in &skewed_strings {
            log.write(s.as_str());
        }
        for s in &skewed_strings {
            assert_eq!(s.as_str(), log.read());
        }
        assert_eq!(EOF, log.read());
    }

    #[test]
    fn test_read_error() {
        let mut log = new_record_test();
        log.write("foo");
        log.force_error();
        assert_eq!(EOF, log.read());
        assert_eq!(BLOCK_SIZE as u64, log.dropped_bytes());
        assert!(log.match_error("read error"));
    }

    #[test]
    #[should_panic(expected = "invalid RecordType: 101")]
    fn test_bad_record_type() {
        let mut log = new_record_test();
        let test = "foo";
        log.write(test);
        // the record type is in header[6]
        log.increment_byte(6, 100);
        log.fix_checksum(0, test.len());
        log.read();
    }

    #[test]
    fn test_truncated_trailing_record_is_ignored() {
        let mut log = new_record_test();
        log.write("foo");
        log.shrink_size(4); // drop all data payload (3) as well as record type (1)
        assert_eq!(EOF, log.read());
        // truncated last record is ignored, not treated as an error
        assert_eq!(0, log.dropped_bytes());
        assert_eq!("", log.reported_msg());
    }

    #[test]
    fn test_bad_record_length() {
        let mut log = new_record_test();
        let payload_size = BLOCK_SIZE - HEADER_SIZE;
        log.write(big_string("bar", payload_size).as_str());
        log.write("foo");
        // Least significant size byte is stored in header[4]
        log.increment_byte(4, 1);
        assert_eq!("foo", log.read());
        assert_eq!(BLOCK_SIZE as u64, log.dropped_bytes());
        assert!(log.match_error("bad record length"));
    }

    #[test]
    fn test_bad_length_at_end_is_ignored() {
        let mut log = new_record_test();
        log.write("foo");
        log.shrink_size(1);
        assert_eq!(EOF, log.read());
        assert_eq!(0, log.dropped_bytes());
        assert_eq!("", log.reported_msg());
    }

    #[test]
    fn test_checksum_mismatch() {
        let mut log = new_record_test();
        log.write("foo");
        log.increment_byte(0, 10);
        assert_eq!(EOF, log.read());
        assert_eq!(10, log.dropped_bytes());
        assert!(log.match_error("checksum mismatch"));
    }

    #[test]
    fn test_unexpected_middle_type() {
        let mut log = new_record_test();
        log.write("foo");
        log.set_byte(6, Middle as u8);
        log.fix_checksum(0, 3);
        assert_eq!(EOF, log.read());
        assert_eq!(3, log.dropped_bytes());
        assert!(log.match_error("missing start"));
    }

    #[test]
    fn test_unexpected_last_type() {
        let mut log = new_record_test();
        log.write("foo");
        log.set_byte(6, Last as u8);
        log.fix_checksum(0, 3);
        assert_eq!(EOF, log.read());
        assert_eq!(3, log.dropped_bytes());
        assert!(log.match_error("missing start"));
    }

    #[test]
    fn test_unexpected_full_type() {
        let mut log = new_record_test();
        log.write("foo");
        log.write("bar");
        log.set_byte(6, First as u8);
        log.fix_checksum(0, 3);
        assert_eq!("bar", log.read());
        assert_eq!(EOF, log.read());
        assert_eq!(3, log.dropped_bytes());
        assert!(log.match_error("partial record without end"));
    }

    #[test]
    fn test_unexpected_first_type() {
        let mut log = new_record_test();
        log.write("foo");
        log.write(big_string("bar", 100_000).as_str());
        log.set_byte(6, First as u8);
        log.fix_checksum(0, 3);
        assert_eq!(big_string("bar", 100_000).as_str(), log.read());
        assert_eq!(EOF, log.read());
        assert_eq!(3, log.dropped_bytes());
        assert!(log.match_error("partial record without end"));
    }

    #[test]
    fn test_missing_last_is_ignored() {
        let mut log = new_record_test();
        log.write(big_string("bar", BLOCK_SIZE).as_str());
        // Remove the LAST block, including header.
        log.shrink_size(14);
        assert_eq!(EOF, log.read());
        assert_eq!(0, log.dropped_bytes());
        assert_eq!("", log.reported_msg());
    }

    #[test]
    fn test_partial_last_is_ignored() {
        let mut log = new_record_test();
        log.write(big_string("bar", BLOCK_SIZE).as_str());
        // cause a bad record length in the Last block
        log.shrink_size(1);
        assert_eq!(EOF, log.read());
        assert_eq!(0, log.dropped_bytes());
        assert_eq!("", log.reported_msg());
    }

    #[test]
    fn test_skip_into_multi_record() {
        // Consider a fragmented record:
        //    first(R1), middle(R1), last(R1), first(R2)
        // If initial_offset points to a record after first(R1) but before first(R2)
        // incomplete fragment errors are not actual errors, and must be suppressed
        // until a new first or full record is encountered.
        let mut log = new_record_test();
        log.write(big_string("foo", 3 * BLOCK_SIZE).as_str());
        log.write("correct");
        log.start_reading_at(BLOCK_SIZE as u64);
        assert_eq!("correct", log.read());
        assert_eq!("", log.reported_msg());
        assert_eq!(0, log.dropped_bytes());
        assert_eq!(EOF, log.read());
    }

    #[test]
    fn test_error_joins_records() {
        // Consider two fragmented records:
        //    first(R1) last(R1) first(R2) last(R2)
        // where the middle two fragments are bad records.  We do not want
        // first(R1),last(R2) to get joined and returned as a valid record.

        let mut log = new_record_test();
        // write records that span two blocks
        log.write(big_string("foo", BLOCK_SIZE).as_str());
        log.write(big_string("bar", BLOCK_SIZE).as_str());
        log.write("correct");

        // wipe the middle block
        for i in BLOCK_SIZE..2 * BLOCK_SIZE {
            log.set_byte(i, b'x');
        }
        assert_eq!("correct", log.read());
        assert_eq!(EOF, log.read());
        let dropped_bytes = log.dropped_bytes();
        assert!(dropped_bytes < 2 * BLOCK_SIZE as u64 + 100);
        assert!(dropped_bytes > 2 * BLOCK_SIZE as u64);
        assert!(log.match_error("bad record read in middle of record"));
    }

    macro_rules! initial_offset_check {
        ($($name:ident: $param: expr, )*) => {
            $(
                #[test]
                fn $name() {
                    let mut log = new_record_test();
                    let (initial_offset, expected_index) = $param;
                    log.check_initial_offset_record(initial_offset, expected_index);
                }
            )*
        };
    }

    initial_offset_check!(
        test_check_read_start: (0, 0),
        test_check_read_second_one_off: (1, 1),
        test_check_read_second_ten_thousand: (10000, 1),
        test_check_read_second_start: (10007, 1),
        test_check_read_third_one_off: (10008, 2),
        test_check_read_third_start: (20014, 2),
        test_check_read_fourth_one_off: (20015, 3),
        test_check_read_fourth_first_block_trailer: (BLOCK_SIZE as u64 -4, 3),
        test_check_read_fourth_middle_block: (BLOCK_SIZE as u64 + 1, 3),
        test_check_read_fourth_last_block: (2 * BLOCK_SIZE as u64 + 1, 3),
        test_check_read_fourth_start: (2 * (HEADER_SIZE as u64 + 1000) + (2 * BLOCK_SIZE as u64 - 1000) + 3 * HEADER_SIZE as u64, 3),
        test_check_read_initial_offset_into_block_padding: (3 * BLOCK_SIZE as u64 - 3, 5),
    );

    #[test]
    fn test_check_read_end() {
        let mut log = new_record_test();
        log.check_offset_past_end_returns_no_records(0);
    }

    #[test]
    fn test_check_read_past_end() {
        let mut log = new_record_test();
        log.check_offset_past_end_returns_no_records(0);
    }
}
