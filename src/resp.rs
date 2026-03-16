use bytes::{Buf, BufMut, BytesMut};
use memchr::memmem;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
}

impl fmt::Display for RespValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RespValue::SimpleString(s) => write!(f, "SimpleString({})", s),
            RespValue::Error(e) => write!(f, "Error({})", e),
            RespValue::Integer(i) => write!(f, "Integer({})", i),
            RespValue::BulkString(Some(b)) => {
                let s = String::from_utf8_lossy(b);
                write!(f, "BulkString({})", s)
            }
            RespValue::BulkString(None) => write!(f, "BulkString(nil)"),
            RespValue::Array(Some(arr)) => write!(f, "Array({:?})", arr),
            RespValue::Array(None) => write!(f, "Array(nil)"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RespParseError {
    InvalidFormat(String),
    UnexpectedEof,
    InvalidMarker(u8),
    InvalidNumber(String),
    SizeExceeded(usize),
}

impl fmt::Display for RespParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RespParseError::InvalidFormat(msg) => write!(f, "Invalid format: {}", msg),
            RespParseError::UnexpectedEof => write!(f, "Unexpected end of input"),
            RespParseError::InvalidMarker(m) => write!(f, "Invalid marker: {}", m),
            RespParseError::InvalidNumber(s) => write!(f, "Invalid number: {}", s),
            RespParseError::SizeExceeded(size) => write!(f, "Size exceeded: {}", size),
        }
    }
}

impl std::error::Error for RespParseError {}

pub type RespResult<T> = Result<T, RespParseError>;

pub struct RespParser {
    buffer: BytesMut,
}

impl RespParser {
    const MAX_BULK_SIZE: usize = 1024 * 1024;
    const MAX_ARRAY_LEN: usize = 10000;

    pub fn new() -> Self {
        Self {
            buffer: BytesMut::new(),
        }
    }

    pub fn feed(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    pub fn parse_one(&mut self) -> RespResult<Option<RespValue>> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        let marker = self.buffer[0];
        let value = match marker {
            b'+' => self.parse_simple_string()?,
            b'-' => self.parse_error()?,
            b':' => self.parse_integer()?,
            b'$' => self.parse_bulk_string()?,
            b'*' => self.parse_array()?,
            _ => return Err(RespParseError::InvalidMarker(marker)),
        };

        Ok(Some(value))
    }

    fn parse_simple_string(&mut self) -> RespResult<RespValue> {
        let pos = memmem::find(&self.buffer[1..], b"\r\n").ok_or(RespParseError::UnexpectedEof)?;

        let cursor = pos + 1;
        let string_bytes = self.buffer[1..cursor].to_vec();
        self.buffer.advance(cursor + 2);

        let string = String::from_utf8(string_bytes)
            .map_err(|e| RespParseError::InvalidFormat(e.to_string()))?;

        Ok(RespValue::SimpleString(string))
    }

    fn parse_error(&mut self) -> RespResult<RespValue> {
        let pos = memmem::find(&self.buffer[1..], b"\r\n").ok_or(RespParseError::UnexpectedEof)?;

        let cursor = pos + 1;
        let error_bytes = self.buffer[1..cursor].to_vec();
        self.buffer.advance(cursor + 2);

        let error = String::from_utf8(error_bytes)
            .map_err(|e| RespParseError::InvalidFormat(e.to_string()))?;

        Ok(RespValue::Error(error))
    }

    fn parse_integer(&mut self) -> RespResult<RespValue> {
        let pos = memmem::find(&self.buffer[1..], b"\r\n").ok_or(RespParseError::UnexpectedEof)?;

        let cursor = pos + 1;
        let num_str = std::str::from_utf8(&self.buffer[1..cursor])
            .map_err(|e| RespParseError::InvalidFormat(e.to_string()))?;

        let value: i64 = num_str
            .parse()
            .map_err(|_| RespParseError::InvalidNumber(num_str.to_string()))?;

        self.buffer.advance(cursor + 2);
        Ok(RespValue::Integer(value))
    }

    fn parse_bulk_string(&mut self) -> RespResult<RespValue> {
        // Use memmem for fast CRLF search
        let pos = memmem::find(&self.buffer[1..], b"\r\n").ok_or(RespParseError::UnexpectedEof)?;

        let len_str = std::str::from_utf8(&self.buffer[1..pos + 1])
            .map_err(|e| RespParseError::InvalidFormat(e.to_string()))?;

        if len_str == "-1" {
            self.buffer.advance(pos + 3);
            return Ok(RespValue::BulkString(None));
        }

        let len: usize = len_str
            .parse()
            .map_err(|_| RespParseError::InvalidNumber(len_str.to_string()))?;

        if len > Self::MAX_BULK_SIZE {
            return Err(RespParseError::SizeExceeded(len));
        }

        let data_start = pos + 3; // 1 (marker) + pos + 2 (CRLF)
        let data_end = data_start + len + 2;

        if self.buffer.len() < data_end {
            return Err(RespParseError::UnexpectedEof);
        }

        // Validate CRLF after data
        if self.buffer[data_start + len] != b'\r'
            || self.buffer[data_start + len + 1] != b'\n'
        {
            return Err(RespParseError::InvalidFormat(
                "Missing CRLF after bulk string".to_string(),
            ));
        }

        let data = self.buffer[data_start..data_start + len].to_vec();
        self.buffer.advance(data_end);

        return Ok(RespValue::BulkString(Some(data)));
    }

    fn parse_array(&mut self) -> RespResult<RespValue> {
        let mut cursor = 1;

        while cursor < self.buffer.len() {
            if cursor + 1 < self.buffer.len()
                && self.buffer[cursor] == b'\r'
                && self.buffer[cursor + 1] == b'\n'
            {
                let count_str = std::str::from_utf8(&self.buffer[1..cursor])
                    .map_err(|e| RespParseError::InvalidFormat(e.to_string()))?;

                let count: usize = count_str
                    .parse()
                    .map_err(|_| RespParseError::InvalidNumber(count_str.to_string()))?;

                if count > Self::MAX_ARRAY_LEN {
                    return Err(RespParseError::SizeExceeded(count));
                }

                if count_str == "-1" {
                    self.buffer.advance(cursor + 2);
                    return Ok(RespValue::Array(None));
                }

                self.buffer.advance(cursor + 2);

                let mut elements = Vec::with_capacity(count);
                for _ in 0..count {
                    match self.parse_one()? {
                        Some(value) => elements.push(value),
                        None => return Err(RespParseError::UnexpectedEof),
                    }
                }

                return Ok(RespValue::Array(Some(elements)));
            }
            cursor += 1;
        }

        Err(RespParseError::UnexpectedEof)
    }
}

impl Default for RespParser {
    fn default() -> Self {
        Self::new()
    }
}

pub fn serialize(value: &RespValue) -> Vec<u8> {
    let mut output = Vec::new();
    serialize_value(value, &mut output);
    output
}

fn serialize_value(value: &RespValue, output: &mut Vec<u8>) {
    match value {
        RespValue::SimpleString(s) => {
            output.put_u8(b'+');
            output.put_slice(s.as_bytes());
            output.put_slice(b"\r\n");
        }
        RespValue::Error(e) => {
            output.put_u8(b'-');
            output.put_slice(e.as_bytes());
            output.put_slice(b"\r\n");
        }
        RespValue::Integer(i) => {
            output.put_u8(b':');
            let num_str = i.to_string();
            output.put_slice(num_str.as_bytes());
            output.put_slice(b"\r\n");
        }
        RespValue::BulkString(Some(data)) => {
            output.put_u8(b'$');
            let len_str = data.len().to_string();
            output.put_slice(len_str.as_bytes());
            output.put_slice(b"\r\n");
            output.put_slice(data);
            output.put_slice(b"\r\n");
        }
        RespValue::BulkString(None) => {
            output.put_slice(b"$-1\r\n");
        }
        RespValue::Array(Some(arr)) => {
            output.put_u8(b'*');
            let len_str = arr.len().to_string();
            output.put_slice(len_str.as_bytes());
            output.put_slice(b"\r\n");
            for item in arr {
                serialize_value(item, output);
            }
        }
        RespValue::Array(None) => {
            output.put_slice(b"*-1\r\n");
        }
    }
}

impl RespValue {
    pub fn ok() -> Self {
        RespValue::SimpleString("OK".to_string())
    }

    pub fn pong() -> Self {
        RespValue::SimpleString("PONG".to_string())
    }

    pub fn nil() -> Self {
        RespValue::BulkString(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_string() {
        let mut parser = RespParser::new();
        parser.feed(b"+OK\r\n");
        let value = parser.parse_one().unwrap().unwrap();
        assert_eq!(value, RespValue::SimpleString("OK".to_string()));
    }

    #[test]
    fn test_parse_error() {
        let mut parser = RespParser::new();
        parser.feed(b"-Error message\r\n");
        let value = parser.parse_one().unwrap().unwrap();
        assert_eq!(value, RespValue::Error("Error message".to_string()));
    }

    #[test]
    fn test_parse_integer() {
        let mut parser = RespParser::new();
        parser.feed(b":123\r\n");
        let value = parser.parse_one().unwrap().unwrap();
        assert_eq!(value, RespValue::Integer(123));
    }

    #[test]
    fn test_parse_bulk_string() {
        let mut parser = RespParser::new();
        parser.feed(b"$5\r\nhello\r\n");
        let value = parser.parse_one().unwrap().unwrap();
        assert_eq!(value, RespValue::BulkString(Some(b"hello".to_vec())));
    }

    #[test]
    fn test_parse_bulk_string_nil() {
        let mut parser = RespParser::new();
        parser.feed(b"$-1\r\n");
        let value = parser.parse_one().unwrap().unwrap();
        assert_eq!(value, RespValue::BulkString(None));
    }

    #[test]
    fn test_parse_array() {
        let mut parser = RespParser::new();
        parser.feed(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let value = parser.parse_one().unwrap().unwrap();
        let expected = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"foo".to_vec())),
            RespValue::BulkString(Some(b"bar".to_vec())),
        ]));
        assert_eq!(value, expected);
    }

    #[test]
    fn test_serialize_simple_string() {
        let value = RespValue::SimpleString("OK".to_string());
        let serialized = serialize(&value);
        assert_eq!(serialized, b"+OK\r\n");
    }

    #[test]
    fn test_serialize_bulk_string() {
        let value = RespValue::BulkString(Some(b"hello".to_vec()));
        let serialized = serialize(&value);
        assert_eq!(serialized, b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_serialize_integer() {
        let value = RespValue::Integer(42);
        let serialized = serialize(&value);
        assert_eq!(serialized, b":42\r\n");
    }

    #[test]
    fn test_serialize_array() {
        let value = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"foo".to_vec())),
            RespValue::BulkString(Some(b"bar".to_vec())),
        ]));
        let serialized = serialize(&value);
        assert_eq!(serialized, b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    }

    #[test]
    fn test_parse_incomplete() {
        let mut parser = RespParser::new();
        parser.feed(b"+OK");
        assert!(matches!(
            parser.parse_one(),
            Err(RespParseError::UnexpectedEof)
        ));
    }

    #[test]
    fn test_parse_incremental() {
        let mut parser = RespParser::new();
        parser.feed(b"+OK");
        assert!(parser.parse_one().is_err());

        parser.feed(b"\r\n");
        let value = parser.parse_one().unwrap().unwrap();
        assert_eq!(value, RespValue::SimpleString("OK".to_string()));
    }
}
