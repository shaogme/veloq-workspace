use core::{
    cmp,
    fmt::{self, Write},
    hash::{Hash, Hasher},
    iter::FusedIterator,
    ops, slice, str,
};

use crate::alloc_crate::{
    borrow::Cow, boxed::Box, collections::TryReserveError, rc::Rc, string::String, sync::Arc,
    vec::Vec,
};

#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Copy)]
pub struct CodePoint(u32);

impl fmt::Debug for CodePoint {
    #[inline]
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "U+{:04X}", self.0)
    }
}

impl CodePoint {
    #[inline]
    pub const unsafe fn from_u32_unchecked(value: u32) -> CodePoint {
        CodePoint(value)
    }

    #[inline]
    pub const fn from_char(value: char) -> CodePoint {
        CodePoint(value as u32)
    }

    #[inline]
    pub const fn to_u32(self) -> u32 {
        self.0
    }

    #[inline]
    pub const fn to_lead_surrogate(self) -> Option<u16> {
        match self.0 {
            lead @ 0xD800..=0xDBFF => Some(lead as u16),
            _ => None,
        }
    }

    #[inline]
    pub const fn to_trail_surrogate(self) -> Option<u16> {
        match self.0 {
            trail @ 0xDC00..=0xDFFF => Some(trail as u16),
            _ => None,
        }
    }
}

impl Hash for CodePoint {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

#[inline]
pub fn encode_utf16_raw(mut code_point: u32, dst: &mut [u16; 2]) -> &[u16] {
    if (code_point & 0xFFFF) == code_point {
        dst[0] = code_point as u16;
        &dst[..1]
    } else {
        code_point -= 0x10000;
        dst[0] = 0xD800 | ((code_point >> 10) as u16);
        dst[1] = 0xDC00 | ((code_point & 0x3FF) as u16);
        &dst[..2]
    }
}

#[inline]
pub fn encode_utf8_raw(code: u32, dst: &mut [u8; 4]) -> &[u8] {
    if code < 0x80 {
        dst[0] = code as u8;
        &dst[..1]
    } else if code < 0x800 {
        dst[0] = 0xC0 | ((code >> 6) as u8);
        dst[1] = 0x80 | ((code & 0x3F) as u8);
        &dst[..2]
    } else if code < 0x10000 {
        dst[0] = 0xE0 | ((code >> 12) as u8);
        dst[1] = 0x80 | (((code >> 6) & 0x3F) as u8);
        dst[2] = 0x80 | ((code & 0x3F) as u8);
        &dst[..3]
    } else {
        dst[0] = 0xF0 | ((code >> 18) as u8);
        dst[1] = 0x80 | (((code >> 12) & 0x3F) as u8);
        dst[2] = 0x80 | (((code >> 6) & 0x3F) as u8);
        dst[3] = 0x80 | ((code & 0x3F) as u8);
        &dst[..4]
    }
}

#[inline]
fn decode_surrogate(second_byte: u8, third_byte: u8) -> u16 {
    0xD800 | ((second_byte as u16 & 0x3F) << 6) | (third_byte as u16 & 0x3F)
}

#[inline]
fn decode_surrogate_pair(lead: u16, trail: u16) -> char {
    let code_point = 0x10000 + ((((lead - 0xD800) as u32) << 10) | (trail - 0xDC00) as u32);
    unsafe { char::from_u32_unchecked(code_point) }
}

#[inline]
unsafe fn next_code_point<'a, I: Iterator<Item = &'a u8>>(bytes: &mut I) -> Option<u32> {
    let x = *bytes.next()?;
    if x < 128 {
        return Some(x as u32);
    }
    let init = (x & (0x7F >> 2)) as u32;
    let y = *bytes.next()?;
    let mut ch = (init << 6) | (y & 0x3F) as u32;
    if x >= 0xE0 {
        let z = *bytes.next()?;
        let y_z = ((y & 0x3F) as u32) << 6 | (z & 0x3F) as u32;
        ch = init << 12 | y_z;
        if x >= 0xF0 {
            let w = *bytes.next()?;
            ch = (init & 7) << 18 | (y_z << 6) | (w & 0x3F) as u32;
        }
    }
    Some(ch)
}

#[derive(Debug)]
pub enum Utf8BoundaryError {
    NotABoundary,
    OutOfBounds,
    BetweenSurrogates,
}

#[derive(Eq, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Wtf8 {
    bytes: [u8],
}

impl AsRef<[u8]> for Wtf8 {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl fmt::Debug for Wtf8 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_char('"')?;
        let mut pos = 0;
        while let Some((surrogate_pos, surrogate)) = self.next_surrogate(pos) {
            let valid = unsafe { str::from_utf8_unchecked(&self.bytes[pos..surrogate_pos]) };
            for c in valid.chars() {
                for ec in c.escape_debug() {
                    formatter.write_char(ec)?;
                }
            }
            write!(formatter, "\\u{{{:x}}}", surrogate)?;
            pos = surrogate_pos + 3;
        }
        let tail = unsafe { str::from_utf8_unchecked(&self.bytes[pos..]) };
        for c in tail.chars() {
            for ec in c.escape_debug() {
                formatter.write_char(ec)?;
            }
        }
        formatter.write_char('"')
    }
}

impl fmt::Display for Wtf8 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let wtf8_bytes = &self.bytes;
        let mut pos = 0;
        loop {
            match self.next_surrogate(pos) {
                Some((surrogate_pos, _)) => {
                    let s = unsafe { str::from_utf8_unchecked(&wtf8_bytes[pos..surrogate_pos]) };
                    formatter.write_str(s)?;
                    formatter.write_char(char::REPLACEMENT_CHARACTER)?;
                    pos = surrogate_pos + 3;
                }
                None => {
                    let s = unsafe { str::from_utf8_unchecked(&wtf8_bytes[pos..]) };
                    if pos == 0 {
                        return s.fmt(formatter);
                    } else {
                        return formatter.write_str(s);
                    }
                }
            }
        }
    }
}

impl Wtf8 {
    #[inline]
    pub fn from_str(value: &str) -> &Wtf8 {
        unsafe { Wtf8::from_bytes_unchecked(value.as_bytes()) }
    }

    #[inline]
    pub const unsafe fn from_bytes_unchecked(value: &[u8]) -> &Wtf8 {
        unsafe { &*(value as *const [u8] as *const Wtf8) }
    }

    #[inline]
    pub unsafe fn from_mut_bytes_unchecked(value: &mut [u8]) -> &mut Wtf8 {
        unsafe { &mut *(value as *mut [u8] as *mut Wtf8) }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    #[inline]
    pub fn code_points(&self) -> Wtf8CodePoints<'_> {
        Wtf8CodePoints {
            bytes: self.bytes.iter(),
        }
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[inline]
    pub fn as_str(&self) -> Result<&str, str::Utf8Error> {
        str::from_utf8(&self.bytes)
    }

    #[inline]
    pub fn encode_wide(&self) -> EncodeWide<'_> {
        EncodeWide {
            code_points: self.code_points(),
            extra: 0,
        }
    }

    #[inline]
    pub fn next_surrogate(&self, mut pos: usize) -> Option<(usize, u16)> {
        let mut iter = self.bytes[pos..].iter();
        loop {
            let b = *iter.next()?;
            if b < 0x80 {
                pos += 1;
            } else if b < 0xE0 {
                iter.next();
                pos += 2;
            } else if b == 0xED {
                match (iter.next(), iter.next()) {
                    (Some(&b2), Some(&b3)) if b2 >= 0xA0 => {
                        return Some((pos, decode_surrogate(b2, b3)));
                    }
                    _ => pos += 3,
                }
            } else if b < 0xF0 {
                iter.next();
                iter.next();
                pos += 3;
            } else {
                iter.next();
                iter.next();
                iter.next();
                pos += 4;
            }
        }
    }

    #[inline]
    pub fn final_lead_surrogate(&self) -> Option<u16> {
        match self.bytes {
            [.., 0xED, b2 @ 0xA0..=0xAF, b3] => Some(decode_surrogate(b2, b3)),
            _ => None,
        }
    }

    #[inline]
    pub fn initial_trail_surrogate(&self) -> Option<u16> {
        match self.bytes {
            [0xED, b2 @ 0xB0..=0xBF, b3, ..] => Some(decode_surrogate(b2, b3)),
            _ => None,
        }
    }

    #[inline]
    pub fn make_ascii_lowercase(&mut self) {
        self.bytes.make_ascii_lowercase();
    }

    #[inline]
    pub fn make_ascii_uppercase(&mut self) {
        self.bytes.make_ascii_uppercase();
    }

    #[inline]
    pub fn is_ascii(&self) -> bool {
        self.bytes.is_ascii()
    }

    #[inline]
    pub fn eq_ignore_ascii_case(&self, other: &Self) -> bool {
        self.bytes.eq_ignore_ascii_case(&other.bytes)
    }

    #[inline]
    pub fn is_code_point_boundary(&self, index: usize) -> bool {
        if index == 0 {
            return true;
        }
        match self.bytes.get(index) {
            None => index == self.len(),
            Some(&b) => (b as i8) >= -0x40,
        }
    }

    #[track_caller]
    #[inline]
    pub fn check_utf8_boundary(&self, index: usize) {
        if let Err(err) = self.try_check_utf8_boundary(index) {
            match err {
                Utf8BoundaryError::NotABoundary => {
                    panic!("byte index {index} is not a codepoint boundary");
                }
                Utf8BoundaryError::OutOfBounds => {
                    panic!("byte index {index} is out of bounds");
                }
                Utf8BoundaryError::BetweenSurrogates => {
                    panic!("byte index {index} lies between surrogate codepoints");
                }
            }
        }
    }

    #[track_caller]
    #[inline]
    pub fn try_check_utf8_boundary(&self, index: usize) -> Result<(), Utf8BoundaryError> {
        if index == 0 {
            return Ok(());
        }
        match self.bytes.get(index) {
            Some(0xED) => (),
            Some(&b) if (b as i8) >= -0x40 => return Ok(()),
            Some(_) => return Err(Utf8BoundaryError::NotABoundary),
            None if index == self.len() => return Ok(()),
            None => return Err(Utf8BoundaryError::OutOfBounds),
        }
        if index + 1 < self.bytes.len()
            && self.bytes[index + 1] >= 0xA0
            && index >= 3
            && self.bytes[index - 3] == 0xED
            && self.bytes[index - 2] >= 0xA0
        {
            return Err(Utf8BoundaryError::BetweenSurrogates);
        }
        Ok(())
    }

    pub fn to_owned(&self) -> Wtf8Buf {
        Wtf8Buf {
            bytes: self.as_bytes().to_vec(),
            is_known_utf8: false,
        }
    }

    pub fn clone_into(&self, buf: &mut Wtf8Buf) {
        buf.is_known_utf8 = false;
        self.as_bytes().clone_into(&mut buf.bytes);
    }

    pub fn to_string_lossy(&self) -> Cow<'_, str> {
        let Some((surrogate_pos, _)) = self.next_surrogate(0) else {
            return Cow::Borrowed(unsafe { str::from_utf8_unchecked(self.as_bytes()) });
        };
        let wtf8_bytes = self.as_bytes();
        let mut utf8_bytes = Vec::with_capacity(self.len());
        utf8_bytes.extend_from_slice(&wtf8_bytes[..surrogate_pos]);
        utf8_bytes.extend_from_slice("\u{FFFD}".as_bytes());
        let mut pos = surrogate_pos + 3;
        loop {
            match self.next_surrogate(pos) {
                Some((surrogate_pos, _)) => {
                    utf8_bytes.extend_from_slice(&wtf8_bytes[pos..surrogate_pos]);
                    utf8_bytes.extend_from_slice("\u{FFFD}".as_bytes());
                    pos = surrogate_pos + 3;
                }
                None => {
                    utf8_bytes.extend_from_slice(&wtf8_bytes[pos..]);
                    return Cow::Owned(unsafe { String::from_utf8_unchecked(utf8_bytes) });
                }
            }
        }
    }

    pub fn empty_box() -> Box<Wtf8> {
        let boxed: Box<[u8]> = Default::default();
        unsafe { Box::from_raw(Box::into_raw(boxed) as *mut Wtf8) }
    }

    pub fn to_arc(&self) -> Arc<Wtf8> {
        let arc: Arc<[u8]> = Arc::from(self.as_bytes());
        unsafe { Arc::from_raw(Arc::into_raw(arc) as *const Wtf8) }
    }

    pub fn to_rc(&self) -> Rc<Wtf8> {
        let rc: Rc<[u8]> = Rc::from(self.as_bytes());
        unsafe { Rc::from_raw(Rc::into_raw(rc) as *const Wtf8) }
    }

    #[inline]
    pub fn to_ascii_lowercase(&self) -> Wtf8Buf {
        Wtf8Buf {
            bytes: self.as_bytes().to_ascii_lowercase(),
            is_known_utf8: false,
        }
    }

    #[inline]
    pub fn to_ascii_uppercase(&self) -> Wtf8Buf {
        Wtf8Buf {
            bytes: self.as_bytes().to_ascii_uppercase(),
            is_known_utf8: false,
        }
    }
}

impl ops::Index<ops::Range<usize>> for Wtf8 {
    type Output = Wtf8;

    #[inline]
    fn index(&self, range: ops::Range<usize>) -> &Wtf8 {
        assert!(range.start <= range.end);
        assert!(self.is_code_point_boundary(range.start));
        assert!(self.is_code_point_boundary(range.end));
        unsafe {
            let len = range.end - range.start;
            let start = self.as_bytes().as_ptr().add(range.start);
            Wtf8::from_bytes_unchecked(slice::from_raw_parts(start, len))
        }
    }
}

impl ops::Index<ops::RangeFrom<usize>> for Wtf8 {
    type Output = Wtf8;

    #[inline]
    fn index(&self, range: ops::RangeFrom<usize>) -> &Wtf8 {
        &self[range.start..self.len()]
    }
}

impl ops::Index<ops::RangeTo<usize>> for Wtf8 {
    type Output = Wtf8;

    #[inline]
    fn index(&self, range: ops::RangeTo<usize>) -> &Wtf8 {
        &self[0..range.end]
    }
}

impl ops::Index<ops::RangeFull> for Wtf8 {
    type Output = Wtf8;

    #[inline]
    fn index(&self, _range: ops::RangeFull) -> &Wtf8 {
        self
    }
}

impl Hash for Wtf8 {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(&self.bytes);
        0xfeu8.hash(state);
    }
}

#[derive(Clone)]
pub struct Wtf8CodePoints<'a> {
    bytes: slice::Iter<'a, u8>,
}

impl Iterator for Wtf8CodePoints<'_> {
    type Item = CodePoint;

    #[inline]
    fn next(&mut self) -> Option<CodePoint> {
        unsafe { next_code_point(&mut self.bytes).map(|c| CodePoint::from_u32_unchecked(c)) }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.bytes.len();
        (len.saturating_add(3) / 4, Some(len))
    }
}

impl FusedIterator for Wtf8CodePoints<'_> {}

#[derive(Clone)]
pub struct EncodeWide<'a> {
    code_points: Wtf8CodePoints<'a>,
    extra: u16,
}

impl Iterator for EncodeWide<'_> {
    type Item = u16;

    #[inline]
    fn next(&mut self) -> Option<u16> {
        if self.extra != 0 {
            let tmp = self.extra;
            self.extra = 0;
            return Some(tmp);
        }

        let mut buf = [0; 2];
        self.code_points.next().map(|code_point| {
            let n = encode_utf16_raw(code_point.to_u32(), &mut buf).len();
            if n == 2 {
                self.extra = buf[1];
            }
            buf[0]
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let (low, high) = self.code_points.size_hint();
        let ext = (self.extra != 0) as usize;
        (
            low + ext,
            high.and_then(|n| n.checked_mul(2))
                .and_then(|n| n.checked_add(ext)),
        )
    }
}

impl FusedIterator for EncodeWide<'_> {}

impl fmt::Debug for EncodeWide<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct CodeUnit(u16);
        impl fmt::Debug for CodeUnit {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match char::from_u32(self.0 as u32) {
                    Some(c) => write!(f, "{c:?}"),
                    None => write!(f, "0x{:04X}", self.0),
                }
            }
        }

        write!(f, "EncodeWide(")?;
        f.debug_list()
            .entries(self.clone().map(CodeUnit))
            .finish()?;
        write!(f, ")")
    }
}

#[derive(Clone)]
pub struct Wtf8Buf {
    bytes: Vec<u8>,
    is_known_utf8: bool,
}

impl PartialEq for Wtf8Buf {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl Eq for Wtf8Buf {}

impl PartialOrd for Wtf8Buf {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Wtf8Buf {
    #[inline]
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.bytes.cmp(&other.bytes)
    }
}

impl ops::Deref for Wtf8Buf {
    type Target = Wtf8;

    #[inline]
    fn deref(&self) -> &Wtf8 {
        self.as_slice()
    }
}

impl ops::DerefMut for Wtf8Buf {
    #[inline]
    fn deref_mut(&mut self) -> &mut Wtf8 {
        self.as_mut_slice()
    }
}

impl fmt::Debug for Wtf8Buf {
    #[inline]
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, formatter)
    }
}

impl fmt::Display for Wtf8Buf {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_known_utf8 {
            let s = unsafe { str::from_utf8_unchecked(self.as_bytes()) };
            fmt::Display::fmt(s, formatter)
        } else {
            fmt::Display::fmt(&**self, formatter)
        }
    }
}

impl Wtf8Buf {
    #[inline]
    pub const fn new() -> Wtf8Buf {
        Wtf8Buf {
            bytes: Vec::new(),
            is_known_utf8: true,
        }
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Wtf8Buf {
        Wtf8Buf {
            bytes: Vec::with_capacity(capacity),
            is_known_utf8: true,
        }
    }

    #[inline]
    pub const unsafe fn from_bytes_unchecked(value: Vec<u8>) -> Wtf8Buf {
        Wtf8Buf {
            bytes: value,
            is_known_utf8: false,
        }
    }

    #[inline]
    pub const fn from_string(string: String) -> Wtf8Buf {
        Wtf8Buf {
            bytes: string.into_bytes(),
            is_known_utf8: true,
        }
    }

    pub fn clear(&mut self) {
        self.bytes.clear();
        self.is_known_utf8 = true;
    }

    pub fn from_wide(v: &[u16]) -> Wtf8Buf {
        let mut string = Wtf8Buf::with_capacity(v.len());
        for item in char::decode_utf16(v.iter().cloned()) {
            match item {
                Ok(ch) => string.push_char(ch),
                Err(surrogate) => {
                    let surrogate = surrogate.unpaired_surrogate();
                    let code_point = unsafe { CodePoint::from_u32_unchecked(surrogate as u32) };
                    string.is_known_utf8 = false;
                    unsafe {
                        string.push_code_point_unchecked(code_point);
                    }
                }
            }
        }
        string
    }

    unsafe fn push_code_point_unchecked(&mut self, code_point: CodePoint) {
        let mut bytes = [0; 4];
        let bytes = encode_utf8_raw(code_point.to_u32(), &mut bytes);
        self.bytes.extend_from_slice(bytes);
    }

    #[inline]
    pub fn as_slice(&self) -> &Wtf8 {
        unsafe { Wtf8::from_bytes_unchecked(&self.bytes) }
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut Wtf8 {
        unsafe { Wtf8::from_mut_bytes_unchecked(&mut self.bytes) }
    }

    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.bytes.reserve(additional);
    }

    #[inline]
    pub fn try_reserve(&mut self, additional: usize) -> Result<(), TryReserveError> {
        self.bytes.try_reserve(additional)
    }

    #[inline]
    pub fn reserve_exact(&mut self, additional: usize) {
        self.bytes.reserve_exact(additional);
    }

    #[inline]
    pub fn try_reserve_exact(&mut self, additional: usize) -> Result<(), TryReserveError> {
        self.bytes.try_reserve_exact(additional)
    }

    #[inline]
    pub fn shrink_to_fit(&mut self) {
        self.bytes.shrink_to_fit();
    }

    #[inline]
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.bytes.shrink_to(min_capacity);
    }

    #[inline]
    pub fn leak<'a>(self) -> &'a mut Wtf8 {
        unsafe { Wtf8::from_mut_bytes_unchecked(self.bytes.leak()) }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.bytes.capacity()
    }

    #[inline]
    pub fn push_wtf8(&mut self, other: &Wtf8) {
        match (self.final_lead_surrogate(), other.initial_trail_surrogate()) {
            (Some(lead), Some(trail)) => {
                let len_without_lead_surrogate = self.len() - 3;
                self.bytes.truncate(len_without_lead_surrogate);
                let other_without_trail_surrogate = &other.as_bytes()[3..];
                self.bytes.reserve(4 + other_without_trail_surrogate.len());
                self.push_char(decode_surrogate_pair(lead, trail));
                self.bytes.extend_from_slice(other_without_trail_surrogate);
            }
            _ => {
                if self.is_known_utf8 && other.next_surrogate(0).is_some() {
                    self.is_known_utf8 = false;
                }
                self.bytes.extend_from_slice(other.as_bytes());
            }
        }
    }

    #[inline]
    pub fn push_char(&mut self, c: char) {
        unsafe { self.push_code_point_unchecked(CodePoint::from_char(c)) }
    }

    #[inline]
    pub fn push(&mut self, code_point: CodePoint) {
        if let Some(trail) = code_point.to_trail_surrogate() {
            if let Some(lead) = self.final_lead_surrogate() {
                let len_without_lead_surrogate = self.len() - 3;
                self.bytes.truncate(len_without_lead_surrogate);
                self.push_char(decode_surrogate_pair(lead, trail));
                return;
            }
            self.is_known_utf8 = false;
        } else if code_point.to_lead_surrogate().is_some() {
            self.is_known_utf8 = false;
        }

        unsafe { self.push_code_point_unchecked(code_point) }
    }

    #[inline]
    pub fn truncate(&mut self, new_len: usize) {
        if new_len <= self.len() {
            assert!(self.is_code_point_boundary(new_len));
            self.bytes.truncate(new_len);
        }
    }

    #[inline]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    pub fn into_string(self) -> Result<String, Wtf8Buf> {
        if self.is_known_utf8 || self.next_surrogate(0).is_none() {
            Ok(unsafe { String::from_utf8_unchecked(self.bytes) })
        } else {
            Err(self)
        }
    }

    #[inline]
    pub fn into_box(self) -> Box<Wtf8> {
        let boxed = self.bytes.into_boxed_slice();
        unsafe { Box::from_raw(Box::into_raw(boxed) as *mut Wtf8) }
    }

    pub fn from_box(boxed: Box<Wtf8>) -> Wtf8Buf {
        let bytes: Box<[u8]> = unsafe { Box::from_raw(Box::into_raw(boxed) as *mut [u8]) };
        Wtf8Buf {
            bytes: bytes.into_vec(),
            is_known_utf8: false,
        }
    }
}

impl FromIterator<CodePoint> for Wtf8Buf {
    fn from_iter<T: IntoIterator<Item = CodePoint>>(iter: T) -> Wtf8Buf {
        let mut string = Wtf8Buf::new();
        string.extend(iter);
        string
    }
}

impl Extend<CodePoint> for Wtf8Buf {
    fn extend<T: IntoIterator<Item = CodePoint>>(&mut self, iter: T) {
        let iterator = iter.into_iter();
        let (low, _high) = iterator.size_hint();
        self.bytes.reserve(low);
        iterator.for_each(move |code_point| self.push(code_point));
    }
}

impl Hash for Wtf8Buf {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(&self.bytes);
        0xfeu8.hash(state);
    }
}

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Buf {
    pub(crate) inner: Wtf8Buf,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Slice {
    pub(crate) inner: Wtf8,
}

impl fmt::Debug for Buf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_slice(), f)
    }
}

impl fmt::Display for Buf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}

impl fmt::Debug for Slice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.inner, f)
    }
}

impl fmt::Display for Slice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}

impl Clone for Buf {
    #[inline]
    fn clone(&self) -> Self {
        Buf {
            inner: self.inner.clone(),
        }
    }

    #[inline]
    fn clone_from(&mut self, source: &Self) {
        self.inner.clone_from(&source.inner);
    }
}

impl Buf {
    #[inline]
    pub const fn new() -> Buf {
        Buf {
            inner: Wtf8Buf::new(),
        }
    }

    #[inline]
    pub fn into_encoded_bytes(self) -> Vec<u8> {
        self.inner.into_bytes()
    }

    #[inline]
    pub unsafe fn from_encoded_bytes_unchecked(s: Vec<u8>) -> Self {
        Buf {
            inner: unsafe { Wtf8Buf::from_bytes_unchecked(s) },
        }
    }

    #[inline]
    pub fn into_string(self) -> Result<String, Buf> {
        self.inner.into_string().map_err(|b| Buf { inner: b })
    }

    #[inline]
    pub const fn from_string(s: String) -> Buf {
        Buf {
            inner: Wtf8Buf::from_string(s),
        }
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Buf {
        Buf {
            inner: Wtf8Buf::with_capacity(capacity),
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[inline]
    pub fn push_slice(&mut self, s: &Slice) {
        self.inner.push_wtf8(&s.inner);
    }

    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.inner.reserve(additional);
    }

    #[inline]
    pub fn try_reserve(&mut self, additional: usize) -> Result<(), TryReserveError> {
        self.inner.try_reserve(additional)
    }

    #[inline]
    pub fn reserve_exact(&mut self, additional: usize) {
        self.inner.reserve_exact(additional);
    }

    #[inline]
    pub fn try_reserve_exact(&mut self, additional: usize) -> Result<(), TryReserveError> {
        self.inner.try_reserve_exact(additional)
    }

    #[inline]
    pub fn shrink_to_fit(&mut self) {
        self.inner.shrink_to_fit();
    }

    #[inline]
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.inner.shrink_to(min_capacity);
    }

    #[inline]
    pub fn as_slice(&self) -> &Slice {
        unsafe { &*(self.inner.as_slice() as *const Wtf8 as *const Slice) }
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut Slice {
        unsafe { &mut *(self.inner.as_mut_slice() as *mut Wtf8 as *mut Slice) }
    }

    #[inline]
    pub fn leak<'a>(self) -> &'a mut Slice {
        let leaked = self.inner.leak();
        unsafe { &mut *(leaked as *mut Wtf8 as *mut Slice) }
    }

    #[inline]
    pub fn into_box(self) -> Box<Slice> {
        let boxed = self.inner.into_box();
        unsafe { Box::from_raw(Box::into_raw(boxed) as *mut Slice) }
    }

    #[inline]
    pub fn from_box(boxed: Box<Slice>) -> Buf {
        let inner: Box<Wtf8> = unsafe { Box::from_raw(Box::into_raw(boxed) as *mut Wtf8) };
        Buf {
            inner: Wtf8Buf::from_box(inner),
        }
    }

    #[inline]
    pub fn to_arc(&self) -> Arc<Slice> {
        self.as_slice().to_arc()
    }

    #[inline]
    pub fn to_rc(&self) -> Rc<Slice> {
        self.as_slice().to_rc()
    }

    #[inline]
    pub unsafe fn truncate_unchecked(&mut self, len: usize) {
        self.inner.truncate(len);
    }
}

impl Slice {
    #[inline]
    pub fn as_encoded_bytes(&self) -> &[u8] {
        self.inner.as_bytes()
    }

    #[inline]
    pub unsafe fn from_encoded_bytes_unchecked(s: &[u8]) -> &Slice {
        unsafe { &*(Wtf8::from_bytes_unchecked(s) as *const Wtf8 as *const Slice) }
    }

    #[track_caller]
    #[inline]
    pub fn check_public_boundary(&self, index: usize) {
        self.inner.check_utf8_boundary(index);
    }

    #[inline]
    pub fn try_check_public_boundary(&self, index: usize) -> Option<()> {
        self.inner.try_check_utf8_boundary(index).ok()
    }

    #[inline]
    pub fn from_str(s: &str) -> &Slice {
        unsafe { &*(Wtf8::from_str(s) as *const Wtf8 as *const Slice) }
    }

    #[inline]
    pub fn to_str(&self) -> Result<&str, str::Utf8Error> {
        self.inner.as_str()
    }

    #[inline]
    pub fn to_string_lossy(&self) -> Cow<'_, str> {
        self.inner.to_string_lossy()
    }

    #[inline]
    pub fn to_owned(&self) -> Buf {
        Buf {
            inner: self.inner.to_owned(),
        }
    }

    #[inline]
    pub fn clone_into(&self, buf: &mut Buf) {
        self.inner.clone_into(&mut buf.inner);
    }

    #[inline]
    pub fn empty_box() -> Box<Slice> {
        let boxed = Wtf8::empty_box();
        unsafe { Box::from_raw(Box::into_raw(boxed) as *mut Slice) }
    }

    #[inline]
    pub fn to_arc(&self) -> Arc<Slice> {
        let arc = self.inner.to_arc();
        unsafe { Arc::from_raw(Arc::into_raw(arc) as *const Slice) }
    }

    #[inline]
    pub fn to_rc(&self) -> Rc<Slice> {
        let rc = self.inner.to_rc();
        unsafe { Rc::from_raw(Rc::into_raw(rc) as *const Slice) }
    }

    #[inline]
    pub fn make_ascii_lowercase(&mut self) {
        self.inner.make_ascii_lowercase();
    }

    #[inline]
    pub fn make_ascii_uppercase(&mut self) {
        self.inner.make_ascii_uppercase();
    }

    #[inline]
    pub fn to_ascii_lowercase(&self) -> Buf {
        Buf {
            inner: self.inner.to_ascii_lowercase(),
        }
    }

    #[inline]
    pub fn to_ascii_uppercase(&self) -> Buf {
        Buf {
            inner: self.inner.to_ascii_uppercase(),
        }
    }

    #[inline]
    pub fn is_ascii(&self) -> bool {
        self.inner.is_ascii()
    }

    #[inline]
    pub fn eq_ignore_ascii_case(&self, other: &Self) -> bool {
        self.inner.eq_ignore_ascii_case(&other.inner)
    }
}
