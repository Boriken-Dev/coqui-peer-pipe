//! A length-prefix codec of the tests' own: four big-endian bytes, then the message, empty allowed. The
//! crate under test picks no framing, so the tests bring one.
#![allow(dead_code)]

use coqui_transport::frame::{Codec, Decode, FrameError};

#[derive(Clone, Copy, Debug, Default)]
pub struct LengthPrefix;

#[derive(Debug)]
pub struct LengthDecoder {
    maximum: usize,
    buffer: Vec<u8>,
    failed: bool,
}

impl Codec for LengthPrefix {
    type Decoder = LengthDecoder;

    fn encode(&self, message: &[u8], maximum: usize) -> Result<Vec<u8>, FrameError> {
        let length = u32::try_from(message.len())
            .ok()
            .filter(|_| message.len() <= maximum)
            .ok_or(FrameError::Size)?;
        let mut frame = length.to_be_bytes().to_vec();
        frame.extend_from_slice(message);
        Ok(frame)
    }

    fn decoder(&self, maximum: usize) -> LengthDecoder {
        LengthDecoder {
            maximum,
            buffer: Vec::new(),
            failed: false,
        }
    }
}

impl Decode for LengthDecoder {
    fn set_maximum(&mut self, maximum: usize) {
        self.maximum = maximum;
    }

    fn push(&mut self, part: &[u8]) {
        if !self.failed {
            self.buffer.extend_from_slice(part);
        }
    }

    fn next_frame(&mut self) -> Result<Option<Vec<u8>>, FrameError> {
        if self.failed {
            return Err(FrameError::Size);
        }
        let Some(header) = self.buffer.first_chunk::<4>() else {
            return Ok(None);
        };
        let length = usize::try_from(u32::from_be_bytes(*header)).unwrap_or(usize::MAX);
        if length > self.maximum {
            self.failed = true;
            self.buffer = Vec::new();
            return Err(FrameError::Size);
        }
        if self.buffer.len() < 4 + length {
            return Ok(None);
        }
        let rest = self.buffer.split_off(4 + length);
        let mut message = core::mem::replace(&mut self.buffer, rest);
        message.drain(..4);
        Ok(Some(message))
    }

    fn buffered(&self) -> usize {
        self.buffer.len()
    }
}
