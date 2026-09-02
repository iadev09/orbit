//! Error type for `orbit-rs`. Deliberately small — this layer has few
//! independent failure modes.

use std::fmt;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum Error {
    /// `Fleet::join` was called twice in the same process.
    AlreadyJoined { name: &'static str },

    /// Fleet size cannot be zero — Orbit's plurality requirement (see VISION §2).
    EmptyFleet,

    /// A node id must address one of the fleet's declared member slots.
    NodeOutsideFleet { node_id: u16, fleet_size: u8 },

    /// A cache frame could not fit into the current ring payload.
    CacheFrameTooLarge {
        key_len: usize,
        value_len: usize,
        max_payload: usize,
    },

    /// An event frame could not fit into the current ring payload.
    EventFrameTooLarge {
        topic_len: usize,
        payload_len: usize,
        max_payload: usize,
    },

    /// A contest frame could not fit into the current ring payload.
    ContestFrameTooLarge {
        subject_len: usize,
        owner_len: usize,
        max_payload: usize,
    },

    /// Contest could not reconstruct its committed ordered window safely.
    ContestRingUnavailable { unavailable: u64 },

    /// An RPC request or reply could not fit into its lane payload.
    RpcFrameTooLarge {
        frame: &'static str,
        method_len: usize,
        payload_len: usize,
        max_payload: usize,
    },

    /// Shared-memory ring operation failed.
    Io(std::io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyJoined { name } => {
                write!(f, "fleet '{name}' has already been joined in this process")
            }
            Self::EmptyFleet => {
                write!(f, "fleet_size must be ≥ 1; Orbit needs at least one member")
            }
            Self::NodeOutsideFleet {
                node_id,
                fleet_size,
            } => write!(f, "node_id {node_id} is outside fleet_size {fleet_size}"),
            Self::CacheFrameTooLarge {
                key_len,
                value_len,
                max_payload,
            } => {
                write!(
                    f,
                    "orbit cache frame too large: key_len={key_len} value_len={value_len} max_payload={max_payload}"
                )
            }
            Self::EventFrameTooLarge {
                topic_len,
                payload_len,
                max_payload,
            } => {
                write!(
                    f,
                    "orbit event frame too large: topic_len={topic_len} payload_len={payload_len} max_payload={max_payload}"
                )
            }
            Self::ContestFrameTooLarge {
                subject_len,
                owner_len,
                max_payload,
            } => {
                write!(
                    f,
                    "orbit contest frame too large: subject_len={subject_len} owner_len={owner_len} max_payload={max_payload}"
                )
            }
            Self::ContestRingUnavailable { unavailable } => {
                write!(
                    f,
                    "orbit contest ordered ring has {unavailable} unavailable committed frame(s)"
                )
            }
            Self::RpcFrameTooLarge {
                frame,
                method_len,
                payload_len,
                max_payload,
            } => {
                write!(
                    f,
                    "orbit RPC {frame} frame too large: method_len={method_len} payload_len={payload_len} max_payload={max_payload}"
                )
            }
            Self::Io(err) => write!(f, "orbit io error: {err}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}
