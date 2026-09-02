use super::{RingCursor, RingFrameSource, RingLoss, RingPoll, RingRead};

/// Walk `cursor` toward the current visible head of `source`.
///
/// At most one ring window is inspected. If the cursor has fallen behind
/// the oldest available counter, the skipped counters are recorded as
/// overwritten and the walk resumes at the window floor. An in-flight
/// counter stops the walk without advancing past it.
pub fn poll_ring<S: RingFrameSource>(source: &S, cursor: &mut RingCursor) -> RingPoll {
    let head = source.head();
    let from_counter = cursor.next_counter();

    if from_counter >= head {
        cursor.set_next_counter(head);
        return RingPoll {
            from_counter,
            to_counter: head,
            ..RingPoll::default()
        };
    }

    let capacity = source.capacity() as u64;
    let oldest_available = head.saturating_sub(capacity);
    let mut next = from_counter;
    let mut loss = RingLoss::default();

    if next < oldest_available {
        loss.overwritten = oldest_available - next;
        next = oldest_available;
    }

    let kind = source.kind();
    let mut frames = Vec::new();
    while next < head {
        match source.read_state_at(next) {
            RingRead::Ready(frame) => {
                if frame.id.kind() != kind || frame.id.counter() != next {
                    loss.unavailable = loss.unavailable.saturating_add(1);
                } else {
                    frames.push(frame);
                }
                next = next.saturating_add(1);
            }
            RingRead::Pending => break,
            RingRead::Unavailable => {
                loss.unavailable = loss.unavailable.saturating_add(1);
                next = next.saturating_add(1);
            }
        }
    }

    cursor.set_next_counter(next);
    RingPoll {
        frames,
        loss,
        from_counter,
        to_counter: next,
    }
}
