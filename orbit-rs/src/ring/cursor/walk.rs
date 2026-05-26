use super::{RingCursor, RingFrameSource, RingLoss, RingPoll};

/// Walk `cursor` to the current head of `source`.
///
/// At most one ring window is inspected. If the cursor has fallen behind
/// the oldest available counter, the skipped counters are recorded as
/// overwritten and the walk resumes at the window floor.
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
    for counter in next..head {
        let Some(frame) = source.read_at(counter) else {
            loss.unavailable = loss.unavailable.saturating_add(1);
            continue;
        };
        if frame.id.kind() != kind || frame.id.counter() != counter {
            loss.unavailable = loss.unavailable.saturating_add(1);
            continue;
        }
        frames.push(frame);
    }

    cursor.set_next_counter(head);
    RingPoll {
        frames,
        loss,
        from_counter,
        to_counter: head,
    }
}
