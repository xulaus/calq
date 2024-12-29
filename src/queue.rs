use priority_queue::PriorityQueue;

// TODO: These common types need somewhere more sensible than a copy paste
type DBID = u128;
type TimeStamp = u64;
type Priority = u8;

use std::{cmp::Reverse, result::Result};

#[derive(Default, Debug)]
pub struct Queue {
    queue: PriorityQueue<DBID, (Priority, Reverse<TimeStamp>)>,
    delayed_queue: PriorityQueue<(Priority, DBID), Reverse<TimeStamp>>,
}

impl Queue {
    pub fn add_job(
        self: &mut Self,
        job_id: DBID,
        priority: Priority,
        execute_at: TimeStamp,
    ) -> Result<(), ()> {
        self.queue.push(job_id, (priority, Reverse(execute_at)));
        return Ok(());
    }

    pub fn delay_job(
        self: &mut Self,
        job_id: DBID,
        priority: Priority,
        execute_at: TimeStamp,
    ) -> Result<(), ()> {
        self.delayed_queue
            .push((priority, job_id), Reverse(execute_at));
        Ok(())
    }

    pub fn enqueue_from_delayed(self: &mut Self, until: TimeStamp) -> Vec<DBID> {
        let mut ret = vec![];
        while let Some((_, timestamp)) = self.delayed_queue.peek() {
            if timestamp.0 > until {
                break;
            }

            let ((priority, key), timestamp) = self.delayed_queue.pop().unwrap();
            ret.push(key);
            self.queue.push(key, (priority, timestamp));
        }
        ret
    }

    pub fn is_empty(self: &Self) -> bool {
        self.queue.is_empty() && self.delayed_queue.is_empty()
    }

    pub fn pop(self: &mut Self) -> Option<DBID> {
        if let Some((key, _)) = self.queue.pop() {
            Some(key)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {

    use std::assert_matches::assert_matches;

    use super::*;

    #[test]
    fn respects_priorities() {
        let mut queue: Queue = Default::default();
        queue.add_job(2, 2, 1).unwrap();
        queue.add_job(1, 3, 1).unwrap();
        queue.add_job(3, 1, 1).unwrap();
        assert_matches!(queue.pop(), Some(1));
        assert_matches!(queue.pop(), Some(2));
        assert_matches!(queue.pop(), Some(3));
    }

    #[test]
    fn respects_priorities_when_dequeueing_delayed() {
        let mut queue: Queue = Default::default();
        queue.add_job(3, 3, 1).unwrap();
        queue.add_job(1, 4, 1).unwrap();
        queue.add_job(5, 1, 1).unwrap();
        queue.delay_job(2, 9, 1).unwrap();
        queue.delay_job(4, 2, 2).unwrap();
        assert_matches!(queue.pop(), Some(1));

        queue.enqueue_from_delayed(2);
        assert_matches!(queue.pop(), Some(2));
        assert_matches!(queue.pop(), Some(3));
        assert_matches!(queue.pop(), Some(4));
        assert_matches!(queue.pop(), Some(5));
    }
}
