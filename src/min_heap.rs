use std::cmp::Reverse;
use std::collections::binary_heap::{BinaryHeap, IntoIter};
use std::iter::FromIterator;

/// Wrapper around `BinaryHeap` to be default min heap instead of max heap.
pub struct MinHeap<T> {
    inner: BinaryHeap<Reverse<T>>,
}

impl<T: Ord> MinHeap<T> {
    pub fn push(&mut self, t: T) {
        self.inner.push(Reverse(t));
    }

    pub fn pop(&mut self) -> Option<T> {
        match self.inner.pop() {
            None => None,
            Some(Reverse(t)) => Some(t),
        }
    }

    pub fn peek(&self) -> Option<&T> {
        match self.inner.peek() {
            None => None,
            Some(Reverse(t)) => Some(t),
        }
    }

    #[cfg(test)]
    pub fn new() -> Self {
        Self {
            inner: BinaryHeap::new(),
        }
    }
}

impl<T: Ord> FromIterator<T> for MinHeap<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let inner = iter.into_iter().map(|t| Reverse(t)).collect();
        Self { inner }
    }
}

impl<T> IntoIterator for MinHeap<T> {
    type Item = T;
    type IntoIter = std::iter::Map<IntoIter<Reverse<T>>, fn(Reverse<T>) -> T>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter().map(|r| r.0)
    }
}

#[cfg(test)]
mod tests {
    use super::MinHeap;

    #[test]
    fn test_push_pop() {
        // Elements pushed into the heap should be popped back in increasing order.
        let mut heap = MinHeap::new();

        heap.push(7);
        heap.push(11);
        heap.push(4);
        heap.push(1);

        assert_eq!(heap.pop(), Some(1));
        assert_eq!(heap.pop(), Some(4));
        assert_eq!(heap.pop(), Some(7));
        assert_eq!(heap.pop(), Some(11));
        assert_eq!(heap.pop(), None);
    }

    #[test]
    fn test_push_pop_push() {
        // Elements pushed into the heap should be popped back in increasing order.
        let mut heap = MinHeap::new();

        heap.push(8);
        heap.push(16);
        heap.push(2);
        heap.push(1);

        assert_eq!(heap.pop(), Some(1));
        assert_eq!(heap.pop(), Some(2));

        heap.push(4);
        heap.push(32);

        assert_eq!(heap.pop(), Some(4));
        assert_eq!(heap.pop(), Some(8));
        assert_eq!(heap.pop(), Some(16));
        assert_eq!(heap.pop(), Some(32));
        assert_eq!(heap.pop(), None);
    }

    #[test]
    fn test_collect_pop() {
        // Elements collected into a heap should be popped back in increasing order.
        let mut heap: MinHeap<usize> = [9, 3, 100, 10, 5].iter().copied().collect();

        assert_eq!(heap.pop(), Some(3));
        assert_eq!(heap.pop(), Some(5));
        assert_eq!(heap.pop(), Some(9));
        assert_eq!(heap.pop(), Some(10));
        assert_eq!(heap.pop(), Some(100));
        assert_eq!(heap.pop(), None);
    }

    #[test]
    fn test_peek() {
        // Peek should reveal the smallest element, but not remove it
        let mut heap = MinHeap::new();

        heap.push(37);
        heap.push(17);
        heap.push(101);

        assert_eq!(Some(&17), heap.peek());
        assert_eq!(Some(17), heap.pop());

        assert_eq!(Some(&37), heap.peek());
        assert_eq!(Some(37), heap.pop());

        assert_eq!(Some(&101), heap.peek());
        assert_eq!(Some(101), heap.pop());

        assert_eq!(None, heap.pop());
    }
}
