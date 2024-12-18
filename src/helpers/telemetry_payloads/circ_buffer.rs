use std::ops::{Index, IndexMut};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct CircularBuffer<const MAX_SIZE: usize, T> {
    head: usize,
    memory: Vec<Option<T>>,
}

impl<const MAX_SIZE: usize, T> CircularBuffer<MAX_SIZE, T> {
    pub fn insert_point(&mut self, new_value: Option<T>) -> Option<T> {
        let current_last_idx = if self.head == 0 {
            MAX_SIZE - 1
        } else {
            self.head - 1
        };
        self.head = current_last_idx;

        std::mem::replace(&mut self.memory[current_last_idx], new_value)
    }

    pub fn fill_with(&mut self, generative_fun: impl Fn(usize) -> Option<T>, num_samples: usize) {
        for i in 0..num_samples {
            self.insert_point(generative_fun(i));
        }
    }

    pub fn valid_entries(&self) -> usize {
        self.memory.iter().filter(|x| x.is_some()).count()
    }
}

impl<const MAX_SIZE: usize, T> CircularBuffer<MAX_SIZE, T>
where
    T: PartialEq,
{
    pub fn entries_matching(&self, value: &T) -> usize {
        self.memory
            .iter()
            .filter(|x| x.as_ref().is_some_and(|x| *x == *value))
            .count()
    }
}

impl<const MAX_SIZE: usize, T> CircularBuffer<MAX_SIZE, T>
where
    T: Copy,
{
    pub fn new() -> Self {
        Self {
            head: 0,
            memory: vec![None; MAX_SIZE],
        }
    }

    pub fn get(&self, idx: usize) -> Option<T> {
        if idx > MAX_SIZE - 1 {
            return None;
        }
        self.memory[(self.head + idx) % MAX_SIZE]
    }

    pub fn iter(&self) -> impl Iterator<Item = Option<T>> + '_ {
        CircularBufferIter {
            buffer: self,
            idx: 0,
        }
    }

    pub fn clear(&mut self) {
        self.memory.fill(None);
        self.head = 0;
    }

    pub fn fill(&mut self, value_to_fill: Option<T>, num_samples: usize) {
        for _ in 0..num_samples {
            self.insert_point(value_to_fill);
        }
    }
}

impl<const MAX_SIZE: usize> CircularBuffer<MAX_SIZE, f64> {
    pub fn delta(&self, position: usize) -> Option<f64> {
        if position >= MAX_SIZE {
            return None;
        }
        let front = self.memory[self.head];
        let nth = self.memory[(self.head + position) % MAX_SIZE];
        front.zip(nth).map(|(front, nth)| front - nth)
    }

    pub fn moving_avg(&self, len: usize, delay: usize) -> Option<f64> {
        if delay + len >= MAX_SIZE {
            return None;
        }

        let it = self.iter();
        let (sum, count) =
            it.skip(delay)
                .take(len)
                .fold((0.0, 0_usize), |(sum, count), item| -> (f64, usize) {
                    if let Some(val) = item {
                        (sum + val, count + 1)
                    } else {
                        (sum, count)
                    }
                });

        Some(sum / f64::try_from(i32::try_from(count).unwrap()).unwrap())
    }
}

impl<const N: usize, T> Index<usize> for CircularBuffer<N, T> {
    type Output = Option<T>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.memory[(self.head + index) % N]
    }
}

impl<const N: usize, T> IndexMut<usize> for CircularBuffer<N, T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.memory[(self.head + index) % N]
    }
}

impl<const N: usize, T> Default for CircularBuffer<N, T>
where
    T: Copy,
{
    fn default() -> Self {
        Self::new()
    }
}

pub struct CircularBufferIter<'a, const MAX_SIZE: usize, T> {
    buffer: &'a CircularBuffer<MAX_SIZE, T>,
    idx: usize,
}

impl<'a, const MAX_SIZE: usize, T> Iterator for CircularBufferIter<'a, MAX_SIZE, T>
where
    T: Copy,
{
    type Item = Option<T>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= MAX_SIZE {
            return None;
        }
        let item = self.buffer.get(self.idx);
        self.idx += 1;
        Some(item)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct CircularBufferF64<const SIZE: usize> {
    head: usize,
    memory: Vec<Option<f64>>,
}

impl<const SIZE: usize> CircularBufferF64<SIZE> {
    pub fn new() -> Self {
        Self {
            head: 0,
            memory: vec![None; SIZE],
        }
    }

    pub fn insert_point(&mut self, new_value: Option<f64>) -> Option<f64> {
        let current_last_idx = if self.head == 0 {
            SIZE - 1
        } else {
            self.head - 1
        };
        self.head = current_last_idx;

        std::mem::replace(&mut self.memory[current_last_idx], new_value)
    }

    pub fn get(&self, idx: usize) -> Option<f64> {
        if idx > SIZE - 1 {
            return None;
        }
        self.memory[(self.head + idx) % SIZE]
    }

    pub fn iter(&self) -> impl Iterator<Item = Option<f64>> + '_ {
        CircularBufferIterF64 {
            buffer: self,
            idx: 0,
        }
    }

    pub fn delta(&self, position: usize) -> Option<f64> {
        if position >= SIZE {
            return None;
        }
        let front = self.memory[self.head];
        let nth = self.memory[(self.head + position) % SIZE];
        front.zip(nth).map(|(front, nth)| front - nth)
    }

    pub fn moving_avg(&self, len: usize, delay: usize) -> Option<f64> {
        if delay + len >= SIZE {
            return None;
        }

        let it = self.iter();
        let (sum, count) =
            it.skip(delay)
                .take(len)
                .fold((0.0, 0_usize), |(sum, count), item| -> (f64, usize) {
                    if let Some(val) = item {
                        (sum + val, count + 1)
                    } else {
                        (sum, count)
                    }
                });

        Some(sum / f64::try_from(i32::try_from(count).unwrap()).unwrap())
    }

    pub fn fill_with(&mut self, generative_fun: impl Fn(usize) -> Option<f64>, num_samples: usize) {
        for i in 0..num_samples {
            self.insert_point(generative_fun(i));
        }
    }

    pub fn clear(&mut self) {
        self.memory.fill(None);
        self.head = 0;
    }
}

impl<const SIZE: usize> Index<usize> for CircularBufferF64<SIZE> {
    type Output = Option<f64>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.memory[(self.head + index) % SIZE]
    }
}

impl<const SIZE: usize> IndexMut<usize> for CircularBufferF64<SIZE> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.memory[(self.head + index) % SIZE]
    }
}

impl<const SIZE: usize> Default for CircularBufferF64<SIZE> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct CircularBufferIterF64<'a, const SIZE: usize> {
    buffer: &'a CircularBufferF64<SIZE>,
    idx: usize,
}

impl<'a, const SIZE: usize> Iterator for CircularBufferIterF64<'a, SIZE> {
    type Item = Option<f64>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.buffer.memory.len() {
            return None;
        }
        let item = self.buffer.get(self.idx);
        self.idx += 1;
        Some(item)
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct CircularBufferBool<const SIZE: usize> {
    head: usize,
    memory: Vec<Option<bool>>,
}

impl<const SIZE: usize> CircularBufferBool<SIZE> {
    pub fn new() -> Self {
        Self {
            head: 0,
            memory: vec![None; SIZE],
        }
    }

    pub fn insert_point(&mut self, new_value: Option<bool>) -> Option<bool> {
        let current_last_idx = if self.head == 0 {
            SIZE - 1
        } else {
            self.head - 1
        };
        self.head = current_last_idx;

        std::mem::replace(&mut self.memory[current_last_idx], new_value)
    }

    pub fn valid_entries(&self) -> usize {
        self.memory.iter().filter(|x| x.is_some()).count()
    }

    pub fn entries_matching(&self, value: &bool) -> usize {
        self.memory
            .iter()
            .filter(|x| x.as_ref().is_some_and(|x| *x == *value))
            .count()
    }

    pub fn get(&self, idx: usize) -> Option<bool> {
        if idx > SIZE - 1 {
            return None;
        }
        self.memory[(self.head + idx) % SIZE]
    }
}
