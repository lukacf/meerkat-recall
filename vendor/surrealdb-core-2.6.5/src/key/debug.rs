use std::ops::Range;

pub trait Sprintable {
    fn sprint(&self) -> String;
}

impl Sprintable for &str {
    fn sprint(&self) -> String {
        self.to_string()
    }
}

impl Sprintable for String {
    fn sprint(&self) -> String {
        self.clone()
    }
}

impl Sprintable for Vec<u8> {
    fn sprint(&self) -> String {
        self.as_slice().escape_ascii().to_string()
    }
}

impl Sprintable for &[u8] {
    fn sprint(&self) -> String {
        // The standard formatter writes printable ASCII runs together and uses
        // the same escape_default representation for every escaped byte.
        self.escape_ascii().to_string()
    }
}

impl<T> Sprintable for Vec<T>
where
    T: Sprintable,
{
    fn sprint(&self) -> String {
        self.iter()
            .map(Sprintable::sprint)
            .collect::<Vec<_>>()
            .join(" + ")
    }
}

impl<T> Sprintable for Range<T>
where
    T: Sprintable,
{
    fn sprint(&self) -> String {
        format!("{}..{}", self.start.sprint(), self.end.sprint())
    }
}
