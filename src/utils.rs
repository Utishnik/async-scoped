macro_rules! cfg_smol {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "use-smol")]
            $item
        )*
    }
}

#[allow(unused_macros)]
macro_rules! cfg_smol_or_else {
    ($($item:item)*) => {
        $(
            #[cfg(all(feature = "use-tokio", not( feature = "use-smol" ) ))]
            $item
        )*
    }
}

macro_rules! cfg_tokio {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "use-tokio")]
            $item
        )*
    }
}

macro_rules! cfg_any_spawner {
    ($($item:item)*) => {
        $(
            #[cfg(any(feature = "use-smol", feature = "use-tokio", feature = "use-compio"))]
            $item
        )*
    }
}

#[allow(unused_macros)]
macro_rules! cfg_no_spawner {
    ($($item:item)*) => {
        $(
            #[cfg(not( any(feature = "use-smol", feature = "use-tokio", feature = "use-compio") ))]
            $item
        )*
    }
}
