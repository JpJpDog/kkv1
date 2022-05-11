// use std::{error::Error, fmt::Display};

// #[derive(Debug)]
// pub enum PageSystemError {}

// impl Display for PageSystemError {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         // write!(f, "{}", match *self {
//         // })
//         todo!()
//     }
// }

// impl Error for PageSystemError {
//     fn source(&self) -> Option<&(dyn Error + 'static)> {
//         None
//     }

//     fn type_id(&self, _: private::Internal) -> std::any::TypeId
//     where
//         Self: 'static,
//     {
//         std::any::TypeId::of::<Self>()
//     }

//     fn backtrace(&self) -> Option<&std::backtrace::Backtrace> {
//         None
//     }

//     fn description(&self) -> &str {
//         "description() is deprecated; use Display"
//     }

//     fn cause(&self) -> Option<&dyn Error> {
//         self.source()
//     }
// }
