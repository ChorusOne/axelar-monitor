#![allow(dead_code)]

pub mod axelar {
    pub mod evm {
        pub mod v1beta1 {
            include!("axelar.evm.v1beta1.rs");
        }
    }
    pub mod reward {
        pub mod v1beta1 {
            include!("axelar.reward.v1beta1.rs");
        }
    }
    pub mod permission {
        pub mod exported {
            pub mod v1beta1 {
                include!("axelar.permission.exported.v1beta1.rs");
            }
        }
    }
    pub mod tss {
        pub mod v1beta1 {
            include!("axelar.tss.v1beta1.rs");
        }
        pub mod exported {
            pub mod v1beta1 {
                include!("axelar.tss.exported.v1beta1.rs");
            }
        }
        pub mod tofnd {
            pub mod v1beta1 {
                include!("axelar.tss.tofnd.v1beta1.rs");
            }
        }
    }
    pub mod vote {
        pub mod v1beta1 {
            include!("axelar.vote.v1beta1.rs");
        }
        pub mod exported {
            pub mod v1beta1 {
                include!("axelar.vote.exported.v1beta1.rs");
            }
        }
    }
    pub mod snapshot {
        pub mod exported {
            pub mod v1beta1 {
                include!("axelar.snapshot.exported.v1beta1.rs");
            }
        }
    }
    pub mod utils {
        pub mod v1beta1 {
            include!("axelar.utils.v1beta1.rs");
        }
    }
}
