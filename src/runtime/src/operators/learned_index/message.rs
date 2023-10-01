use actix::{dev::MessageResponse, Actor, Message, MessageResult};

use super::{error::OperatorError, opt::Operation, pointer::Pointer, state::ActorID};

#[derive(Debug, Message)]
#[rtype(result = "DataMessage")]
pub struct ControlMessage {
    target: Pointer,
    operation: Operation,
}

// impl Message for ControlMessage {
//     type Result = DataMessage;
// }

#[derive(Debug)]
pub struct DataMessage {
    data: String,
    size: usize,
    is_continous: bool,
}

// impl Message for DataMessage {
//     type Result = MessageResult<u32>;
// }

// impl<A: Actor, M: Message> MessageResponse<A, M> for DataMessage {
//     fn handle(self, ctx: &mut <A as Actor>::Context, tx: Option<actix::dev::OneshotSender<<M as
// Message>::Result>>) {         tx.unwrap().send(self);
//     }
// }

impl ControlMessage {
    pub fn new(target: Pointer) -> Self {
        ControlMessage {
            target,
            operation: Operation::Read,
        }
    }

    pub fn get_operation(&self) -> Operation {
        self.operation.clone()
    }

    pub fn read_from_pointer(&self) -> String {
        let p = &self.target;
        let addr = p.pref as *mut u8;
        let len = p.size;
        if p.cap.is_some() {
            return unsafe { String::from_raw_parts(addr, len, p.cap.unwrap()) };
        } else {
            panic!()
        }
    }
}

impl DataMessage {
    pub fn new(value: String) -> Self {
        DataMessage {
            size: 0,
            is_continous: false,
            data: "".to_string(),
        }
    }
}
