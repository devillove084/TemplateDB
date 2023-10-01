use actix::{
    dev::MessageResponse, Actor, ActorState, Context, Handler, Message, MessageResult, Recipient,
};

use super::{
    allocator::AllocatorActor,
    error::OperatorError,
    message::{ControlMessage, DataMessage},
    opt::Operation,
    sink::Buffer,
    state::ActorID,
};

pub struct ReadActor {
    state: ActorState,
    buffer: Buffer<AllocatorActor>,
}

impl Actor for ReadActor {
    type Context = Context<ReadActor>;
}

impl Handler<ControlMessage> for ReadActor {
    type Result = MessageResult<ControlMessage>;

    fn handle(&mut self, msg: ControlMessage, ctx: &mut Self::Context) -> Self::Result {
        let opt = msg.get_operation();
        if opt == Operation::Read {
            return MessageResult(DataMessage::new(msg.read_from_pointer()));
        }
        MessageResult(DataMessage::new("String".to_string()))
    }
}

#[cfg(test)]
mod test {
    use actix::{Actor, AsyncContext, Context, System};

    use super::ReadActor;
    use crate::operators::learned_index::{
        allocator::AllocatorActor, message::ControlMessage, pointer::Pointer, sink::Buffer,
    };

    #[actix::test]
    async fn read_actor_test() {
        let addr = ReadActor {
            state: actix::ActorState::Started,
            buffer: Buffer::<AllocatorActor>::default(),
        }
        .start();
        let test_string = "TestString".to_string();
        let (_, len, cap) = test_string.clone().into_raw_parts();

        let ptr = Pointer {
            pref: unsafe { *test_string.as_ptr() as usize },
            size: len,
            cap: Some(cap),
        };
        let control_message = ControlMessage::new(ptr);

        let res = addr.send(control_message).await.unwrap();
        println!("{:?}", res);

        System::current().stop();
    }
}
