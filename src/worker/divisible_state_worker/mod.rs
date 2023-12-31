use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use log::error;
use atlas_common::error::*;
use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx, SendReturnError, TryRecvError};
use atlas_common::globals::ReadOnly;
use atlas_common::persistentdb::KVDB;
use atlas_core::ordering_protocol::loggable::{OrderProtocolPersistenceHelper, PersistentOrderProtocolTypes};
use atlas_core::ordering_protocol::networking::serialize::{OrderingProtocolMessage, PermissionedOrderingProtocolMessage};
use atlas_core::persistent_log::{PersistableStateTransferProtocol};
use atlas_core::smr::networking::serialize::DecisionLogMessage;
use atlas_core::smr::smr_decision_log::DecisionLogPersistenceHelper;
use atlas_smr_application::serialize::ApplicationData;
use atlas_smr_application::state::divisible_state::{DivisibleState, StatePart};
use crate::{ResponseMessage};
use crate::serialize::{deserialize_state_descriptor, deserialize_state_part, serialize_state_descriptor, serialize_state_part, serialize_state_part_descriptor};
use crate::stateful_logs::divisible_state::DivisibleStateMessage;
use crate::worker::{COLUMN_FAMILY_STATE, PersistentLogWorker};

#[derive(Clone)]
pub struct PersistentDivStateStub<S: DivisibleState> {
    pub(crate) tx: ChannelSyncTx<DivisibleStateMessage<S>>,
}

pub struct PersistentDivStateHandle<S: DivisibleState> {
    round_robin_counter: AtomicUsize,
    tx: Vec<PersistentDivStateStub<S>>,
}

impl<S> PersistentDivStateHandle<S> where S: DivisibleState {
    pub(crate) fn new(tx: Vec<PersistentDivStateStub<S>>) -> Self {
        Self {
            round_robin_counter: AtomicUsize::new(0),
            tx,
        }
    }

    /// Employ a simple round robin load distribution
    fn next_worker(&self) -> &PersistentDivStateStub<S> {
        let counter = self.round_robin_counter.fetch_add(1, Ordering::Relaxed);

        self.tx.get(counter % self.tx.len()).unwrap()
    }

    pub fn queue_descriptor(&self, descriptor: S::StateDescriptor) -> Result<()> {
        let state_message = DivisibleStateMessage::Descriptor(descriptor);

        self.next_worker().send(state_message)
    }

    pub fn queue_state_parts(&self, parts: Vec<Arc<ReadOnly<S::StatePart>>>) -> Result<()> {
        let state_message = DivisibleStateMessage::Parts(parts);
        
        self.next_worker().send(state_message)
    }

    pub fn queue_descriptor_and_parts(&self, descriptor: S::StateDescriptor, parts: Vec<Arc<ReadOnly<S::StatePart>>>) -> Result<()> {
        let state_message = DivisibleStateMessage::PartsAndDescriptor(parts, descriptor);
        
        self.next_worker().send(state_message)
    }

    pub fn queue_delete_part(&self, part_descriptor: S::PartDescription) -> Result<()> {
        let state_message = DivisibleStateMessage::DeletePart(part_descriptor);

        self.next_worker().send(state_message)
    }
}

pub struct DivStatePersistentLogWorker<S, D, OPM, POPT, LS, POP, PSP, DLPH>
    where S: DivisibleState + 'static,
          D: ApplicationData + 'static,
          OPM: OrderingProtocolMessage<D> + 'static,
          POPT: PersistentOrderProtocolTypes<D, OPM> + 'static,
          LS: DecisionLogMessage<D, OPM, POPT> + 'static,
          POP: OrderProtocolPersistenceHelper<D, OPM, POPT> + 'static,
          PSP: PersistableStateTransferProtocol + 'static,
          DLPH: DecisionLogPersistenceHelper<D, OPM, POPT, LS> + 'static,
{
    rx: ChannelSyncRx<DivisibleStateMessage<S>>,
    worker: PersistentLogWorker<D, OPM, POPT, LS, PSP, POP, DLPH>,
    db: KVDB,
}

impl<S, D, OPM, POPT, LS, POP, PSP, DLPH> DivStatePersistentLogWorker<S, D, OPM, POPT, LS, POP, PSP, DLPH>
    where S: DivisibleState + 'static,
          D: ApplicationData + 'static,
          OPM: OrderingProtocolMessage<D> + 'static,
          POPT: PersistentOrderProtocolTypes<D, OPM> + 'static,
          LS: DecisionLogMessage<D, OPM, POPT> + 'static,
          POP: OrderProtocolPersistenceHelper<D, OPM, POPT> + 'static,
          PSP: PersistableStateTransferProtocol + 'static,
          DLPH: DecisionLogPersistenceHelper<D, OPM, POPT, LS> + 'static,
{
    pub fn new(request_rx: ChannelSyncRx<DivisibleStateMessage<S>>,
               inner_worker: PersistentLogWorker<D, OPM, POPT, LS, PSP, POP, DLPH>,
               db: KVDB) -> Result<Self> {
        Ok(Self {
            rx: request_rx,
            worker: inner_worker,
            db,
        })
    }

    pub fn work(mut self) {
        loop {
            match self.rx.try_recv() {
                Ok(message) => {
                    let result = self.exec_req(message);

                    // Try to receive more messages if possible
                    continue;
                }
                Err(err) => {
                    match err {
                        TryRecvError::ChannelEmpty => {}
                        TryRecvError::ChannelDc | TryRecvError::Timeout => {
                            error!("Error receiving message: {:?}", err);
                        }
                    }
                }
            }

            if let Err(err) = self.worker.work_iteration() {
                error!("Failed to execute persistent log request because {:?}", err);

                break;
            }
        }
    }

    fn exec_req(&mut self, message: DivisibleStateMessage<S>) -> Result<ResponseMessage> {
        Ok(
            match message {
                DivisibleStateMessage::Parts(part) => {
                    write_state_parts::<S>(&self.db, &part)?;

                    ResponseMessage::RegisteredCallback
                }
                DivisibleStateMessage::Descriptor(description) => {
                    write_state_descriptor::<S>(&self.db, &description)?;

                    ResponseMessage::RegisteredCallback
                }
                DivisibleStateMessage::PartsAndDescriptor(parts, description) => {
                    write_state_parts_and_descriptor::<S>(&self.db, &parts, &description)?;

                    ResponseMessage::RegisteredCallback
                }
                DivisibleStateMessage::DeletePart(_) => {
                    todo!();
                    ResponseMessage::RegisteredCallback
                }
            }
        )
    }
}

impl<S> Deref for PersistentDivStateStub<S> where S: DivisibleState {
    type Target = ChannelSyncTx<DivisibleStateMessage<S>>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

const LATEST_STATE_DESCRIPTOR: &str = "latest_state_descriptor";

pub(crate) fn read_latest_descriptor<S: DivisibleState>(db: &KVDB) -> Result<Option<S::StateDescriptor>> {
    let result = db.get(COLUMN_FAMILY_STATE, LATEST_STATE_DESCRIPTOR)?;

    if let Some(mut descriptor) = result {
        let state_descriptor = deserialize_state_descriptor::<&[u8], S>(&mut descriptor.as_slice())?;

        Ok(Some(state_descriptor))
    } else {
        Ok(None)
    }
}

pub(crate) fn read_state_part<S: DivisibleState>(db: &KVDB, part: &S::PartDescription) -> Result<Option<S::StatePart>> {
    let mut key = Vec::new();

    serialize_state_part_descriptor::<Vec<u8>, S>(&mut key, part)?;

    let result = db.get(COLUMN_FAMILY_STATE, key)?;

    if let Some(mut value) = result {
        let state_part = deserialize_state_part::<&[u8], S>(&mut value.as_slice())?;

        Ok(Some(state_part))
    } else {
        Ok(None)
    }
}

fn write_state_parts<S: DivisibleState>(
    db: &KVDB,
    parts: &Vec<Arc<ReadOnly<S::StatePart>>>,
) -> Result<()> {
    for state_part in parts {
        let part_desc = state_part.descriptor();

        let mut key = Vec::new();

        serialize_state_part_descriptor::<Vec<u8>, S>(&mut key, &part_desc)?;

        let mut value = Vec::new();

        serialize_state_part::<Vec<u8>, S>(&mut value, &**state_part)?;

        db.set(COLUMN_FAMILY_STATE, key, value)?;
    }

    Ok(())
}

fn write_state_descriptor<S: DivisibleState>(db: &KVDB, descriptor: &S::StateDescriptor)
                                             -> Result<()> {
    let mut value = Vec::new();

    serialize_state_descriptor::<Vec<u8>, S>(&mut value, &descriptor)?;

    db.set(COLUMN_FAMILY_STATE, LATEST_STATE_DESCRIPTOR, &value)?;

    Ok(())
}

fn write_state_parts_and_descriptor<S: DivisibleState>(
    db: &KVDB,
    parts: &Vec<Arc<ReadOnly<S::StatePart>>>,
    descriptor: &S::StateDescriptor,
) -> Result<()> {
    write_state_parts::<S>(db, parts)?;
    write_state_descriptor::<S>(db, descriptor)?;

    Ok(())
}