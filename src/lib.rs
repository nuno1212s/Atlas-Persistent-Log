extern crate core;

use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

use atlas_common::channel;
use atlas_common::channel::{ChannelSyncTx, new_oneshot_channel, OneShotTx};
use atlas_common::crypto::hash::Digest;
use atlas_common::error::*;
use atlas_common::globals::ReadOnly;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_common::persistentdb::KVDB;
use atlas_communication::message::StoredMessage;
use atlas_core::ordering_protocol::{DecisionMetadata, ProtocolConsensusDecision, ProtocolMessage, View};
use atlas_core::ordering_protocol::loggable::{OrderProtocolPersistenceHelper, PersistentOrderProtocolTypes, PProof};
use atlas_core::ordering_protocol::networking::serialize::{OrderingProtocolMessage, PermissionedOrderingProtocolMessage};
use atlas_core::persistent_log::{DivisibleStateLog, MonolithicStateLog, OperationMode, OrderingProtocolLog, PersistableStateTransferProtocol, PersistentDecisionLog};
use atlas_core::smr::networking::serialize::DecisionLogMessage;
use atlas_core::smr::smr_decision_log::{DecisionLogPersistenceHelper, DecLog, DecLogMetadata, LoggingDecision, ShareableMessage};
use atlas_core::state_transfer::Checkpoint;
use atlas_core::state_transfer::networking::serialize::StateTransferMessage;
use atlas_smr_application::app::UpdateBatch;
use atlas_smr_application::ExecutorHandle;
use atlas_smr_application::serialize::ApplicationData;
use atlas_smr_application::state::divisible_state::DivisibleState;
use atlas_smr_application::state::monolithic_state::MonolithicState;

use crate::backlog::{ConsensusBacklog, ConsensusBackLogHandle};
use crate::worker::{COLUMN_FAMILY_OTHER, COLUMN_FAMILY_PROOFS, PersistentLogWorker, PersistentLogWorkerHandle, PersistentLogWriteStub, write_latest_seq_no};
use crate::worker::divisible_state_worker::{DivStatePersistentLogWorker, PersistentDivStateHandle, PersistentDivStateStub};
use crate::worker::monolithic_worker::{MonStatePersistentLogWorker, PersistentMonolithicStateHandle, PersistentMonolithicStateStub, read_mon_state};

pub mod serialize;
pub mod backlog;
mod worker;
pub mod metrics;

pub mod stateful_logs {
    pub mod monolithic_state;
    pub mod divisible_state;
}

/// The general type for a callback.
/// Callbacks are optional and can be used when you want to
/// execute a function when the logger stops finishes the computation
// pub type CallbackType = Box<dyn FnOnce(Result<ResponseMessage>) + Send>;
pub type CallbackType = ();

pub enum PersistentLogMode<D: ApplicationData> {
    /// The strict log mode is meant to indicate that the consensus can only be finalized and the
    /// requests executed when the replica has all the information persistently stored.
    ///
    /// This allows for all replicas to crash and still be able to recover from their own stored
    /// local state, meaning we can always recover without losing any piece of replied to information
    /// So we have the guarantee that once a request has been replied to, it will never be lost (given f byzantine faults).
    ///
    /// Performance will be dependent on the speed of the datastore as the consensus will only move to the
    /// executing phase once all requests have been successfully stored.
    Strict(ConsensusBackLogHandle<D::Request>),

    /// Optimistic mode relies a lot more on the assumptions that are made by the BFT algorithm in order
    /// to maximize the performance.
    ///
    /// It works by separating the persistent data storage with the consensus algorithm. It relies on
    /// the fact that we only allow for f faults concurrently, so we assume that we can never have a situation
    /// where more than f replicas fail at the same time, so they can always rely on the existence of other
    /// replicas that it can use to rebuild it's state from where it left off.
    ///
    /// One might say this provides no security benefits comparatively to storing information just in RAM (since
    /// we don't have any guarantees on what was actually stored in persistent storage)
    /// however this does provide more performance benefits as we don't have to rebuild the entire state from the
    /// other replicas of the system, which would degrade performance. We can take our incomplete state and
    /// just fill in the blanks using the state transfer algorithm
    Optimistic,

    /// Perform no persistent logging to the database and rely only on the prospect that
    /// We are always able to rebuild our state from other replicas that may be online
    None,
}

pub trait PersistentLogModeTrait: Send {
    fn init_persistent_log<D>(executor: ExecutorHandle<D>) -> PersistentLogMode<D>
        where
            D: ApplicationData + 'static;
}

///Strict log mode initializer
pub struct StrictPersistentLog;

impl PersistentLogModeTrait for StrictPersistentLog {
    fn init_persistent_log<D>(executor: ExecutorHandle<D>) -> PersistentLogMode<D>
        where
            D: ApplicationData + 'static,
    {
        let handle = ConsensusBacklog::init_backlog(executor);

        PersistentLogMode::Strict(handle)
    }
}

///Optimistic log mode initializer
pub struct OptimisticPersistentLog;

impl PersistentLogModeTrait for OptimisticPersistentLog {
    fn init_persistent_log<D: ApplicationData + 'static>(_: ExecutorHandle<D>) -> PersistentLogMode<D> {
        PersistentLogMode::Optimistic
    }
}

pub struct NoPersistentLog;

impl PersistentLogModeTrait for NoPersistentLog {
    fn init_persistent_log<D>(_: ExecutorHandle<D>) -> PersistentLogMode<D> where D: ApplicationData + 'static {
        PersistentLogMode::None
    }
}

///TODO: Handle sequence numbers that loop the u32 range.
/// This is the main reference to the persistent log, used to push data to it
pub struct PersistentLog<D: ApplicationData,
    OPM: OrderingProtocolMessage<D>,
    POPT: PersistentOrderProtocolTypes<D, OPM>,
    LS: DecisionLogMessage<D, OPM, POPT>,
    STM: StateTransferMessage>
{
    persistency_mode: PersistentLogMode<D>,

    // A handle for the persistent log workers (each with his own thread)
    worker_handle: Arc<PersistentLogWorkerHandle<D, OPM, POPT, LS>>,

    p: PhantomData<STM>,
    ///The persistent KV-DB to be used
    db: KVDB,
}


/// The type of the installed state information
pub type InstallState<D, OPM: OrderingProtocolMessage<D>,
    POPT: PersistentOrderProtocolTypes<D, OPM>,
    LS: DecisionLogMessage<D, OPM, POPT>, > = (
    //The decision log that comes after that state
    DecLog<D, OPM, POPT, LS>,
);

/// Work messages for the persistent log workers
pub enum PWMessage<D, OPM: OrderingProtocolMessage<D>,
    POPT: PersistentOrderProtocolTypes<D, OPM>,
    LS: DecisionLogMessage<D, OPM, POPT>> {
    // Read the proof for a given seq no
    ReadProof(SeqNo, OneShotTx<Option<PProof<D, OPM, POPT>>>),

    // Read the decision log and return it
    ReadDecisionLog(OneShotTx<Option<DecLog<D, OPM, POPT, LS>>>),

    //Persist a new sequence number as the consensus instance has been committed and is therefore ready to be persisted
    Committed(SeqNo),

    // The decision log has been checkpointed, we have to delete all of the proofs
    // Until that sequence number
    DecisionLogCheckpointed(SeqNo),

    // Persist the metadata for a given decision
    ProofMetadata(DecisionMetadata<D, OPM>),

    //Persist a given message into storage
    Message(ShareableMessage<ProtocolMessage<D, OPM>>),

    //Remove all associated stored messages for this given seq number
    Invalidate(SeqNo),

    // Register a proof of the decision log
    Proof(PProof<D, OPM, POPT>),
    // Decision log metadata
    DecisionLogMetadata(DecLogMetadata<D, OPM, POPT, LS>),
    //Install a recovery state received from CST or produced by us
    InstallState(InstallState<D, OPM, POPT, LS>),
    
    /// Register a new receiver for messages sent by the persistency workers
    RegisterCallbackReceiver(ChannelSyncTx<ResponseMessage>),
}


/// Messages sent by the persistency workers to notify the registered receivers
#[derive(Clone)]
pub enum ResponseMessage {
    ///Notify that we have persisted the view with the given sequence number
    ViewPersisted(SeqNo),

    ///Notifies that we have persisted the sequence number that has been persisted (Only the actual sequence number)
    /// Not related to actually persisting messages
    CommittedPersisted(SeqNo),
    // The decision log checkpoint has been persisted (all
    // proofs up to that sequence number have been deleted)
    DecisionLogCheckpointPersisted(SeqNo),
    // Notifies that the metadata for a given seq no has been persisted
    WroteMetadata(SeqNo),
    WroteDecLogMetadata,
    ///Notifies that a message with a given SeqNo and a given unique identifier for the message
    /// TODO: Decide this unique identifier
    WroteMessage(SeqNo, Digest),
    // Notifies that the state has been successfully installed and returns
    InstalledState(SeqNo),
    /// Notifies that all messages relating to the given sequence number have been destroyed
    InvalidationPersisted(SeqNo),
    /// Notifies that the given checkpoint was persisted into the database
    Checkpointed(SeqNo),
    /*
    WroteParts(Vec<Digest>),

    WroteDescriptor(SeqNo),

    WrotePartsAndDescriptor(SeqNo, Vec<Digest>),*/

    // Stored the proof with the given sequence
    Proof(SeqNo),

    ReadProof,
    ReadDecisionLog,

    RegisteredCallback,
}

/// Messages that are sent to the logging thread to log specific requests
pub(crate) type ChannelMsg<D, OPM: OrderingProtocolMessage<D>,
    POPT: PersistentOrderProtocolTypes<D, OPM>,
    LS: DecisionLogMessage<D, OPM, POPT>> = (PWMessage<D, OPM, POPT, LS>, Option<CallbackType>);

impl<D, OPM, POPT, LS, STM> PersistentLog<D, OPM, POPT, LS, STM>
    where D: ApplicationData + 'static,
          OPM: OrderingProtocolMessage<D> + 'static,
          POPT: PersistentOrderProtocolTypes<D, OPM> + 'static,
          LS: DecisionLogMessage<D, OPM, POPT> + 'static,
          STM: StateTransferMessage + 'static,
{
    fn init_log<K, T, POS, PSP, DLPH>(executor: ExecutorHandle<D>, db_path: K) -> Result<Self>
        where
            K: AsRef<Path>,
            T: PersistentLogModeTrait,
            POS: OrderProtocolPersistenceHelper<D, OPM, POPT> + Send + 'static,
            PSP: PersistableStateTransferProtocol + Send + 'static,
            DLPH: DecisionLogPersistenceHelper<D, OPM, POPT, LS> + 'static
    {
        let mut message_types = POS::message_types();

        let mut prefixes = vec![COLUMN_FAMILY_OTHER, COLUMN_FAMILY_PROOFS];

        prefixes.append(&mut message_types);

        let log_mode = T::init_persistent_log(executor);

        let mut response_txs = vec![];

        match &log_mode {
            PersistentLogMode::Strict(handle) => response_txs.push(handle.logger_tx().clone()),
            _ => {}
        }

        let kvdb = KVDB::new(db_path, prefixes)?;

        let (tx, rx) = channel::new_bounded_sync(1024,
        Some("Persistent Log Handle"));

        let worker = PersistentLogWorker::<D, OPM, POPT, LS, PSP, POS, DLPH>::new(rx, response_txs, kvdb.clone());

        match &log_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                std::thread::Builder::new().name(format!("Persistent log Worker #1"))
                    .spawn(move || {
                        worker.work();
                    }).unwrap();
            }
            _ => {}
        }

        let persistent_log_write_stub = PersistentLogWriteStub { tx };

        let worker_handle = Arc::new(PersistentLogWorkerHandle::new(vec![persistent_log_write_stub]));

        Ok(Self {
            persistency_mode: log_mode,
            worker_handle,
            p: Default::default(),
            db: kvdb,
        })
    }

    pub fn kind(&self) -> &PersistentLogMode<D> {
        &self.persistency_mode
    }
}

impl<D, OPM, POPT, LS, STM> OrderingProtocolLog<D, OPM> for PersistentLog<D, OPM, POPT, LS, STM>
    where D: ApplicationData + 'static,
          OPM: OrderingProtocolMessage<D> + 'static,
          POPT: PersistentOrderProtocolTypes<D, OPM> + 'static,
          LS: DecisionLogMessage<D, OPM, POPT> + 'static,
          STM: StateTransferMessage + 'static {
    #[inline]
    fn write_committed_seq_no(&self, write_mode: OperationMode, seq: SeqNo) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match write_mode {
                    OperationMode::NonBlockingSync(callback) => {
                        self.worker_handle.queue_committed(seq, callback)
                    }
                    OperationMode::BlockingSync => write_latest_seq_no(&self.db, seq),
                }
            }
            PersistentLogMode::None => {
                Ok(())
            }
        }
    }


    #[inline]
    fn write_message(&self, write_mode: OperationMode, msg: ShareableMessage<ProtocolMessage<D, OPM>>) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match write_mode {
                    OperationMode::NonBlockingSync(callback) => {
                        self.worker_handle.queue_message(msg, callback)
                    }
                    OperationMode::BlockingSync => {
                        todo!()
                    }
                }
            }
            PersistentLogMode::None => {
                Ok(())
            }
        }
    }

    #[inline]
    fn write_decision_metadata(&self, write_mode: OperationMode, metadata: DecisionMetadata<D, OPM>) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match write_mode {
                    OperationMode::NonBlockingSync(callback) => {
                        self.worker_handle.queue_proof_metadata(metadata, callback)
                    }
                    OperationMode::BlockingSync => {
                        todo!()
                    }
                }
            }
            PersistentLogMode::None => {
                Ok(())
            }
        }
    }


    #[inline]
    fn write_invalidate(&self, write_mode: OperationMode, seq: SeqNo) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match write_mode {
                    OperationMode::NonBlockingSync(callback) => {
                        self.worker_handle.queue_invalidate(seq, callback)
                    }
                    OperationMode::BlockingSync => {
                        todo!()
                    }
                }
            }
            PersistentLogMode::None => {
                Ok(())
            }
        }
    }
}

impl<D, OPM, POPT, LS, STM> PersistentDecisionLog<D, OPM, POPT, LS> for PersistentLog<D, OPM, POPT, LS, STM>
    where D: ApplicationData + 'static,
          OPM: OrderingProtocolMessage<D> + 'static,
          POPT: PersistentOrderProtocolTypes<D, OPM> + 'static,
          LS: DecisionLogMessage<D, OPM, POPT> + 'static,
          STM: StateTransferMessage + 'static {
    fn checkpoint_received(&self, mode: OperationMode, seq: SeqNo) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match mode {
                    OperationMode::NonBlockingSync(callback) => {
                        self.worker_handle.queue_decision_log_checkpoint(seq, callback)
                    }
                    OperationMode::BlockingSync => {
                        todo!()
                    }
                }
            }
            PersistentLogMode::None => {
                Ok(())
            }
        }
    }

    fn write_proof(&self, write_mode: OperationMode, proof: PProof<D, OPM, POPT>) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match write_mode {
                    OperationMode::NonBlockingSync(callback) => {
                        self.worker_handle.queue_proof(proof, callback)
                    }
                    OperationMode::BlockingSync => {
                        todo!()
                    }
                }
            }
            PersistentLogMode::None => Ok(())
        }
    }

    fn read_proof(&self, mode: OperationMode, seq: SeqNo) -> Result<Option<PProof<D, OPM, POPT>>> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                let (tx, rx) = new_oneshot_channel();

                let result = self.worker_handle.queue_read_proof(seq, tx, None)?;

                match mode {
                    OperationMode::NonBlockingSync(callback) => {
                        todo!()
                    }
                    OperationMode::BlockingSync => {
                        Ok(rx.recv().unwrap())
                    }
                }
            }
            PersistentLogMode::None => Ok(None)
        }
    }

    fn read_decision_log(&self, mode: OperationMode) -> Result<Option<DecLog<D, OPM, POPT, LS>>> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                let (tx, rx) = new_oneshot_channel();

                let result = self.worker_handle.queue_read_dec_log(tx, None)?;

                match mode {
                    OperationMode::NonBlockingSync(callback) => {
                        todo!()
                    }
                    OperationMode::BlockingSync => {
                        Ok(rx.recv().unwrap())
                    }
                }
            }
            PersistentLogMode::None => Ok(None)
        }
    }

    fn reset_log(&self, mode: OperationMode) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match mode {
                    OperationMode::NonBlockingSync(_) => {
                        Ok(())
                    }
                    OperationMode::BlockingSync => todo!()
                }
            }
            PersistentLogMode::None => Ok(())
        }
    }

    fn write_decision_log(&self, mode: OperationMode, log: DecLog<D, OPM, POPT, LS>) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match mode {
                    OperationMode::NonBlockingSync(callback) => {
                        self.worker_handle.queue_install_state((log, ), callback)
                    }
                    OperationMode::BlockingSync => todo!()
                }
            }
            PersistentLogMode::None => Ok(())
        }
    }

    fn wait_for_full_persistence(&self, batch: UpdateBatch<D::Request>, decision_logging: LoggingDecision) -> Result<Option<UpdateBatch<D::Request>>> {
        match &self.persistency_mode {
            PersistentLogMode::Strict(backlog) => {
                backlog.queue_decision(batch, decision_logging)?;

                Ok(None)
            }
            PersistentLogMode::None | PersistentLogMode::Optimistic => Ok(Some(batch))
        }
    }

    fn write_decision_log_metadata(&self, mode: OperationMode, log_metadata: DecLogMetadata<D, OPM, POPT, LS>) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match mode {
                    OperationMode::NonBlockingSync(callback) => {
                        self.worker_handle.queue_decision_log_metadata(log_metadata, callback)
                    }
                    OperationMode::BlockingSync => todo!()
                }
            }
            PersistentLogMode::None => Ok(())
        }
    }
}

impl<D, OPM, POPT, LS, STM> Clone for PersistentLog<D, OPM, POPT, LS, STM>
    where D: ApplicationData + 'static,
          OPM: OrderingProtocolMessage<D> + 'static,
          POPT: PersistentOrderProtocolTypes<D, OPM> + 'static,
          LS: DecisionLogMessage<D, OPM, POPT> + 'static,
          STM: StateTransferMessage + 'static {
    fn clone(&self) -> Self {
        Self {
            persistency_mode: self.persistency_mode.clone(),
            worker_handle: self.worker_handle.clone(),
            p: Default::default(),
            db: self.db.clone(),
        }
    }
}

impl<D: ApplicationData> Clone for PersistentLogMode<D> {
    fn clone(&self) -> Self {
        match self {
            PersistentLogMode::Strict(handle) => {
                PersistentLogMode::Strict(handle.clone())
            }
            PersistentLogMode::Optimistic => {
                PersistentLogMode::Optimistic
            }
            PersistentLogMode::None => {
                PersistentLogMode::None
            }
        }
    }
}


