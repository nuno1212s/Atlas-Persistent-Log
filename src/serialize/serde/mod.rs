use std::io::{Read, Write};
use anyhow::Context;

use atlas_common::error::*;
use atlas_core::ordering_protocol::{DecisionMetadata, ProtocolMessage, View};
use atlas_core::ordering_protocol::loggable::PersistentOrderProtocolTypes;
use atlas_core::ordering_protocol::networking::serialize::{OrderingProtocolMessage, PermissionedOrderingProtocolMessage};
use atlas_core::smr::networking::serialize::DecisionLogMessage;
use atlas_core::smr::smr_decision_log::DecLogMetadata;
use atlas_smr_application::serialize::ApplicationData;
use atlas_smr_application::state::divisible_state::DivisibleState;
use atlas_smr_application::state::monolithic_state::MonolithicState;

pub(super) fn deserialize_message<R, D, OPM>(read: &mut R) -> Result<ProtocolMessage<D, OPM>>
    where R: Read,
          OPM: OrderingProtocolMessage<D> {
    bincode::serde::decode_from_std_read(read, bincode::config::standard()).context(
        "Failed to deserialize protocol message from reader")
}

pub(super) fn deserialize_proof_metadata<R, D, OPM>(read: &mut R) -> Result<DecisionMetadata<D, OPM>>
    where R: Read,
          OPM: OrderingProtocolMessage<D> {
    bincode::serde::decode_from_std_read(read, bincode::config::standard()).context(
        "Failed to deserialize proof metadata from metadata")
}

pub(super) fn serialize_proof_metadata<W, D, OPM>(write: &mut W, proof: &DecisionMetadata<D, OPM>) -> Result<usize>
    where W: Write,
          OPM: OrderingProtocolMessage<D> {
    bincode::serde::encode_into_std_write(proof, write, bincode::config::standard()).context(
        "Failed to serialize proof metadata into writer")
}

pub(super) fn serialize_decision_log_metadata<W, D, OPM, POPT, LS>(write: &mut W, metadata: &DecLogMetadata<D, OPM, POPT, LS>) -> Result<usize>
    where W: Write,
          OPM: OrderingProtocolMessage<D>,
          POPT: PersistentOrderProtocolTypes<D, OPM>,
          LS: DecisionLogMessage<D, OPM, POPT> {
    bincode::serde::encode_into_std_write(metadata, write, bincode::config::standard()).context(
        "Failed to serialize decision log metadata into writer")
}

pub(super) fn serialize_message<W, D, OPM>(write: &mut W, message: &ProtocolMessage<D, OPM>) -> Result<usize>
    where W: Write,
          OPM: OrderingProtocolMessage<D> {
    bincode::serde::encode_into_std_write(message, write, bincode::config::standard()).context(
        "Failed to serialize protocol message into writer")
}

pub(super) fn serialize_view<W, POP>(write: &mut W, view: &View<POP>) -> Result<usize>
    where W: Write, POP: PermissionedOrderingProtocolMessage {
    bincode::serde::encode_into_std_write(view, write, bincode::config::standard()).context(
        "Failed to serialize view into writer")
}

pub(super) fn deserialize_view<R, POP>(read: &mut R) -> Result<View<POP>>
    where R: Read, POP: PermissionedOrderingProtocolMessage {
    bincode::serde::decode_from_std_read(read, bincode::config::standard()).context(
        "Failed to deserialize view from reader")
}

pub(super) fn deserialize_decision_log_metadata<R, D, OPM, POPT, LS>(read: &mut R) -> Result<DecLogMetadata<D, OPM, POPT, LS>>
    where R: Read, D: ApplicationData,
          OPM: OrderingProtocolMessage<D>,
          POPT: PersistentOrderProtocolTypes<D, OPM>,
          LS: DecisionLogMessage<D, OPM, POPT> {
    bincode::serde::decode_from_std_read(read, bincode::config::standard()).context(
        "Failed to deserialize decision log metadata from reader")
}

pub(super) fn serialize_state_part_descriptor<W, S>(write: &mut W, part: &S::PartDescription) -> Result<usize>
    where W: Write, S: DivisibleState {
    bincode::serde::encode_into_std_write(&part, write, bincode::config::standard()).context(
        "Failed to serialize state part descriptor into writer")
}

pub(super) fn deserialize_state_part_descriptor<R, S>(read: &mut R) -> Result<S::PartDescription>
    where R: Read, S: DivisibleState {
    bincode::serde::decode_from_std_read(read, bincode::config::standard()).context(
        "Failed to deserialize state part descriptor from reader")
}

pub(super) fn serialize_state_part<W, S>(write: &mut W, part: &S::StatePart) -> Result<usize>
    where W: Write, S: DivisibleState {
    bincode::serde::encode_into_std_write(&part, write, bincode::config::standard()).context(
        "Failed to serialize state part into writer")
}

pub(super) fn deserialize_state_part<R, S>(read: &mut R) -> Result<S::StatePart>
    where R: Read, S: DivisibleState {
    bincode::serde::decode_from_std_read(read, bincode::config::standard()).context(
        "Failed to deserialize state part from reader")
}

pub(super) fn serialize_state_descriptor<W, S>(write: &mut W, desc: &S::StateDescriptor) -> Result<usize>
    where W: Write, S: DivisibleState {
    bincode::serde::encode_into_std_write(&desc, write, bincode::config::standard()).context(
        "Failed to serialize state descriptor into Writer")
}

pub(super) fn deserialize_state_descriptor<R, S>(read: &mut R) -> Result<S::StateDescriptor>
    where R: Read, S: DivisibleState {
    bincode::serde::decode_from_std_read(read, bincode::config::standard()).context(
        "Failed to deserialize state descriptor from Reader")
}

pub(super) fn serialize_state<W, S>(write: &mut W, state: &S) -> Result<usize>
    where W: Write, S: MonolithicState {
    bincode::serde::encode_into_std_write(&state, write, bincode::config::standard()).context(
        "Failed to serialize state into writer")
}

pub(super) fn deserialize_state<R, S>(read: &mut R) -> Result<S>
    where R: Read, S: MonolithicState {
    bincode::serde::decode_from_std_read(read, bincode::config::standard()).context(
        "Failed to deserialize state into reader")
}