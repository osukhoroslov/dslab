use dslab_compute::multicore::{CompFinished, CompStarted};
use dslab_core::async_core::await_details::EventKey;
use dslab_network::model::DataTransferCompleted;
use dslab_storage::events::{DataReadCompleted, DataWriteCompleted};

pub fn get_data_transfer_completed_details(event: &DataTransferCompleted) -> EventKey {
    event.dt.id as EventKey
}

pub fn get_data_read_completed_details(event: &DataReadCompleted) -> EventKey {
    event.request_id
}

pub fn get_data_write_completed_details(event: &DataWriteCompleted) -> EventKey {
    event.request_id
}

pub fn get_compute_start_details(event: &CompStarted) -> EventKey {
    event.id
}

pub fn get_compute_finished_details(event: &CompFinished) -> EventKey {
    event.id
}
