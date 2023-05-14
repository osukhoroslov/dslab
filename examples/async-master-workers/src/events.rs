use dslab_compute::multicore::{CompFinished, CompStarted};
use dslab_core::{async_core::shared_state::DetailsKey, event::EventData};
use dslab_network::model::DataTransferCompleted;
use dslab_storage::events::{DataReadCompleted, DataWriteCompleted};

pub fn get_data_transfer_completed_details(data: &dyn EventData) -> DetailsKey {
    let event = data.downcast_ref::<DataTransferCompleted>().unwrap();
    event.data.id as DetailsKey
}

pub fn get_data_read_completed_details(data: &dyn EventData) -> DetailsKey {
    let event = data.downcast_ref::<DataReadCompleted>().unwrap();
    event.request_id
}

pub fn get_data_write_completed_details(data: &dyn EventData) -> DetailsKey {
    let event = data.downcast_ref::<DataWriteCompleted>().unwrap();
    event.request_id
}

pub fn get_compute_start_details(data: &dyn EventData) -> DetailsKey {
    let event = data.downcast_ref::<CompStarted>().unwrap();
    event.id
}

pub fn get_compute_finished_details(data: &dyn EventData) -> DetailsKey {
    let event = data.downcast_ref::<CompFinished>().unwrap();
    event.id
}
