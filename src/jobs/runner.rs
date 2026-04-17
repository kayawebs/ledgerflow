use std::future::Future;

#[derive(Default, Clone)]
pub struct SyncJobRunner;

impl SyncJobRunner {
    pub async fn run<F, T>(&self, job: F) -> T
    where
        F: Future<Output = T>,
    {
        job.await
    }
}
