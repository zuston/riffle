#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn test_localfile_disk_unhealthy() {
        // when the local disk is unhealthy, the data should be flushed
        // to the cold store(like hdfs). If not having cold, it will retry again
        // then again.
    }
}
