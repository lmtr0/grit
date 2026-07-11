#[cfg(any(feature = "redis", feature = "nats", feature = "s3"))]
#[test]
#[ignore = "live service tests must be run through ./test.sh"]
fn live_service_tests_are_run_through_test_script() {
    assert_eq!(
        std::env::var("GRIT_LIB_SERVER_TEST_SCRIPT").as_deref(),
        Ok("1"),
        "grit-lib-server live tests need to be run through ./test.sh so Redis, NATS, MinIO, and AWS SDK environment variables are configured consistently"
    );
}
