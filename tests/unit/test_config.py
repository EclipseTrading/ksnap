from ksnap.config import KsnapConfig


def test_knsap_config_from_cli_args():
    l = [
        "backup",
        "-b",
        "hkstgkafka02.hk.eclipseoptions.com:9092,hkstgkafka03.hk.eclipseoptions.com:9092,hkstgkafka04.hk.eclipseoptions.com:9092",
        "-t",
        "CBBCAutomationInstrumentFitPOParameters,CBBCAutomationInstrumentPOState,CBBCAutomationStrategyParameters",
        "-d",
        "backup/test_3",
        "--ignore-missing-topics",
        "--threads",
        "32"
    ]
    config = KsnapConfig.from_cli_args(l)
    assert config
    assert config.action == "backup"
    assert config.brokers == [
        "hkstgkafka02.hk.eclipseoptions.com:9092",
        "hkstgkafka03.hk.eclipseoptions.com:9092",
        "hkstgkafka04.hk.eclipseoptions.com:9092",
    ]
    assert config.topics == [
        "CBBCAutomationInstrumentFitPOParameters",
        "CBBCAutomationInstrumentPOState",
        "CBBCAutomationStrategyParameters",
    ]
    assert config.data == "backup/test_3"
    assert config.ignore_missing_topics
    assert config.threads == 32
